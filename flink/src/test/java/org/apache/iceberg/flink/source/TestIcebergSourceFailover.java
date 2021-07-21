/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.source;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorConfig;
import org.apache.iceberg.flink.source.reader.RowDataIteratorReaderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceFailover {

  private static final int PARALLELISM = 4;

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final MiniClusterWithClientResource miniClusterResource =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .withHaLeadershipControl()
              .build());

  @Rule
  public final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();

  @Test
  public void testBoundedWithTaskManagerFailover() throws Exception {
    testBoundedIcebergSource(FailoverType.TM);
  }

  @Test
  public void testBoundedWithJobManagerFailover() throws Exception {
    testBoundedIcebergSource(FailoverType.JM);
  }

  private void testBoundedIcebergSource(FailoverType failoverType) throws Exception {
    final List<Record> expectedRecords = new ArrayList<>();
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);
    for (int i = 0; i < 4; ++i) {
      List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
    final Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_SIZE, 128);
    final RowType rowType = FlinkSchemaUtil.convert(scanContext.project());

    final DataStream<Row> stream = env.fromSource(
        IcebergSource.<RowData>builder()
            .tableLoader(tableResource.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .readerFactory(new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType))
            .scanContext(scanContext)
            .build(),
        WatermarkStrategy.noWatermarks(),
        "IcebergSource",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

    final DataStream<Row> streamFailingInTheMiddleOfReading =
        RecordCounterToFail.wrapWithFailureAfter(stream, expectedRecords.size() / 2);

    final ClientAndIterator<Row> client =
        DataStreamUtils.collectWithClient(
            streamFailingInTheMiddleOfReading, "Bounded Iceberg Source Failover Test");
    final JobID jobId = client.client.getJobID();

    RecordCounterToFail.waitToFail();
    triggerFailover(
        failoverType,
        jobId,
        RecordCounterToFail::continueProcessing,
        miniClusterResource.getMiniCluster());

    try (CloseableIterator<Row> iter = client.iterator) {
      final List<Row> actualRows = Lists.newArrayList(iter);
      TestHelpers.assertRecords(actualRows, expectedRecords, TestFixtures.SCHEMA);
    }
  }

  @Test
  public void testContinuousWithTaskManagerFailover() throws Exception {
    testContinuousIcebergSource(FailoverType.TM);
  }

  @Test
  public void testContinuousWithJobManagerFailover() throws Exception {
    testContinuousIcebergSource(FailoverType.JM);
  }

  private void testContinuousIcebergSource(FailoverType failoverType) throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);
    final List<Record> expectedRecords = Lists.newArrayList();

    final List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0);
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.enableCheckpointing(10L);
    final Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_SIZE, 128);
    final RowType rowType = FlinkSchemaUtil.convert(scanContext.project());

    final DataStream<Row> stream = env.fromSource(
        IcebergSource.<RowData>builder()
            .tableLoader(tableResource.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .readerFactory(new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType))
            .scanContext(scanContext)
            .enumeratorConfig(IcebergEnumeratorConfig.builder()
                .splitDiscoveryInterval(Duration.ofMillis(10))
                .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .build())
            .build(),
        WatermarkStrategy.noWatermarks(),
        "IcebergSource",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

    final ClientAndIterator<Row> client =
        DataStreamUtils.collectWithClient(
            stream, "Continuous Iceberg Source Failover Test");
    final JobID jobId = client.client.getJobID();

    final List<Row> actualRows = Lists.newArrayList();
    final CloseableIterator<Row> closeableIterator = client.iterator;
    actualRows.addAll(TestIcebergSourceContinuous.waitForResult(closeableIterator, 2));
    TestHelpers.assertRecords(actualRows, expectedRecords, TestFixtures.SCHEMA);

    try (CloseableIterator<Row> iter = closeableIterator) {
      for (int i = 1; i < 5; i++) {
        Thread.sleep(10);
        List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
        expectedRecords.addAll(records);
        dataAppender.appendToTable(records);
        if (i == 2) {
          triggerFailover(failoverType, jobId, () -> {
          }, miniClusterResource.getMiniCluster());
        }
      }
      actualRows.addAll(TestIcebergSourceContinuous.waitForResult(iter, 8));
    }
    TestHelpers.assertRecords(actualRows, expectedRecords, TestFixtures.SCHEMA);
  }

  // ------------------------------------------------------------------------
  // test utilities copied from Flink's FileSourceTextLinesITCase
  // ------------------------------------------------------------------------

  private enum FailoverType {
    NONE,
    TM,
    JM
  }

  private static void triggerFailover(
      FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    switch (type) {
      case NONE:
        afterFailAction.run();
        break;
      case TM:
        restartTaskManager(afterFailAction, miniCluster);
        break;
      case JM:
        triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
        break;
    }
  }

  private static void triggerJobManagerFailover(
      JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
    final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
    haLeadershipControl.revokeJobMasterLeadership(jobId).get();
    afterFailAction.run();
    haLeadershipControl.grantJobMasterLeadership(jobId).get();
  }

  private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    miniCluster.terminateTaskManager(0).get();
    afterFailAction.run();
    miniCluster.startTaskManager();
  }

  private static class RecordCounterToFail {

    private static AtomicInteger records;
    private static CompletableFuture<Void> fail;
    private static CompletableFuture<Void> continueProcessing;

    private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {

      records = new AtomicInteger();
      fail = new CompletableFuture<>();
      continueProcessing = new CompletableFuture<>();
      return stream.map(
          record -> {
            final boolean reachedFailPoint = records.incrementAndGet() > failAfter;
            final boolean notFailedYet = !fail.isDone();
            if (notFailedYet && reachedFailPoint) {
              fail.complete(null);
              continueProcessing.get();
            }
            return record;
          });
    }

    private static void waitToFail() throws ExecutionException, InterruptedException {
      fail.get();
    }

    private static void continueProcessing() {
      continueProcessing.complete(null);
    }
  }
}
