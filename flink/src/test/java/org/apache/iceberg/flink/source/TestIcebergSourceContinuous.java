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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorConfig;
import org.apache.iceberg.flink.source.reader.RowDataIteratorReaderFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceContinuous {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final AtomicLong randomSeed = new AtomicLong(0L);

  @Test
  public void testTableScanThenIncremental() throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    final IcebergEnumeratorConfig enumeratorConfig = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMillis(10L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();

    try (CloseableIterator<Row> iter = createStream(enumeratorConfig).executeAndCollect(getClass().getSimpleName())) {

      final List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);
      tableResource.table().currentSnapshot().snapshotId();

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      tableResource.table().currentSnapshot().snapshotId();

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testEarliestSnapshot() throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    final List<Record> batch0 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    final IcebergEnumeratorConfig enumeratorConfig = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMillis(10L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.EARLIEST_SNAPSHOT)
        .build();

    try (CloseableIterator<Row> iter = createStream(enumeratorConfig).executeAndCollect(getClass().getSimpleName())) {

      final List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testLatestSnapshot() throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    final List<Record> batch0 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    final IcebergEnumeratorConfig enumeratorConfig = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMillis(10L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.LATEST_SNAPSHOT)
        .build();

    try (CloseableIterator<Row> iter = createStream(enumeratorConfig).executeAndCollect(getClass().getSimpleName())) {

      // we want to make sure job is running first so that enumerator can
      // start from the latest snapshot before inserting the next batch2 below.
      waitUntilJobIsRunning(MINI_CLUSTER_RESOURCE.getClusterClient());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testSpecificSnapshotId() throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    final List<Record> batch0 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    final long snapshot0 = tableResource.table().currentSnapshot().snapshotId();
    System.out.println("testSpecificSnapshotId batch0: " + batch0);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    System.out.println("testSpecificSnapshotId batch1: " + batch1);

    final IcebergEnumeratorConfig enumeratorConfig = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMillis(10L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_ID)
        .startSnapshotId(snapshot0)
        .build();

    try (CloseableIterator<Row> iter = createStream(enumeratorConfig).executeAndCollect(getClass().getSimpleName())) {

      final List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);
      System.out.println("testSpecificSnapshotId batch2: " + batch2);

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      System.out.println("testSpecificSnapshotId batch3: " + batch3);

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testSpecificSnapshotTimestamp() throws Exception {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    final List<Record> batch0 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    List<HistoryEntry> history = tableResource.table().history();
    Assert.assertEquals(1, history.size());
    final long snapshot0Timestamp = history.get(0).timestampMillis();

    // sleep for 2 ms to make sure snapshot1 has a higher timestamp value
    Thread.sleep(2);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(
        tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    final IcebergEnumeratorConfig enumeratorConfig = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMillis(10L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_TIMESTAMP)
        .startSnapshotTimeMs(snapshot0Timestamp)
        .build();

    try (CloseableIterator<Row> iter = createStream(enumeratorConfig).executeAndCollect(getClass().getSimpleName())) {

      // consume data from snapshot1
      final List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(
          tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  private DataStream<Row> createStream(IcebergEnumeratorConfig enumeratorConfig) throws Exception {

    final Configuration config = new Configuration();
    final ScanContext scanContext = ScanContext.builder()
        .project(tableResource.table().schema())
        .build();
    final RowType rowType = FlinkSchemaUtil.convert(scanContext.project());

    // start the source and collect output
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final DataStream<Row> stream = env.fromSource(
        IcebergSource.<RowData>builder()
            .tableLoader(tableResource.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .readerFactory(new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType))
            .scanContext(scanContext)
            .enumeratorConfig(enumeratorConfig)
            .build(),
        WatermarkStrategy.noWatermarks(),
        "icebergSource",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

    return stream;
  }

  public static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = new ArrayList<>(limit);
    while (results.size() < limit) {
      if (iter.hasNext()) {
        results.add(iter.next());
      } else {
        break;
      }
    }
    return results;
  }

  public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
    while (getRunningJobs(client).isEmpty()) {
      Thread.sleep(10);
    }
  }

  public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
    Collection<JobStatusMessage> statusMessages = client.listJobs().get();
    return statusMessages.stream()
        .filter(status -> status.getJobState() == JobStatus.RUNNING)
        .map(JobStatusMessage::getJobId)
        .collect(Collectors.toList());
  }
}
