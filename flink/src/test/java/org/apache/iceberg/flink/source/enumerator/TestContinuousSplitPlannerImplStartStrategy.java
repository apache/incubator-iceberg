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

package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

public class TestContinuousSplitPlannerImplStartStrategy {

  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  public static final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  @ClassRule
  public static final TestRule chain = RuleChain
      .outerRule(TEMPORARY_FOLDER)
      .around(tableResource);

  private static final FileFormat fileFormat = FileFormat.PARQUET;

  private static Snapshot snapshot1;
  private static Snapshot snapshot2;
  private static Snapshot snapshot3;

  @BeforeClass
  public static void beforeClass() throws IOException {
    GenericAppenderHelper dataAppender = new GenericAppenderHelper(tableResource.table(), fileFormat, TEMPORARY_FOLDER);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch1);
    snapshot1 = tableResource.table().currentSnapshot();
    // snapshot2
    final List<Record> batch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch2);
    snapshot2 = tableResource.table().currentSnapshot();
    // snapshot3
    final List<Record> batch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch3);
    snapshot3 = tableResource.table().currentSnapshot();
  }

  @Test
  public void testStartSnapshotIdForTableScanThenIncrementalStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    Assert.assertEquals(snapshot3.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForLatestSnapshotStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.LATEST_SNAPSHOT)
        .build();
    Assert.assertEquals(snapshot3.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForEarliestSnapshotStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.EARLIEST_SNAPSHOT)
        .build();
    Assert.assertEquals(snapshot1.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotIdStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_ID)
        .startSnapshotId(snapshot2.snapshotId())
        .build();
    Assert.assertEquals(snapshot2.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotTimestampStrategySnapshot2() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_TIMESTAMP)
        .startSnapshotTimeMs(snapshot2.timestampMillis())
        .build();
    Assert.assertEquals(snapshot2.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotTimestampStrategySnapshot2Minus1() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_TIMESTAMP)
        .startSnapshotTimeMs(snapshot2.timestampMillis() - 1L)
        .build();
    Assert.assertEquals(snapshot1.snapshotId(),
        ContinuousSplitPlannerImpl.getStartSnapshot(tableResource.table(), config).snapshotId());
  }

}
