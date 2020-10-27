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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.PlanScanAction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public abstract class TestSnapshotSelection {

  @Parameterized.Parameters(name = "Plan Mode {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {PlanScanAction.PlanMode.LOCAL},
        new Object[] {PlanScanAction.PlanMode.DISTRIBUTED},
    };
  }

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  private final PlanScanAction.PlanMode planMode;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  public TestSnapshotSelection(PlanScanAction.PlanMode planMode) {
    this.planMode = planMode;
  }

  @BeforeClass
  public static void startSpark() {
    TestSnapshotSelection.spark = SparkSession
        .builder()
        .config(PlanScanAction.ICEBERG_TEST_PLAN_MODE, "true")
        .master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSnapshotSelection.spark;
    TestSnapshotSelection.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testSnapshotSelectionById() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords = Lists.newArrayList(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f")
    );
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read()
        .format("iceberg")
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords = currentSnapshotResult.orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    Assert.assertEquals("Current snapshot rows should match", expectedRecords, currentSnapshotRecords);

    // verify records in the previous snapshot
    Snapshot currentSnapshot = table.currentSnapshot();
    Long parentSnapshotId = currentSnapshot.parentId();
    Dataset<Row> previousSnapshotResult = spark.read()
        .format("iceberg")
        .option("snapshot-id", parentSnapshotId)
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords = previousSnapshotResult.orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    Assert.assertEquals("Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);
  }

  @Test
  public void testSnapshotSelectionByTimestamp() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // remember the time when the first snapshot was valid
    long firstSnapshotTimestamp = System.currentTimeMillis();

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords = Lists.newArrayList(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f")
    );
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read()
        .format("iceberg")
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords = currentSnapshotResult.orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    Assert.assertEquals("Current snapshot rows should match", expectedRecords, currentSnapshotRecords);

    // verify records in the previous snapshot
    Dataset<Row> previousSnapshotResult = spark.read()
        .format("iceberg")
        .option("as-of-timestamp", firstSnapshotTimestamp)
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords = previousSnapshotResult.orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    Assert.assertEquals("Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSnapshotSelectionByInvalidSnapshotId() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, tableLocation);

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .option("snapshot-id", -10)
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);

    df.collectAsList();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSnapshotSelectionByInvalidTimestamp() throws IOException {
    long timestamp = System.currentTimeMillis();

    String tableLocation = temp.newFolder("iceberg-table").toString();
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, tableLocation);

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .option("as-of-timestamp", timestamp)
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);

    df.collectAsList();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSnapshotSelectionBySnapshotIdAndTimestamp() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, tableLocation);

    List<SimpleRecord> firstBatchRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    long timestamp = System.currentTimeMillis();
    long snapshotId = table.currentSnapshot().snapshotId();
    Dataset<Row> df = spark.read()
        .format("iceberg")
        .option("snapshot-id", snapshotId)
        .option("as-of-timestamp", timestamp)
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);

    df.collectAsList();
  }
}
