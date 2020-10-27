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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.PlanScanAction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
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
public abstract class TestDataSourceOptions {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;

  private final PlanScanAction.PlanMode planMode;

  @Parameterized.Parameters(name = "Plan Mode = {0}")
  public static Object[] parameters() {
    return new Object[] {PlanScanAction.PlanMode.LOCAL, PlanScanAction.PlanMode.DISTRIBUTED};
  }

  public TestDataSourceOptions(PlanScanAction.PlanMode planMode) {
    this.planMode = planMode;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    TestDataSourceOptions.spark = SparkSession
        .builder()
        .config(PlanScanAction.ICEBERG_TEST_PLAN_MODE, "true")
        .master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestDataSourceOptions.spark;
    TestDataSourceOptions.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testWriteFormatOptionOverridesTableProperties() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", "parquet")
        .mode("append")
        .save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> {
        FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
        Assert.assertEquals(FileFormat.PARQUET, fileFormat);
      });
    }
  }

  @Test
  public void testNoWriteFormatOption() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> {
        FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
        Assert.assertEquals(FileFormat.AVRO, fileFormat);
      });
    }
  }

  @Test
  public void testHadoopOptions() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    String originalDefaultFS = sparkHadoopConf.get("fs.default.name");

    try {
      HadoopTables tables = new HadoopTables(CONF);
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> options = Maps.newHashMap();
      tables.create(SCHEMA, spec, options, tableLocation);

      // set an invalid value for 'fs.default.name' in Spark Hadoop config
      // to verify that 'hadoop.' data source options are propagated correctly
      sparkHadoopConf.set("fs.default.name", "hdfs://localhost:9000");

      List<SimpleRecord> expectedRecords = Lists.newArrayList(
          new SimpleRecord(1, "a"),
          new SimpleRecord(2, "b")
      );
      Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
      originalDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .option("hadoop.fs.default.name", "file:///")
          .save(tableLocation);

      Dataset<Row> resultDf = spark.read()
          .format("iceberg")
          .option("hadoop.fs.default.name", "file:///")
          .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
          .load(tableLocation);
      List<SimpleRecord> resultRecords = resultDf.orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Records should match", expectedRecords, resultRecords);
    } finally {
      sparkHadoopConf.set("fs.default.name", originalDefaultFS);
    }
  }

  @Test
  public void testSplitOptionsOverridesTableProperties() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SPLIT_SIZE, String.valueOf(128L * 1024 * 1024)); // 128Mb
    tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .option("split-size", String.valueOf(611)) // 611 bytes is the size of SimpleRecord(1,"a")
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation);

    Assert.assertEquals("Spark partitions should match", 2, resultDf.javaRDD().getNumPartitions());
  }

  @Test
  public void testIncrementalScanOptions() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d")
    );
    for (SimpleRecord record : expectedRecords) {
      Dataset<Row> originalDf = spark.createDataFrame(Lists.newArrayList(record), SimpleRecord.class);
      originalDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableLocation);
    }
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);

    // start-snapshot-id and snapshot-id are both configured.
    AssertHelpers.assertThrows(
        "Check both start-snapshot-id and snapshot-id are configured",
        IllegalArgumentException.class,
        "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan",
        () -> {
          spark.read()
              .format("iceberg")
              .option("snapshot-id", snapshotIds.get(3).toString())
              .option("start-snapshot-id", snapshotIds.get(3).toString())
              .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
              .load(tableLocation).explain();
        });

    // end-snapshot-id and as-of-timestamp are both configured.
    AssertHelpers.assertThrows(
        "Check both start-snapshot-id and snapshot-id are configured",
        IllegalArgumentException.class,
        "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan",
        () -> {
          spark.read()
              .format("iceberg")
              .option("as-of-timestamp", Long.toString(table.snapshot(snapshotIds.get(3)).timestampMillis()))
              .option("end-snapshot-id", snapshotIds.get(2).toString())
              .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
              .load(tableLocation).explain();
        });

    // only end-snapshot-id is configured.
    AssertHelpers.assertThrows(
        "Check both start-snapshot-id and snapshot-id are configured",
        IllegalArgumentException.class,
        "Cannot only specify option end-snapshot-id to do incremental scan",
        () -> {
          spark.read()
              .format("iceberg")
              .option("end-snapshot-id", snapshotIds.get(2).toString())
              .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
              .load(tableLocation).explain();
        });

    // test (1st snapshot, current snapshot] incremental scan.
    List<SimpleRecord> result = spark.read()
        .format("iceberg")
        .option("start-snapshot-id", snapshotIds.get(3).toString())
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation)
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    Assert.assertEquals("Records should match", expectedRecords.subList(1, 4), result);

    // test (2nd snapshot, 3rd snapshot] incremental scan.
    List<SimpleRecord> result1 = spark.read()
        .format("iceberg")
        .option("start-snapshot-id", snapshotIds.get(2).toString())
        .option("end-snapshot-id", snapshotIds.get(1).toString())
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation)
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    Assert.assertEquals("Records should match", expectedRecords.subList(2, 3), result1);
  }

  @Test
  public void testMetadataSplitSizeOptionOverrideTableProperties() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    // produce 1st manifest
    originalDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    // produce 2nd manifest
    originalDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();

    Assert.assertEquals("Must be 2 manifests", 2, manifests.size());

    // set the target metadata split size so each manifest ends up in a separate split
    table.updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(manifests.get(0).length()))
        .commit();

    Dataset<Row> entriesDf = spark.read()
        .format("iceberg")
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation + "#entries");
    Assert.assertEquals("Num partitions must match", 2, entriesDf.javaRDD().getNumPartitions());

    // override the table property using options
    entriesDf = spark.read()
        .format("iceberg")
        .option("split-size", String.valueOf(128 * 1024 * 1024))
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation + "#entries");
    Assert.assertEquals("Num partitions must match", 1, entriesDf.javaRDD().getNumPartitions());
  }

  @Test
  public void testDefaultMetadataSplitSize() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    int splitSize = (int) TableProperties.METADATA_SPLIT_SIZE_DEFAULT; // 32MB split size

    int expectedSplits = ((int) tables.load(tableLocation + "#entries")
        .currentSnapshot().allManifests().get(0).length() + splitSize - 1) / splitSize;

    Dataset<Row> metadataDf = spark.read()
        .format("iceberg")
        .option(PlanScanAction.ICEBERG_PLAN_MODE, planMode.name())
        .load(tableLocation + "#entries");

    int partitionNum = metadataDf.javaRDD().getNumPartitions();
    Assert.assertEquals("Spark partitions should match", expectedSplits, partitionNum);
  }

  @Test
  public void testExtraSnapshotMetadata() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();
    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b")
    );
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write()
            .format("iceberg")
            .mode("append")
            .option("snapshot-property.extra-key", "someValue")
            .option("snapshot-property.another-key", "anotherValue")
            .save(tableLocation);

    Table table = tables.load(tableLocation);

    Assert.assertTrue(table.currentSnapshot().summary().get("extra-key").equals("someValue"));
    Assert.assertTrue(table.currentSnapshot().summary().get("another-key").equals("anotherValue"));
  }
}
