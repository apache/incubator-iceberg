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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public abstract class TestSparkDataWrite {
  private static final Configuration CONF = new Configuration();
  private final FileFormat format;
  private static SparkSession spark = null;
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[] parameters() {
    return new Object[] { "parquet", "avro", "orc" };
  }

  @BeforeClass
  public static void startSpark() {
    TestSparkDataWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDataWrite.spark;
    TestSparkDataWrite.spark = null;
    currentSpark.stop();
  }

  public TestSparkDataWrite(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Test
  public void testBasicWrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
    for (ManifestFile manifest : table.currentSnapshot().allManifests()) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        // TODO: avro not support split
        if (!format.equals(FileFormat.AVRO)) {
          Assert.assertNotNull("Split offsets not present", file.splitOffsets());
        }
        Assert.assertEquals("Should have reported record count as 1", 1, file.recordCount());
        // TODO: append more metric info
        if (format.equals(FileFormat.PARQUET)) {
          Assert.assertNotNull("Column sizes metric not present", file.columnSizes());
          Assert.assertNotNull("Counts metric not present", file.valueCounts());
          Assert.assertNotNull("Null value counts metric not present", file.nullValueCounts());
          Assert.assertNotNull("Lower bounds metric not present", file.lowerBounds());
          Assert.assertNotNull("Upper bounds metric not present", file.upperBounds());
        }
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "a"),
        new SimpleRecord(5, "b"),
        new SimpleRecord(6, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    df.withColumn("id", df.col("id").plus(3)).select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "a"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "b"),
        new SimpleRecord(6, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    // overwrite with 2*id to replace record 2, append 4 and 6
    df.withColumn("id", df.col("id").multiply(2)).select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Overwrite)
        .option("overwrite-mode", "dynamic")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    // overwrite with the same data; should not produce two copies
    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Overwrite)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedCreateWithTargetFileSizeViaTableProperties() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    table.updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(4000);
    for (int i = 0; i < 4000; i++) {
      expected.add(new SimpleRecord(i, "a"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().allManifests()) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        files.add(file);
      }
    }
    // TODO: ORC file now not support target file size
    if (!format.equals(FileFormat.ORC)) {
      Assert.assertEquals("Should have 4 DataFiles", 4, files.size());
      Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
    }
  }

  @Test
  public void testPartitionedCreateWithTargetFileSizeViaOption() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.NONE);
  }

  @Test
  public void testPartitionedFanoutCreateWithTargetFileSizeViaOption() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.TABLE);
  }

  @Test
  public void testPartitionedFanoutCreateWithTargetFileSizeViaOption2() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.JOB);
  }

  @Test
  public void testWriteProjection() throws IOException {
    Assume.assumeTrue(
        "Not supported in Spark 3.0; analysis requires all columns are present",
        spark.version().startsWith("2"));

    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, null),
        new SimpleRecord(2, null),
        new SimpleRecord(3, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id").write() // select only id column
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testWriteProjectionWithMiddle() throws IOException {
    Assume.assumeTrue(
        "Not supported in Spark 3.0; analysis requires all columns are present",
        spark.version().startsWith("2"));

    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Schema schema = new Schema(
        optional(1, "c1", Types.IntegerType.get()),
        optional(2, "c2", Types.StringType.get()),
        optional(3, "c3", Types.StringType.get())
    );
    Table table = tables.create(schema, spec, location.toString());

    List<ThreeColumnRecord> expected = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "hello"),
        new ThreeColumnRecord(2, null, "world"),
        new ThreeColumnRecord(3, null, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, ThreeColumnRecord.class);

    df.select("c1", "c3").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<ThreeColumnRecord> actual = result.orderBy("c1").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testViewsReturnRecentResults() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    Dataset<Row> query = spark.read()
        .format("iceberg")
        .load(location.toString())
        .where("id = 1");
    query.createOrReplaceTempView("tmp");

    List<SimpleRecord> actual1 = spark.table("tmp").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expected1 = Lists.newArrayList(
        new SimpleRecord(1, "a")
    );
    Assert.assertEquals("Number of rows should match", expected1.size(), actual1.size());
    Assert.assertEquals("Result rows should match", expected1, actual1);

    df.select("id", "data").write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    List<SimpleRecord> actual2 = spark.table("tmp").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expected2 = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(1, "a")
    );
    Assert.assertEquals("Number of rows should match", expected2.size(), actual2.size());
    Assert.assertEquals("Result rows should match", expected2, actual2);
  }

  public void partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType option)
      throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      expected.add(new SimpleRecord(i, "a"));
      expected.add(new SimpleRecord(i, "b"));
      expected.add(new SimpleRecord(i, "c"));
      expected.add(new SimpleRecord(i, "d"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    switch (option) {
      case NONE:
        df.select("id", "data").sort("data").write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .save(location.toString());
        break;
      case TABLE:
        table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();
        df.select("id", "data").write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .save(location.toString());
        break;
      case JOB:
        df.select("id", "data").write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .option(SparkWriteOptions.FANOUT_ENABLED, true)
            .save(location.toString());
        break;
      default:
        break;
    }

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().allManifests()) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        files.add(file);
      }
    }
    // TODO: ORC file now not support target file size
    if (!format.equals(FileFormat.ORC)) {
      Assert.assertEquals("Should have 8 DataFiles", 8, files.size());
      Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
    }
  }

  public enum IcebergOptionsType {
    NONE,
    TABLE,
    JOB
  }
}
