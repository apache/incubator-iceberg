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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public abstract class TestPartitionValues {
  @Parameterized.Parameters(name = "format = {0}, vectorized = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "parquet", false },
        { "parquet", true },
        { "avro", false },
        { "orc", false },
        { "orc", true }
    };
  }

  private static final Schema SUPPORTED_PRIMITIVES = new Schema(
      required(100, "id", Types.LongType.get()),
      required(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      required(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      required(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      required(107, "date", Types.DateType.get()),
      required(108, "ts", Types.TimestampType.withZone()),
      required(110, "s", Types.StringType.get()),
      required(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
  );

  private static final Schema SIMPLE_SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SIMPLE_SCHEMA)
      .identity("data")
      .build();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestPartitionValues.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestPartitionValues.spark;
    TestPartitionValues.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String format;
  private final boolean vectorized;

  public TestPartitionValues(String format, boolean vectorized) {
    this.format = format;
    this.vectorized = vectorized;
  }

  @Test
  public void testNullPartitionValue() throws Exception {
    String desc = "null_part";
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SIMPLE_SCHEMA, SPEC, location.toString());
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .option("vectorization-enabled", String.valueOf(vectorized))
        .load(location.toString());

    List<SimpleRecord> actual = result
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testReorderedColumns() throws Exception {
    String desc = "reorder_columns";
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SIMPLE_SCHEMA, SPEC, location.toString());
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<SimpleRecord> expected = Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("data", "id").write()
            .format("iceberg")
            .mode("append")
            .option("check-ordering", "false")
            .save(location.toString());

    Dataset<Row> result = spark.read()
            .format("iceberg")
            .option("vectorization-enabled", String.valueOf(vectorized))
            .load(location.toString());

    List<SimpleRecord> actual = result
            .orderBy("id")
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();

    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testReorderedColumnsNoNullability() throws Exception {
    String desc = "reorder_columns_no_nullability";
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SIMPLE_SCHEMA, SPEC, location.toString());
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<SimpleRecord> expected = Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("data", "id").write()
            .format("iceberg")
            .mode("append")
            .option("check-ordering", "false")
            .option("check-nullability", "false")
            .save(location.toString());

    Dataset<Row> result = spark.read()
            .format("iceberg")
            .option("vectorization-enabled", String.valueOf(vectorized))
            .load(location.toString());

    List<SimpleRecord> actual = result
            .orderBy("id")
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();

    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testPartitionValueTypes() throws Exception {
    String[] columnNames = new String[] {
        "b", "i", "l", "f", "d", "date", "ts", "s", "bytes", "dec_9_0", "dec_11_2", "dec_38_10"
    };

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());

    // create a table around the source data
    String sourceLocation = temp.newFolder("source_table").toString();
    Table source = tables.create(SUPPORTED_PRIMITIVES, sourceLocation);

    // write out an Avro data file with all of the data types for source data
    List<GenericData.Record> expected = RandomData.generateList(source.schema(), 2, 128735L);
    File avroData = temp.newFile("data.avro");
    Assert.assertTrue(avroData.delete());
    try (FileAppender<GenericData.Record> appender = Avro.write(Files.localOutput(avroData))
        .schema(source.schema())
        .build()) {
      appender.addAll(expected);
    }

    // add the Avro data file to the source table
    source.newAppend()
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(10)
            .withInputFile(Files.localInput(avroData))
            .build())
        .commit();

    Dataset<Row> sourceDF = spark.read().format("iceberg")
        .option("vectorization-enabled", String.valueOf(vectorized))
        .load(sourceLocation);

    for (String column : columnNames) {
      String desc = "partition_by_" + SUPPORTED_PRIMITIVES.findType(column).toString();

      File parent = temp.newFolder(desc);
      File location = new File(parent, "test");
      File dataFolder = new File(location, "data");
      Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

      PartitionSpec spec = PartitionSpec.builderFor(SUPPORTED_PRIMITIVES).identity(column).build();

      Table table = tables.create(SUPPORTED_PRIMITIVES, spec, location.toString());
      table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

      sourceDF.write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());

      List<Row> actual = spark.read()
          .format("iceberg")
          .option("vectorization-enabled", String.valueOf(vectorized))
          .load(location.toString())
          .collectAsList();

      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());

      for (int i = 0; i < expected.size(); i += 1) {
        TestHelpers.assertEqualsSafe(
            SUPPORTED_PRIMITIVES.asStruct(), expected.get(i), actual.get(i));
      }
    }
  }

  @Test
  public void testNestedPartitionValues() throws Exception {
    String[] columnNames = new String[] {
        "b", "i", "l", "f", "d", "date", "ts", "s", "bytes", "dec_9_0", "dec_11_2", "dec_38_10"
    };

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Schema nestedSchema = new Schema(optional(1, "nested", SUPPORTED_PRIMITIVES.asStruct()));

    // create a table around the source data
    String sourceLocation = temp.newFolder("source_table").toString();
    Table source = tables.create(nestedSchema, sourceLocation);

    // write out an Avro data file with all of the data types for source data
    List<GenericData.Record> expected = RandomData.generateList(source.schema(), 2, 128735L);
    File avroData = temp.newFile("data.avro");
    Assert.assertTrue(avroData.delete());
    try (FileAppender<GenericData.Record> appender = Avro.write(Files.localOutput(avroData))
        .schema(source.schema())
        .build()) {
      appender.addAll(expected);
    }

    // add the Avro data file to the source table
    source.newAppend()
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(10)
            .withInputFile(Files.localInput(avroData))
            .build())
        .commit();

    Dataset<Row> sourceDF = spark.read().format("iceberg")
        .option("vectorization-enabled", String.valueOf(vectorized))
        .load(sourceLocation);

    for (String column : columnNames) {
      String desc = "partition_by_" + SUPPORTED_PRIMITIVES.findType(column).toString();

      File parent = temp.newFolder(desc);
      File location = new File(parent, "test");
      File dataFolder = new File(location, "data");
      Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

      PartitionSpec spec = PartitionSpec.builderFor(nestedSchema).identity("nested." + column).build();

      Table table = tables.create(nestedSchema, spec, location.toString());
      table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

      sourceDF.write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());

      List<Row> actual = spark.read()
          .format("iceberg")
          .option("vectorization-enabled", String.valueOf(vectorized))
          .load(location.toString())
          .collectAsList();

      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());

      for (int i = 0; i < expected.size(); i += 1) {
        TestHelpers.assertEqualsSafe(
            nestedSchema.asStruct(), expected.get(i), actual.get(i));
      }
    }
  }

  /**
   * To verify if WrappedPositionAccessor is generated against a string field within a nested field,
   * rather than a Position2Accessor.
   * Or when building the partition path, a ClassCastException is thrown with the message like:
   * Cannot cast org.apache.spark.unsafe.types.UTF8String to java.lang.CharSequence
   */
  @Test
  public void testPartitionedByNestedString() throws Exception {
    // schema and partition spec
    Schema nestedSchema = new Schema(
        Types.NestedField.required(1, "struct",
            Types.StructType.of(Types.NestedField.required(2, "string", Types.StringType.get()))
        )
    );
    PartitionSpec spec = PartitionSpec.builderFor(nestedSchema).identity("struct.string").build();

    // create table
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    String baseLocation = temp.newFolder("partition_by_nested_string").toString();
    tables.create(nestedSchema, spec, baseLocation);

    // input data frame
    StructField[] structFields = {
        new StructField("struct",
            DataTypes.createStructType(
                new StructField[] {
                    new StructField("string", DataTypes.StringType, false, Metadata.empty())
                }
            ),
            false, Metadata.empty()
        )
    };

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(RowFactory.create("nested_string_value")));
    Dataset<Row> sourceDF = spark.createDataFrame(rows, new StructType(structFields));

    // write into iceberg
    sourceDF.write()
        .format("iceberg")
        .mode("append")
        .save(baseLocation);

    // verify
    List<Row> actual = spark.read()
        .format("iceberg")
        .option("vectorization-enabled", String.valueOf(vectorized))
        .load(baseLocation)
        .collectAsList();

    Assert.assertEquals("Number of rows should match", rows.size(), actual.size());
  }
}
