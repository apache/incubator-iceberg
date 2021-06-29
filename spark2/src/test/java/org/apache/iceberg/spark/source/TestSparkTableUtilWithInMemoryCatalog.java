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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkTableUtilWithInMemoryCatalog {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("data")
      .build();

  private static SparkSession spark;

  @BeforeClass
  public static void startSpark() {
    TestSparkTableUtilWithInMemoryCatalog.spark = SparkSession.builder()
        .master("local[2]")
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkTableUtilWithInMemoryCatalog.spark;
    TestSparkTableUtilWithInMemoryCatalog.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testImportUnpartitionedTable() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    props.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "full");
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), props, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class).coalesce(1);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString(),
          false);

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", records, actualRecords);

      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");
      Types.NestedField idField = table.schema().findField("id");
      checkFieldMetrics(fileDF, idField, true);
      Types.NestedField dataField = table.schema().findField("data");
      checkFieldMetrics(fileDF, dataField, false);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitionedTable() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    props.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "full");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      Assert.assertEquals(
          "Should have 3 partitions",
          3, SparkTableUtil.getPartitions(spark, "parquet_table").size());

      Assert.assertEquals(
          "Should have 1 partition where data = 'a'",
          1, SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'").size());

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString(),
          false);

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", records, actualRecords);

      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");
      Types.NestedField idField = table.schema().findField("id");
      checkFieldMetrics(fileDF, idField, true);
      // 'data' is a partition column and is not physically present in files written by Spark
      Types.NestedField dataField = table.schema().findField("data");
      checkFieldMetrics(fileDF, dataField, true);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitions() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      List<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'");
      SparkTableUtil.importSparkPartitions(spark, partitions, table, table.spec(), stagingDir.toString(),
          false);

      List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", expectedRecords, actualRecords);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitionsWithSnapshotInheritance() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, tableLocation);

    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      List<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'");
      SparkTableUtil.importSparkPartitions(spark, partitions, table, table.spec(), stagingDir.toString(),
          false);

      List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", expectedRecords, actualRecords);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportTableWithMappingForNestedData() throws IOException {
    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> df1 = spark.range(1, 2)
          .withColumn("extra_col", functions.lit(-1))
          .withColumn("struct", functions.expr("named_struct('nested_1', 'a', 'nested_2', 'd', 'nested_3', 'f')"));
      Dataset<Row> df2 = spark.range(2, 3)
          .withColumn("extra_col", functions.lit(-1))
          .withColumn("struct", functions.expr("named_struct('nested_1', 'b', 'nested_2', 'e', 'nested_3', 'g')"));
      df1.union(df2).coalesce(1).select("id", "extra_col", "struct").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .saveAsTable("parquet_table");

      // don't include `extra_col` and `nested_2` on purpose
      Schema schema = new Schema(
          optional(1, "id", Types.LongType.get()),
          required(2, "struct", Types.StructType.of(
              required(3, "nested_1", Types.StringType.get()),
              required(4, "nested_3", Types.StringType.get())
          ))
      );
      Table table = TABLES.create(schema, PartitionSpec.unpartitioned(), tableLocation);

      // assign a custom metrics config and a name mapping
      NameMapping nameMapping = MappingUtil.create(schema);
      table.updateProperties()
          .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
          .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "full")
          .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "struct.nested_3", "full")
          .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
          .commit();

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString(),
          false);

      // validate we get the expected results back
      List<Row> expected = spark.table("parquet_table")
          .select("id", "struct.nested_1", "struct.nested_3")
          .collectAsList();
      List<Row> actual = spark.read().format("iceberg").load(tableLocation)
          .select("id", "struct.nested_1", "struct.nested_3")
          .collectAsList();
      Assert.assertEquals("Rows must match", expected, actual);

      // validate we persisted correct metrics
      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");

      List<Row> bounds = fileDF.select("lower_bounds", "upper_bounds").collectAsList();
      Assert.assertEquals("Must have lower bounds for 2 columns", 2, bounds.get(0).getMap(0).size());
      Assert.assertEquals("Must have upper bounds for 2 columns", 2, bounds.get(0).getMap(1).size());

      Types.NestedField nestedField1 = table.schema().findField("struct.nested_1");
      checkFieldMetrics(fileDF, nestedField1, true);

      Types.NestedField id = table.schema().findField("id");
      checkFieldMetrics(fileDF, id, 1L, 2L);

      Types.NestedField nestedField3 = table.schema().findField("struct.nested_3");
      checkFieldMetrics(fileDF, nestedField3, "f", "g");
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportTableWithMappingForNestedDataPartitionedTable() throws IOException {
    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> df1 = spark.range(1, 2)
          .withColumn("extra_col", functions.lit(-1))
          .withColumn("struct", functions.expr("named_struct('nested_1', 'a', 'nested_2', 'd', 'nested_3', 'f')"))
          .withColumn("data", functions.lit("Z"));
      Dataset<Row> df2 = spark.range(2, 3)
          .withColumn("extra_col", functions.lit(-1))
          .withColumn("struct", functions.expr("named_struct('nested_1', 'b', 'nested_2', 'e', 'nested_3', 'g')"))
          .withColumn("data", functions.lit("Z"));
      df1.union(df2).coalesce(1).select("id", "extra_col", "struct", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      // don't include `extra_col` and `nested_2` on purpose
      Schema schema = new Schema(
          optional(1, "id", Types.LongType.get()),
          required(2, "struct", Types.StructType.of(
              required(4, "nested_1", Types.StringType.get()),
              required(5, "nested_3", Types.StringType.get())
          )),
          required(3, "data", Types.StringType.get())
      );
      PartitionSpec spec = PartitionSpec.builderFor(schema)
          .identity("data")
          .build();
      Table table = TABLES.create(schema, spec, tableLocation);

      // assign a custom metrics config and a name mapping
      NameMapping nameMapping = MappingUtil.create(schema);
      table.updateProperties()
          .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
          .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "full")
          .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "struct.nested_3", "full")
          .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
          .commit();

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString(),
          false);

      // validate we get the expected results back
      List<Row> expected = spark.table("parquet_table")
          .select("id", "struct.nested_1", "struct.nested_3", "data")
          .collectAsList();
      List<Row> actual = spark.read().format("iceberg").load(tableLocation)
          .select("id", "struct.nested_1", "struct.nested_3", "data")
          .collectAsList();
      Assert.assertEquals("Rows must match", expected, actual);

      // validate we persisted correct metrics
      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");

      List<Row> bounds = fileDF.select("lower_bounds", "upper_bounds").collectAsList();
      Assert.assertEquals("Must have lower bounds for 2 columns", 2, bounds.get(0).getMap(0).size());
      Assert.assertEquals("Must have upper bounds for 2 columns", 2, bounds.get(0).getMap(1).size());

      Types.NestedField nestedField1 = table.schema().findField("struct.nested_1");
      checkFieldMetrics(fileDF, nestedField1, true);

      Types.NestedField id = table.schema().findField("id");
      checkFieldMetrics(fileDF, id, 1L, 2L);

      Types.NestedField nestedField3 = table.schema().findField("struct.nested_3");
      checkFieldMetrics(fileDF, nestedField3, "f", "g");
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportTableWithInt96Timestamp() throws IOException {
    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      spark.conf().set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE().key(), "INT96");

      Column timestampColumn = functions.to_timestamp(functions.lit("2010-03-20 10:40:30.1234"));
      Dataset<Row> df = spark.range(1, 10).withColumn("tmp_col", timestampColumn);
      df.coalesce(1).select("id", "tmp_col").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .saveAsTable("parquet_table");

      Schema schema = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "tmp_col", Types.TimestampType.withZone())
      );
      Table table = TABLES.create(schema, PartitionSpec.unpartitioned(), tableLocation);

      // assign a custom metrics config
      table.updateProperties()
          .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
          .commit();

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString(),
          false);

      // validate we get the expected results back
      List<Row> expected = spark.table("parquet_table")
          .select("id", "tmp_col")
          .collectAsList();
      List<Row> actual = spark.read().format("iceberg").load(tableLocation)
          .select("id", "tmp_col")
          .collectAsList();
      Assert.assertEquals("Rows must match", expected, actual);

      // validate we did not persist metrics for INT96
      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");

      Types.NestedField timestampField = table.schema().findField("tmp_col");
      checkFieldMetrics(fileDF, timestampField, true);

      Types.NestedField idField = table.schema().findField("id");
      checkFieldMetrics(fileDF, idField, 1L, 9L);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  private void checkFieldMetrics(Dataset<Row> fileDF, Types.NestedField field, Object min, Object max) {
    List<Row> metricRows = fileDF
        .selectExpr(
            String.format("lower_bounds['%d']", field.fieldId()),
            String.format("upper_bounds['%d']", field.fieldId())
        )
        .collectAsList();

    // we compare string representations not to deal with HeapCharBuffers for strings
    Object actualMin = Conversions.fromByteBuffer(field.type(), ByteBuffer.wrap(metricRows.get(0).getAs(0)));
    Assert.assertEquals("Min value should match", min.toString(), actualMin.toString());
    Object actualMax = Conversions.fromByteBuffer(field.type(), ByteBuffer.wrap(metricRows.get(0).getAs(1)));
    Assert.assertEquals("Max value should match", max.toString(), actualMax.toString());
  }

  private void checkFieldMetrics(Dataset<Row> fileDF, Types.NestedField field, boolean isNull) {
    List<Row> metricRows = fileDF
        .selectExpr(
            String.format("lower_bounds['%d']", field.fieldId()),
            String.format("upper_bounds['%d']", field.fieldId())
        )
        .collectAsList();

    metricRows.forEach(row -> {
      Assert.assertEquals("Invalid metrics for column: " + field.name(), isNull, row.isNullAt(0));
      Assert.assertEquals("Invalid metrics for column: " + field.name(), isNull, row.isNullAt(1));
    });
  }
}
