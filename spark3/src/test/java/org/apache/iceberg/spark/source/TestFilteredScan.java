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
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.data.GenericsHelpers;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.Files.localOutput;
import static org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.column;

@RunWith(Parameterized.class)
public class TestFilteredScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(3, "data", Types.StringType.get())
  );

  private static final PartitionSpec BUCKET_BY_ID = PartitionSpec.builderFor(SCHEMA)
      .bucket("id", 4)
      .build();

  private static final PartitionSpec PARTITION_BY_DAY = PartitionSpec.builderFor(SCHEMA)
      .day("ts")
      .build();

  private static final PartitionSpec PARTITION_BY_HOUR = PartitionSpec.builderFor(SCHEMA)
      .hour("ts")
      .build();

  private static final PartitionSpec PARTITION_BY_DATA = PartitionSpec.builderFor(SCHEMA)
      .identity("data")
      .build();

  private static final PartitionSpec PARTITION_BY_ID = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestFilteredScan.spark = SparkSession.builder().master("local[2]").getOrCreate();

    // define UDFs used by partition tests
    Transform<Long, Integer> bucket4 = Transforms.bucket(Types.LongType.get(), 4);
    spark.udf().register("bucket4", (UDF1<Long, Integer>) bucket4::apply, IntegerType$.MODULE$);

    Transform<Long, Integer> day = Transforms.day(Types.TimestampType.withZone());
    spark.udf().register("ts_day",
        (UDF1<Timestamp, Integer>) timestamp -> day.apply((Long) fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);

    Transform<Long, Integer> hour = Transforms.hour(Types.TimestampType.withZone());
    spark.udf().register("ts_hour",
        (UDF1<Timestamp, Integer>) timestamp -> hour.apply((Long) fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);

    spark.udf().register("data_ident", (UDF1<String, String>) data -> data, StringType$.MODULE$);
    spark.udf().register("id_ident", (UDF1<Long, Long>) id -> id, LongType$.MODULE$);
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestFilteredScan.spark;
    TestFilteredScan.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String format;
  private final boolean vectorized;

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

  public TestFilteredScan(String format, boolean vectorized) {
    this.format = format;
    this.vectorized = vectorized;
  }

  private File parent = null;
  private File unpartitioned = null;
  private List<Record> records = null;

  @Before
  public void writeUnpartitionedTable() throws IOException {
    this.parent = temp.newFolder("TestFilteredScan");
    this.unpartitioned = new File(parent, "unpartitioned");
    File dataFolder = new File(unpartitioned, "data");
    Assert.assertTrue("Mkdir should succeed", dataFolder.mkdirs());

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), unpartitioned.toString());
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    FileFormat fileFormat = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));

    File testFile = new File(dataFolder, fileFormat.addExtension(UUID.randomUUID().toString()));

    this.records = testRecords(tableSchema);

    try (FileAppender<Record> writer = new GenericAppenderFactory(table).newAppender(
        localOutput(testFile), fileFormat)) {
      writer.addAll(records);
    }

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(records.size())
        .withFileSizeInBytes(testFile.length())
        .withPath(testFile.toString())
        .build();

    table.newAppend().appendFile(file).commit();
  }

  @Test
  public void testUnpartitionedIDFilters() {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of(
        "path", unpartitioned.toString())
    );
    SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    for (int i = 0; i < 10; i += 1) {
      pushFilters(builder, EqualTo.apply("id", i));
      Batch scan = builder.build().toBatch();

      InputPartition[] partitions = scan.planInputPartitions();
      Assert.assertEquals("Should only create one task for a small file", 1, partitions.length);

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i),
          read(unpartitioned.toString(), vectorized, "id = " + i));
    }
  }

  @Test
  public void testUnpartitionedCaseInsensitiveIDFilters() {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of(
        "path", unpartitioned.toString())
    );

    // set spark.sql.caseSensitive to false
    String caseSensitivityBeforeTest = TestFilteredScan.spark.conf().get("spark.sql.caseSensitive");
    TestFilteredScan.spark.conf().set("spark.sql.caseSensitive", "false");

    try {

      for (int i = 0; i < 10; i += 1) {
        SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options)
            .caseSensitive(false);

        pushFilters(builder, EqualTo.apply("ID", i)); // note lower(ID) == lower(id), so there must be a match
        Batch scan = builder.build().toBatch();

        InputPartition[] tasks = scan.planInputPartitions();
        Assert.assertEquals("Should only create one task for a small file", 1, tasks.length);

        // validate row filtering
        assertEqualsSafe(SCHEMA.asStruct(), expected(i),
            read(unpartitioned.toString(), vectorized, "id = " + i));
      }
    } finally {
      // return global conf to previous state
      TestFilteredScan.spark.conf().set("spark.sql.caseSensitive", caseSensitivityBeforeTest);
    }
  }

  @Test
  public void testUnpartitionedTimestampFilter() {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of(
        "path", unpartitioned.toString())
    );

    SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
    Batch scan = builder.build().toBatch();

    InputPartition[] tasks = scan.planInputPartitions();
    Assert.assertEquals("Should only create one task for a small file", 1, tasks.length);

    assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
        read(unpartitioned.toString(), vectorized, "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
  }

  @Test
  public void testBucketPartitionedIDFilters() {
    Table table = buildPartitionedTable("bucketed_by_id", BUCKET_BY_ID, "bucket4", "id");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    Batch unfiltered = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();
    Assert.assertEquals("Unfiltered table should created 4 read tasks",
        4, unfiltered.planInputPartitions().length);

    for (int i = 0; i < 10; i += 1) {
      SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, EqualTo.apply("id", i));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();

      // validate predicate push-down
      Assert.assertEquals("Should create one task for a single bucket", 1, tasks.length);

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i), read(table.location(), vectorized, "id = " + i));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testDayPartitionedTimestampFilters() {
    Table table = buildPartitionedTable("partitioned_by_day", PARTITION_BY_DAY, "ts_day", "ts");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));
    Batch unfiltered = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();

    Assert.assertEquals("Unfiltered table should created 2 read tasks",
        2, unfiltered.planInputPartitions().length);

    {
      SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      Assert.assertEquals("Should create one task for 2017-12-21", 1, tasks.length);

      assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
          read(table.location(), vectorized, "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, And.apply(
          GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
          LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      Assert.assertEquals("Should create one task for 2017-12-22", 1, tasks.length);

      assertEqualsSafe(SCHEMA.asStruct(), expected(1, 2), read(table.location(), vectorized,
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testHourPartitionedTimestampFilters() {
    Table table = buildPartitionedTable("partitioned_by_hour", PARTITION_BY_HOUR, "ts_hour", "ts");

    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));
    Batch unfiltered = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();

    Assert.assertEquals("Unfiltered table should created 9 read tasks",
        9, unfiltered.planInputPartitions().length);

    {
      SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      Assert.assertEquals("Should create 4 tasks for 2017-12-21: 15, 17, 21, 22", 4, tasks.length);

      assertEqualsSafe(SCHEMA.asStruct(), expected(8, 9, 7, 6, 5),
          read(table.location(), vectorized, "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, And.apply(
          GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
          LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      Assert.assertEquals("Should create 2 tasks for 2017-12-22: 6, 7", 2, tasks.length);

      assertEqualsSafe(SCHEMA.asStruct(), expected(2, 1), read(table.location(), vectorized,
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testFilterByNonProjectedColumn() {
    {
      Schema actualProjection = SCHEMA.select("id", "data");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(5, 6, 7, 8, 9)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
          unpartitioned.toString(), vectorized,
          "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)",
          "id", "data"));
    }

    {
      // only project id: ts will be projected because of the filter, but data will not be included

      Schema actualProjection = SCHEMA.select("id");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(1, 2)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
          unpartitioned.toString(), vectorized,
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)",
          "id"));
    }
  }

  @Test
  public void testPartitionedByDataStartsWithFilter() {
    Table table = buildPartitionedTable("partitioned_by_data", PARTITION_BY_DATA, "data_ident", "data");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new StringStartsWith("data", "junc"));
    Batch scan = builder.build().toBatch();

    Assert.assertEquals(1, scan.planInputPartitions().length);
  }

  @Test
  public void testPartitionedByIdStartsWith() {
    Table table = buildPartitionedTable("partitioned_by_id", PARTITION_BY_ID, "id_ident", "id");

    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(ImmutableMap.of(
        "path", table.location())
    );

    SparkScanBuilder builder = new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new StringStartsWith("data", "junc"));
    Batch scan = builder.build().toBatch();

    Assert.assertEquals(1, scan.planInputPartitions().length);
  }

  @Test
  public void testUnpartitionedStartsWith() {
    Dataset<Row> df = spark.read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .load(unpartitioned.toString());

    List<String> matchedData = df.select("data")
        .where("data LIKE 'jun%'")
        .as(Encoders.STRING())
        .collectAsList();

    Assert.assertEquals(1, matchedData.size());
    Assert.assertEquals("junction", matchedData.get(0));
  }

  private static Record projectFlat(Schema projection, Record record) {
    Record result = GenericRecord.create(projection);
    List<Types.NestedField> fields = projection.asStruct().fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      result.set(i, record.getField(field.name()));
    }
    return result;
  }

  public static void assertEqualsUnsafe(Types.StructType struct,
                                        List<Record> expected, List<UnsafeRow> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      GenericsHelpers.assertEqualsUnsafe(struct, expected.get(i), actual.get(i));
    }
    Assert.assertEquals("Number of results should match expected", expected.size(), actual.size());
  }

  public static void assertEqualsSafe(Types.StructType struct,
                                      List<Record> expected, List<Row> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      GenericsHelpers.assertEqualsSafe(struct, expected.get(i), actual.get(i));
    }
    Assert.assertEquals("Number of results should match expected", expected.size(), actual.size());
  }

  private List<Record> expected(int... ordinals) {
    List<Record> expected = Lists.newArrayListWithExpectedSize(ordinals.length);
    for (int ord : ordinals) {
      expected.add(records.get(ord));
    }
    return expected;
  }

  private void pushFilters(ScanBuilder scan, Filter... filters) {
    Assert.assertTrue(scan instanceof SupportsPushDownFilters);
    SupportsPushDownFilters filterable = (SupportsPushDownFilters) scan;
    filterable.pushFilters(filters);
  }

  private Table buildPartitionedTable(String desc, PartitionSpec spec, String udf, String partitionColumn) {
    File location = new File(parent, desc);
    Table table = TABLES.create(SCHEMA, spec, location.toString());

    // Do not combine or split files because the tests expect a split per partition.
    // A target split size of 2048 helps us achieve that.
    table.updateProperties().set("read.split.target-size", "2048").commit();

    // copy the unpartitioned table into the partitioned table to produce the partitioned data
    Dataset<Row> allRows = spark.read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .load(unpartitioned.toString());

    allRows
        .coalesce(1) // ensure only 1 file per partition is written
        .withColumn("part", callUDF(udf, column(partitionColumn)))
        .sortWithinPartitions("part")
        .drop("part")
        .write()
        .format("iceberg")
        .mode("append")
        .save(table.location());

    table.refresh();

    return table;
  }

  private List<Record> testRecords(Schema schema) {
    return Lists.newArrayList(
        record(schema, 0L, parse("2017-12-22T09:20:44.294658+00:00"), "junction"),
        record(schema, 1L, parse("2017-12-22T07:15:34.582910+00:00"), "alligator"),
        record(schema, 2L, parse("2017-12-22T06:02:09.243857+00:00"), ""),
        record(schema, 3L, parse("2017-12-22T03:10:11.134509+00:00"), "clapping"),
        record(schema, 4L, parse("2017-12-22T00:34:00.184671+00:00"), "brush"),
        record(schema, 5L, parse("2017-12-21T22:20:08.935889+00:00"), "trap"),
        record(schema, 6L, parse("2017-12-21T21:55:30.589712+00:00"), "element"),
        record(schema, 7L, parse("2017-12-21T17:31:14.532797+00:00"), "limited"),
        record(schema, 8L, parse("2017-12-21T15:21:51.237521+00:00"), "global"),
        record(schema, 9L, parse("2017-12-21T15:02:15.230570+00:00"), "goldfish")
    );
  }

  private static List<Row> read(String table, boolean vectorized, String expr) {
    return read(table, vectorized, expr, "*");
  }

  private static List<Row> read(String table, boolean vectorized, String expr, String select0, String... selectN) {
    Dataset<Row> dataset = spark.read().format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .load(table).filter(expr)
        .select(select0, selectN);
    return dataset.collectAsList();
  }

  private static OffsetDateTime parse(String timestamp) {
    return OffsetDateTime.parse(timestamp);
  }

  private static Record record(Schema schema, Object... values) {
    Record rec = GenericRecord.create(schema);
    for (int i = 0; i < values.length; i += 1) {
      rec.set(i, values[i]);
    }
    return rec;
  }
}
