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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveTableBaseTest;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Enclosed.class)
public class TestSparkTableUtil extends HiveTableBaseTest {
  private static final String TABLE_NAME = "hive_table";
  private static final String QUALIFIED_TABLE_NAME = String.format("%s.%s", HiveTableBaseTest.DB_NAME, TABLE_NAME);
  private static final Path TABLE_LOCATION_PATH = HiveTableBaseTest.getTableLocationPath(TABLE_NAME);
  private static final String TABLE_LOCATION_STR = TABLE_LOCATION_PATH.toString();
  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    String metastoreURI = HiveTableBaseTest.hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname);

    // Create a spark session.
    TestSparkTableUtil.spark = SparkSession.builder().master("local[2]")
            .enableHiveSupport()
            .config("spark.hadoop.hive.metastore.uris", metastoreURI)
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkTableUtil.spark;
    // Stop the spark session.
    TestSparkTableUtil.spark = null;
    currentSpark.stop();
  }

  static void loadData(FileFormat fileFormat) {
    // Create a hive table.
    SQLContext sc = new SQLContext(TestSparkTableUtil.spark);

    sc.sql(String.format(
                    "CREATE TABLE %s (\n" +
                    "    id int COMMENT 'unique id'\n" +
                    ")\n" +
                    "PARTITIONED BY (data string)\n" +
                    "STORED AS %s\n" +
                    "LOCATION '%s'", QUALIFIED_TABLE_NAME, fileFormat, TABLE_LOCATION_STR)
    );

    List<SimpleRecord> expected = Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").orderBy("data").write()
            .mode("append")
            .insertInto(QUALIFIED_TABLE_NAME);
  }

  static void cleanupData() throws IOException {
    // Drop the hive table.
    SQLContext sc = new SQLContext(TestSparkTableUtil.spark);
    sc.sql(String.format("DROP TABLE IF EXISTS %s", QUALIFIED_TABLE_NAME));

    // Delete the data corresponding to the table.
    TABLE_LOCATION_PATH.getFileSystem(HiveTableBaseTest.hiveConf).delete(TABLE_LOCATION_PATH, true);
  }

  @RunWith(Parameterized.class)
  public static class TableImport {

    private final FileFormat format;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Parameterized.Parameters(name = "format = {0}")
    public static Object[] parameters() {
      return new Object[] { "parquet", "orc" };
    }

    public TableImport(String format) {
      this.format = FileFormat.valueOf(format.toUpperCase());
    }

    @Before
    public void before() {
      loadData(format);
    }

    @After
    public void after() throws IOException {
      cleanupData();
    }

    @Test
    public void testImportPartitionedTable() throws Exception {
      File location = temp.newFolder("partitioned_table");
      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").partitionBy("data").format(format.toString())
          .saveAsTable("test_partitioned_table");
      TableIdentifier source = spark.sessionState().sqlParser()
          .parseTableIdentifier("test_partitioned_table");
      HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
      Schema tableSchema = SparkSchemaUtil.schemaForTable(spark, QUALIFIED_TABLE_NAME);
      Table table = tables.create(tableSchema,
          SparkSchemaUtil.specForTable(spark, QUALIFIED_TABLE_NAME),
          ImmutableMap.of(),
          location.getCanonicalPath());
      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      long count = spark.read().format("iceberg").load(location.toString()).count();
      Assert.assertEquals("three values ", 3, count);
    }

    @Test
    public void testImportUnpartitionedTable() throws Exception {
      File location = temp.newFolder("unpartitioned_table");
      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").format(format.toString())
          .saveAsTable("test_unpartitioned_table");
      TableIdentifier source = spark.sessionState().sqlParser()
          .parseTableIdentifier("test_unpartitioned_table");
      HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
      Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, QUALIFIED_TABLE_NAME),
          SparkSchemaUtil.specForTable(spark, QUALIFIED_TABLE_NAME),
          ImmutableMap.of(),
          location.getCanonicalPath());
      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      long count = spark.read().format("iceberg").load(location.toString()).count();
      Assert.assertEquals("three values ", 3, count);
    }

    @Test
    public void testImportAsHiveTable() throws Exception {
      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").format(format.toString())
          .saveAsTable("unpartitioned_table");
      TableIdentifier source = new TableIdentifier("unpartitioned_table");
      org.apache.iceberg.catalog.TableIdentifier testUnpartitionedTableId =
          org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "test_unpartitioned_table_" + format);
      File stagingDir = temp.newFolder("staging-dir");
      Table table = catalog.createTable(
          testUnpartitionedTableId,
          SparkSchemaUtil.schemaForTable(spark, "unpartitioned_table"),
          SparkSchemaUtil.specForTable(spark, "unpartitioned_table"));

      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      long count1 = spark.read().format("iceberg").load(testUnpartitionedTableId.toString()).count();
      Assert.assertEquals("three values ", 3, count1);

      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").partitionBy("data").format(format.toString())
          .saveAsTable("partitioned_table");

      source = new TableIdentifier("partitioned_table");
      org.apache.iceberg.catalog.TableIdentifier testPartitionedTableId =
          org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "test_partitioned_table_" + format);
      table = catalog.createTable(
          testPartitionedTableId,
          SparkSchemaUtil.schemaForTable(spark, "partitioned_table"),
          SparkSchemaUtil.specForTable(spark, "partitioned_table"));

      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      long count2 = spark.read().format("iceberg").load(testPartitionedTableId.toString()).count();
      Assert.assertEquals("three values ", 3, count2);
    }

    @Test
    public void testImportWithNameMapping() throws Exception {
      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").format(format.toString())
          .saveAsTable("original_table");

      // The field is different so that it will project with name mapping
      Schema filteredSchema = new Schema(
          optional(1, "data", Types.StringType.get())
      );

      NameMapping nameMapping = MappingUtil.create(filteredSchema);

      String targetTableName = "target_table_" + format;
      TableIdentifier source = new TableIdentifier("original_table");
      org.apache.iceberg.catalog.TableIdentifier targetTable =
          org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, targetTableName);
      Table table = catalog.createTable(
          targetTable,
          filteredSchema,
          SparkSchemaUtil.specForTable(spark, "original_table"));

      table.updateProperties().set(DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping)).commit();

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());

      // The filter invoke the metric/dictionary row group filter in which it project schema
      // with name mapping again to match the metric read from footer.
      List<String> actual = spark.read().format("iceberg").load(targetTable.toString())
          .select("data")
          .sort("data")
          .filter("data >= 'b'")
          .as(Encoders.STRING())
          .collectAsList();

      List<String> expected = Lists.newArrayList("b", "c");

      Assert.assertEquals(expected, actual);
    }

    @Test
    public void testImportWithNameMappingForVectorizedParquetReader() throws Exception {
      Assume.assumeTrue("Applies only to parquet format.",
          FileFormat.PARQUET == format);
      spark.table(QUALIFIED_TABLE_NAME).write().mode("overwrite").format(format.toString())
          .saveAsTable("original_table");

      // The field is different so that it will project with name mapping
      Schema filteredSchema = new Schema(
          optional(1, "data", Types.StringType.get())
      );

      NameMapping nameMapping = MappingUtil.create(filteredSchema);

      TableIdentifier source = new TableIdentifier("original_table");
      Table table = catalog.createTable(
          org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "target_table_for_vectorization"),
          filteredSchema,
          SparkSchemaUtil.specForTable(spark, "original_table"));

      table.updateProperties()
          .set(DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
          .set(PARQUET_VECTORIZATION_ENABLED, "true")
          .commit();

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());

      // The filter invoke the metric/dictionary row group filter in which it project schema
      // with name mapping again to match the metric read from footer.
      List<String> actual = spark.read().format("iceberg")
          .load(DB_NAME + ".target_table_for_vectorization")
          .select("data")
          .sort("data")
          .filter("data >= 'b'")
          .as(Encoders.STRING())
          .collectAsList();

      List<String> expected = Lists.newArrayList("b", "c");

      Assert.assertEquals(expected, actual);
    }

    @Test
    public void testImportPartitionedWithWhitespace() throws Exception {
      String partitionCol = "dAtA sPaced";
      String spacedTableName = "whitespacetable";
      String whiteSpaceKey = "some key value";

      List<SimpleRecord> spacedRecords = Lists.newArrayList(new SimpleRecord(1, whiteSpaceKey));

      File icebergLocation = temp.newFolder("partitioned_table");

      spark.createDataFrame(spacedRecords, SimpleRecord.class)
          .withColumnRenamed("data", partitionCol)
          .write().mode("overwrite").partitionBy(partitionCol).format(format.toString())
          .saveAsTable(spacedTableName);

      TableIdentifier source = spark.sessionState().sqlParser()
          .parseTableIdentifier(spacedTableName);
      HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
      Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, spacedTableName),
          SparkSchemaUtil.specForTable(spark, spacedTableName),
          ImmutableMap.of(),
          icebergLocation.getCanonicalPath());
      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      List<SimpleRecord> results = spark.read().format("iceberg").load(icebergLocation.toString())
          .withColumnRenamed(partitionCol, "data")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Data should match", spacedRecords, results);
    }

    @Test
    public void testImportUnpartitionedWithWhitespace() throws Exception {
      String spacedTableName = "whitespacetable_" + format;
      String whiteSpaceKey = "some key value";

      List<SimpleRecord> spacedRecords = Lists.newArrayList(new SimpleRecord(1, whiteSpaceKey));

      File whiteSpaceOldLocation = temp.newFolder("white space location");
      File icebergLocation = temp.newFolder("partitioned_table");

      spark.createDataFrame(spacedRecords, SimpleRecord.class)
          .write().mode("overwrite").format(format.toString()).save(whiteSpaceOldLocation.getPath());

      spark.catalog().createExternalTable(spacedTableName, whiteSpaceOldLocation.getPath(), format.toString());

      TableIdentifier source = spark.sessionState().sqlParser()
          .parseTableIdentifier(spacedTableName);
      HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
      Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, spacedTableName),
          SparkSchemaUtil.specForTable(spark, spacedTableName),
          ImmutableMap.of(),
          icebergLocation.getCanonicalPath());
      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
      List<SimpleRecord> results = spark.read().format("iceberg").load(icebergLocation.toString())
          .as(Encoders.bean(SimpleRecord.class)).collectAsList();

      Assert.assertEquals("Data should match", spacedRecords, results);
    }
  }

  public static class GetPartitions {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    // This logic does not really depend on format
    private final FileFormat format = FileFormat.PARQUET;

    @Test
    public void testPartitionScan() throws Exception {

      List<ThreeColumnRecord> records = Lists.newArrayList(
          new ThreeColumnRecord(1, "ab", "data"),
          new ThreeColumnRecord(2, "b c", "data"),
          new ThreeColumnRecord(1, "b c", "data"),
          new ThreeColumnRecord(2, "ab", "data"));

      String tableName = "external_table";

      spark.createDataFrame(records, ThreeColumnRecord.class)
          .write().mode("overwrite").format(format.toString())
          .partitionBy("c1", "c2").saveAsTable(tableName);

      TableIdentifier source = spark.sessionState().sqlParser()
          .parseTableIdentifier(tableName);

      Map<String, String> partition1 = ImmutableMap.of(
          "c1", "1",
          "c2", "ab");
      Map<String, String> partition2 = ImmutableMap.of(
          "c1", "2",
          "c2", "b c");
      Map<String, String> partition3 = ImmutableMap.of(
          "c1", "1",
          "c2", "b c");
      Map<String, String> partition4 = ImmutableMap.of(
          "c1", "2",
          "c2", "ab");

      List<SparkPartition> partitionsC11 =
          SparkTableUtil.getPartitions(spark, source, ImmutableMap.of("c1", "1"));
      Set<Map<String, String>> expectedC11 =
          Sets.newHashSet(partition1, partition3);
      Set<Map<String, String>> actualC11 = partitionsC11.stream().map(
          p -> p.getValues()).collect(Collectors.toSet());
      Assert.assertEquals("Wrong partitions fetched for c1=1", expectedC11, actualC11);

      List<SparkPartition> partitionsC12 =
          SparkTableUtil.getPartitions(spark, source, ImmutableMap.of("c1", "2"));
      Set<Map<String, String>> expectedC12 = Sets.newHashSet(partition2, partition4);
      Set<Map<String, String>> actualC12 = partitionsC12.stream().map(
          p -> p.getValues()).collect(Collectors.toSet());
      Assert.assertEquals("Wrong partitions fetched for c1=2", expectedC12, actualC12);

      List<SparkPartition> partitionsC21 =
          SparkTableUtil.getPartitions(spark, source, ImmutableMap.of("c2", "ab"));
      Set<Map<String, String>> expectedC21 =
          Sets.newHashSet(partition1, partition4);
      Set<Map<String, String>> actualC21 = partitionsC21.stream().map(
          p -> p.getValues()).collect(Collectors.toSet());
      Assert.assertEquals("Wrong partitions fetched for c2=ab", expectedC21, actualC21);

      List<SparkPartition> partitionsC22 =
          SparkTableUtil.getPartitions(spark, source, ImmutableMap.of("c2", "b c"));
      Set<Map<String, String>> expectedC22 =
          Sets.newHashSet(partition2, partition3);
      Set<Map<String, String>> actualC22 = partitionsC22.stream().map(
          p -> p.getValues()).collect(Collectors.toSet());
      Assert.assertEquals("Wrong partitions fetched for c2=b c", expectedC22, actualC22);
    }
  }

  public static class PartitionScan {

    @Before
    public void before() {
      loadData(FileFormat.PARQUET);
    }

    @After
    public void after() throws IOException {
      cleanupData();
    }

    @Test
    public void testPartitionScan() {
      List<SparkPartition> partitions = SparkTableUtil.getPartitions(spark, QUALIFIED_TABLE_NAME);
      Assert.assertEquals("There should be 3 partitions", 3, partitions.size());

      Dataset<Row> partitionDF = SparkTableUtil.partitionDF(spark, QUALIFIED_TABLE_NAME);
      Assert.assertEquals("There should be 3 partitions", 3, partitionDF.count());
    }

    @Test
    public void testPartitionScanByFilter() {
      List<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, QUALIFIED_TABLE_NAME, "data = 'a'");
      Assert.assertEquals("There should be 1 matching partition", 1, partitions.size());

      Dataset<Row> partitionDF = SparkTableUtil.partitionDFByFilter(spark, QUALIFIED_TABLE_NAME, "data = 'a'");
      Assert.assertEquals("There should be 1 matching partition", 1, partitionDF.count());
    }
  }
}
