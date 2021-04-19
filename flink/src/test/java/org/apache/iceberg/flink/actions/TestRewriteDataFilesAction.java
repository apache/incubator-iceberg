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


package org.apache.iceberg.flink.actions;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkTableOptions;
import org.apache.iceberg.flink.FlinkTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.RECORD;

@RunWith(Parameterized.class)
public class TestRewriteDataFilesAction {
  private static final AtomicInteger FILE_COUNT = new AtomicInteger(1);
  private static final Configuration CONF = new Configuration();
  private static final String CATALOG = "test_catalog";
  private static final String DATABASE = "test_database";

  @ClassRule
  public static final TemporaryFolder HADOOP_WAREHOUSE = new TemporaryFolder();

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  private static final String TABLE_NAME_UNPARTITIONED = "test_table_unpartitioned";
  private static final String TABLE_NAME_PARTITIONED = "test_table_partitioned";

  private final FileFormat format;
  private final int formatVersion;
  private final TableEnvironment tEnv;

  private Table icebergTableUnPartitioned;
  private Table icebergTablePartitioned;

  @Parameterized.Parameters(name = "format={0}, formatVersion={1}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"orc", 1},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2}
    );
  }

  public TestRewriteDataFilesAction(String format, int formatVersion) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
    this.tEnv = TableEnvironment.create(
        EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inBatchMode()
            .build()
    );
    this.tEnv.getConfig()
        .getConfiguration()
        .set(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false)
        .set(CoreOptions.DEFAULT_PARALLELISM, 1);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() {
    String warehouseRoot = String.format("file://%s", HADOOP_WAREHOUSE.getRoot().getAbsolutePath());
    // Create catalog and use it.
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", CATALOG, warehouseRoot);
    sql("USE CATALOG %s", CATALOG);

    // Create database and use it.
    sql("CREATE DATABASE %s", DATABASE);
    sql("USE %s", DATABASE);

    // Create tables.
    sql("CREATE TABLE %s (id int, data varchar) " +
            "      WITH (" +
            "         'write.format.default'='%s'" +
            "      )",
        TABLE_NAME_UNPARTITIONED, format.name());
    sql("CREATE TABLE %s (id int, data varchar,spec varchar) " +
            "      PARTITIONED BY (data,spec)" +
            "      WITH (" +
            "         'write.format.default'='%s'" +
            "      )",
        TABLE_NAME_PARTITIONED, format.name());

    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(CONF);
    catalog.initialize("hadoop_catalog", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot));

    icebergTableUnPartitioned = catalog.loadTable(TableIdentifier.of(DATABASE, TABLE_NAME_UNPARTITIONED));
    upgradeToFormatVersion(icebergTableUnPartitioned, formatVersion);

    icebergTablePartitioned = catalog.loadTable(TableIdentifier.of(DATABASE, TABLE_NAME_PARTITIONED));
    upgradeToFormatVersion(icebergTablePartitioned, formatVersion);
  }

  private void upgradeToFormatVersion(Table table, int version) {
    if (version > 1) {
      TableOperations ops = ((BaseTable) table).operations();
      TableMetadata meta = ops.current();
      ops.commit(meta, meta.upgradeToFormatVersion(version));
    }
  }

  private List<Row> sql(String query, Object... args) {
    try (CloseableIterator<Row> iter = FlinkTestBase.exec(tEnv, query, args).collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s.%s", CATALOG, DATABASE, TABLE_NAME_UNPARTITIONED);
    sql("DROP TABLE IF EXISTS %s.%s.%s", CATALOG, DATABASE, TABLE_NAME_PARTITIONED);
    sql("DROP DATABASE IF EXISTS %s.%s", CATALOG, DATABASE);
    sql("DROP CATALOG IF EXISTS %s", CATALOG);
  }

  @Test
  public void testRewriteDataFilesEmptyTable() throws Exception {
    Assert.assertNull("Table must be empty", icebergTableUnPartitioned.currentSnapshot());
    Actions.forTable(icebergTableUnPartitioned)
        .rewriteDataFiles()
        .execute();
    Assert.assertNull("Table must stay empty", icebergTableUnPartitioned.currentSnapshot());
  }


  @Test
  public void testRewriteDataFilesUnpartitionedTable() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files before rewrite", 2, dataFiles.size());

    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTableUnPartitioned)
            .rewriteDataFiles()
            .execute();

    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 1 data files after rewrite", 1, dataFiles1.size());

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(icebergTableUnPartitioned, Lists.newArrayList(
        SimpleDataUtil.createRecord(1, "hello"),
        SimpleDataUtil.createRecord(2, "world")
    ));
  }

  @Test
  public void testRewriteDataFilesPartitionedTable() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 3, 'world' ,'b'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 4, 'world' ,'b'", TABLE_NAME_PARTITIONED);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles.size());

    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTablePartitioned)
            .rewriteDataFiles()
            .execute();

    Assert.assertEquals("Action should rewrite 4 data files", 4, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 2 data file", 2, result.addedDataFiles().size());

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files after rewrite", 2, dataFiles1.size());

    // Assert the table records as expected.
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "spec", Types.StringType.get())
    );

    Record record = GenericRecord.create(schema);
    SimpleDataUtil.assertTableRecords(icebergTablePartitioned, Lists.newArrayList(
        record.copy("id", 1, "data", "hello", "spec", "a"),
        record.copy("id", 2, "data", "hello", "spec", "a"),
        record.copy("id", 3, "data", "world", "spec", "b"),
        record.copy("id", 4, "data", "world", "spec", "b")
    ));
  }


  @Test
  public void testRewriteDataFilesWithFilter() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 3, 'world' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 4, 'world' ,'b'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 5, 'world' ,'b'", TABLE_NAME_PARTITIONED);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 5 data files before rewrite", 5, dataFiles.size());

    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTablePartitioned)
            .rewriteDataFiles()
            .filter(Expressions.equal("spec", "a"))
            .filter(Expressions.startsWith("data", "he"))
            .execute();

    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files after rewrite", 4, dataFiles1.size());

    // Assert the table records as expected.
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "spec", Types.StringType.get())
    );

    Record record = GenericRecord.create(schema);
    SimpleDataUtil.assertTableRecords(icebergTablePartitioned, Lists.newArrayList(
        record.copy("id", 1, "data", "hello", "spec", "a"),
        record.copy("id", 2, "data", "hello", "spec", "a"),
        record.copy("id", 3, "data", "world", "spec", "a"),
        record.copy("id", 4, "data", "world", "spec", "b"),
        record.copy("id", 5, "data", "world", "spec", "b")
    ));
  }

  @Test
  public void testRewriteLargeTableHasResiduals() throws IOException {
    // all records belong to the same partition
    List<String> records1 = Lists.newArrayList();
    List<String> records2 = Lists.newArrayList();
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      int id = i;
      String data = String.valueOf(i % 3);
      if (i % 2 == 0) {
        records1.add("(" + id + ",'" + data + "')");
      } else {
        records2.add("(" + id + ",'" + data + "')");
      }
      Record record = RECORD.copy();
      record.setField("id", id);
      record.setField("data", data);
      expected.add(record);
    }

    sql("INSERT INTO %s values " + StringUtils.join(records1, ","), TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s values " + StringUtils.join(records2, ","), TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTableUnPartitioned.newScan()
        .ignoreResiduals()
        .filter(Expressions.equal("data", "0"))
        .planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files before rewrite", 2, dataFiles.size());

    Actions actions = Actions.forTable(icebergTableUnPartitioned);

    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .filter(Expressions.equal("data", "0"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(icebergTableUnPartitioned, expected);
  }

  /**
   * a test case to test avoid repeate compress
   * <p>
   * If datafile cannot be combined to CombinedScanTask with other DataFiles, the size of the CombinedScanTask list size
   * is 1, so we remove these CombinedScanTasks to avoid compressed repeatedly.
   * <p>
   * In this test case,we generated 3 data files and set targetSizeInBytes greater than the largest file size so that it
   * cannot be  combined a CombinedScanTask with other datafiles. The datafile with the largest file size will not be
   * compressed.
   *
   * @throws IOException IOException
   */
  @Test
  public void testRewriteAvoidRepeateCompress() throws IOException {
    Assume.assumeFalse("ORC does not support getting length when file is opening", format.equals(FileFormat.ORC));
    List<Record> expected = Lists.newArrayList();
    Schema schema = icebergTableUnPartitioned.schema();
    GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema);
    File file = temp.newFile();
    int count = 0;
    try (FileAppender<Record> fileAppender = genericAppenderFactory.newAppender(Files.localOutput(file), format)) {
      long filesize = 20000;
      for (; fileAppender.length() < filesize; count++) {
        Record record = SimpleDataUtil.createRecord(count, "iceberg");
        fileAppender.add(record);
        expected.add(record);
      }
    }

    DataFile dataFile = DataFiles.builder(icebergTableUnPartitioned.spec())
        .withPath(file.getAbsolutePath())
        .withFileSizeInBytes(file.length())
        .withFormat(format)
        .withRecordCount(count)
        .build();

    icebergTableUnPartitioned.newAppend()
        .appendFile(dataFile)
        .commit();

    sql("INSERT INTO %s SELECT 1,'a' ", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2,'b' ", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 3 data files before rewrite", 3, dataFiles.size());

    Actions actions = Actions.forTable(icebergTableUnPartitioned);

    long targetSizeInBytes = file.length() + 10;
    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .targetSizeInBytes(targetSizeInBytes)
        .splitOpenFileCost(1)
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFilesRewrote = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files after rewrite", 2, dataFilesRewrote.size());

    // the biggest file do not be rewrote
    List rewroteDataFileNames = dataFilesRewrote.stream().map(ContentFile::path).collect(Collectors.toList());
    Assert.assertTrue(rewroteDataFileNames.contains(file.getAbsolutePath()));

    // Assert the table records as expected.
    expected.add(SimpleDataUtil.createRecord(1, "a"));
    expected.add(SimpleDataUtil.createRecord(2, "b"));
    SimpleDataUtil.assertTableRecords(icebergTableUnPartitioned, expected);
  }

  private OutputFile newOutputFile() {
    return HadoopOutputFile.fromLocation(String.format("file://%s/%s.%s",
        HADOOP_WAREHOUSE.getRoot().getAbsolutePath(), FILE_COUNT.incrementAndGet(), format), CONF);
  }

  @Test
  public void testRewriteDeleteInUnpartitionedTable() throws IOException {
    Assume.assumeTrue("Iceberg format v1 does not support row-level delete.", formatVersion > 1);
    sql("INSERT INTO %s VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC') ", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s VALUES (4, 'DDD'), (5, 'EEE'), (6, 'FFF') ", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s VALUES (7, 'GGG'), (8, 'HHH'), (9, 'III') ", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    Schema deleteRowSchema = icebergTableUnPartitioned.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);

    List<Record> deletions = Lists.newArrayList(
        dataDelete.copy("data", "BBB"), // id = 2
        dataDelete.copy("data", "EEE"), // id = 5
        dataDelete.copy("data", "HHH")  // id = 8
    );
    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(icebergTableUnPartitioned, newOutputFile(),
        deletions, deleteRowSchema);

    icebergTableUnPartitioned.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    assertSetsEqual(Lists.newArrayList(
        Row.of(1, "AAA"),
        Row.of(3, "CCC"),
        Row.of(4, "DDD"),
        Row.of(6, "FFF"),
        Row.of(7, "GGG"),
        Row.of(9, "III")),
        sql("SELECT * FROM %s", TABLE_NAME_UNPARTITIONED)
    );

    List<FileScanTask> tasks = Lists.newArrayList(icebergTableUnPartitioned.newScan().planFiles());
    Assert.assertEquals("Should have 3 data files", 3, tasks.size());
    Set<DeleteFile> deleteFiles = tasks.stream()
        .map(FileScanTask::deletes)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
    Assert.assertEquals("Should have 1 equality delete files.", 1, deleteFiles.size());

    // Rewrite the files.
    RewriteDataFilesActionResult result = Actions.forTable(icebergTableUnPartitioned)
        .rewriteDataFiles()
        .execute();

    Assert.assertEquals(3, result.deletedDataFiles().size());
    Assert.assertEquals(1, result.deletedDeleteFiles().size());
    Assert.assertEquals(1, result.addedDataFiles().size());
    Assert.assertEquals(0, result.addedDeleteFiles().size());
    Assert.assertEquals(1, Lists.newArrayList(icebergTableUnPartitioned.newScan().planFiles()).size());

    // Assert rows in the final rewritten iceberg table.
    assertSetsEqual(Lists.newArrayList(
        Row.of(1, "AAA"),
        Row.of(3, "CCC"),
        Row.of(4, "DDD"),
        Row.of(6, "FFF"),
        Row.of(7, "GGG"),
        Row.of(9, "III")),
        sql("SELECT * FROM %s", TABLE_NAME_UNPARTITIONED)
    );
  }

  @Test
  public void testRewriteDeleteInPartitionedTable() throws IOException {
    Assume.assumeTrue("Iceberg format v1 does not support row-level delete.", formatVersion > 1);
    sql("INSERT INTO %s VALUES (1, 'AAA', 'p1'), (2, 'BBB', 'p2'), (3, 'CCC', 'p3') ", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s VALUES (4, 'AAA', 'p1'), (5, 'BBB', 'p2'), (6, 'CCC', 'p3') ", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s VALUES (7, 'AAA', 'p1'), (8, 'BBB', 'p2'), (9, 'CCC', 'p3') ", TABLE_NAME_PARTITIONED);

    icebergTablePartitioned.refresh();

    Schema deleteRowSchema = icebergTablePartitioned.schema().select("id", "data", "spec");
    Record dataDelete = GenericRecord.create(deleteRowSchema);

    List<Record> deletions = Lists.newArrayList(
        dataDelete.copy("id", 4, "data", "AAA", "spec", "p1"), // id = 4
        dataDelete.copy("id", 5, "data", "BBB", "spec", "p2"), // id = 5
        dataDelete.copy("id", 6, "data", "CCC", "spec", "p3")  // id = 6
    );

    DeleteFile eqDeletes1 = FileHelpers.writeDeleteFile(icebergTablePartitioned, newOutputFile(),
        TestHelpers.Row.of("AAA", "p1"), deletions.subList(0, 1), deleteRowSchema);

    DeleteFile eqDeletes2 = FileHelpers.writeDeleteFile(icebergTablePartitioned, newOutputFile(),
        TestHelpers.Row.of("BBB", "p2"), deletions.subList(1, 2), deleteRowSchema);

    DeleteFile eqDeletes3 = FileHelpers.writeDeleteFile(icebergTablePartitioned, newOutputFile(),
        TestHelpers.Row.of("CCC", "p3"), deletions.subList(2, 3), deleteRowSchema);

    icebergTablePartitioned.newRowDelta()
        .addDeletes(eqDeletes1)
        .addDeletes(eqDeletes2)
        .addDeletes(eqDeletes3)
        .commit();

    assertSetsEqual(Lists.newArrayList(
        Row.of(1, "AAA", "p1"),
        Row.of(2, "BBB", "p2"),
        Row.of(3, "CCC", "p3"),
        Row.of(7, "AAA", "p1"),
        Row.of(8, "BBB", "p2"),
        Row.of(9, "CCC", "p3")),
        sql("SELECT * FROM %s", TABLE_NAME_PARTITIONED)
    );

    List<FileScanTask> tasks = Lists.newArrayList(icebergTablePartitioned.newScan().planFiles());
    Assert.assertEquals("Should have 9 data files", 9, tasks.size());
    Set<DeleteFile> deleteFiles = tasks.stream()
        .map(FileScanTask::deletes)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
    Assert.assertEquals("Should have 3 equality delete files.", 3, deleteFiles.size());

    // Rewrite the files.
    RewriteDataFilesActionResult result = Actions.forTable(icebergTablePartitioned)
        .rewriteDataFiles()
        .execute();

    Assert.assertEquals(9, result.deletedDataFiles().size());
    Assert.assertEquals(3, result.deletedDeleteFiles().size());
    Assert.assertEquals(3, result.addedDataFiles().size());
    Assert.assertEquals(0, result.addedDeleteFiles().size());
    Assert.assertEquals(3, Lists.newArrayList(icebergTablePartitioned.newScan().planFiles()).size());

    // Assert rows in the final rewritten iceberg table.
    assertSetsEqual(Lists.newArrayList(
        Row.of(1, "AAA", "p1"),
        Row.of(2, "BBB", "p2"),
        Row.of(3, "CCC", "p3"),
        Row.of(7, "AAA", "p1"),
        Row.of(8, "BBB", "p2"),
        Row.of(9, "CCC", "p3")),
        sql("SELECT * FROM %s", TABLE_NAME_PARTITIONED)
    );
  }

  private void assertSetsEqual(Iterable<Row> expected, Iterable<Row> actual) {
    Assert.assertEquals("Should have same elements", Sets.newHashSet(expected), Sets.newHashSet(actual));
  }
}
