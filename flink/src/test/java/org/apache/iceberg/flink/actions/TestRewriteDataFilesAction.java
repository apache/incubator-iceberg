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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.RECORD;

@RunWith(Parameterized.class)
public class TestRewriteDataFilesAction extends FlinkCatalogTestBase {

  private static final String TABLE_NAME_UNPARTITIONED = "test_table_unpartitioned";
  private static final String TABLE_NAME_PARTITIONED = "test_table_partitioned";
  private final FileFormat format;
  private Table icebergTableUnPartitioned;
  private Table icebergTablePartitioned;

  public TestRewriteDataFilesAction(String catalogName, Namespace baseNamespace, FileFormat format) {
    super(catalogName, baseNamespace);
    this.format = format;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        Namespace baseNamespace = (Namespace) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }
    return parameters;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s')", TABLE_NAME_UNPARTITIONED,
        format.name());
    icebergTableUnPartitioned = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace,
        TABLE_NAME_UNPARTITIONED));

    sql("CREATE TABLE %s (id int, data varchar,spec varchar) " +
            " PARTITIONED BY (data,spec) with ('write.format.default'='%s')",
        TABLE_NAME_PARTITIONED, format.name());
    icebergTablePartitioned = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace,
        TABLE_NAME_PARTITIONED));
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_UNPARTITIONED);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_PARTITIONED);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
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
    GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(icebergTableUnPartitioned);
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
}
