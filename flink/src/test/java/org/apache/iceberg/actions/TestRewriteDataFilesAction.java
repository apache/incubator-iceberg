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


package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRewriteDataFilesAction extends FlinkCatalogTestBase {

  private static final String TABLE_NAME_UNPARTIITONED = "test_table_unpartitioned";
  private static final String TABLE_NAME_PARTIITONED = "test_table_partitioned";
  private final FileFormat format;
  private Table icebergTableUnPartitioned;
  private Table icebergTablePartitioned;

  public TestRewriteDataFilesAction(String catalogName, String[] baseNamespace, FileFormat format) {
    super(catalogName, baseNamespace);
    this.format = format;
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        String[] baseNamespace = (String[]) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }
    return parameters;
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s')", TABLE_NAME_UNPARTIITONED,
        format.name());
    icebergTableUnPartitioned = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace,
        TABLE_NAME_UNPARTIITONED));

    sql("CREATE TABLE %s (id int, data varchar)  PARTITIONED BY (data) with ('write.format.default'='%s')",
        TABLE_NAME_PARTIITONED, format.name());
    icebergTablePartitioned = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace,
        TABLE_NAME_PARTIITONED));
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_UNPARTIITONED);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_PARTIITONED);
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
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTIITONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTIITONED);

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
    sql("INSERT INTO %s SELECT 1, 'hello' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 2, 'hello' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 3, 'world' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 4, 'world' ", TABLE_NAME_PARTIITONED);

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
    SimpleDataUtil.assertTableRecords(icebergTablePartitioned, Lists.newArrayList(
        SimpleDataUtil.createRecord(1, "hello"),
        SimpleDataUtil.createRecord(2, "hello"),
        SimpleDataUtil.createRecord(3, "world"),
        SimpleDataUtil.createRecord(4, "world")
    ));
  }


  @Test
  public void testRewriteDataFilesWithFilter() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 1, 'hello' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 1, 'world' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 2, 'world' ", TABLE_NAME_PARTIITONED);
    sql("INSERT INTO %s SELECT 3, 'world' ", TABLE_NAME_PARTIITONED);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 5 data files before rewrite", 5, dataFiles.size());

    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTablePartitioned)
            .rewriteDataFiles()
            .filter(Expressions.equal("id", 1))
            .filter(Expressions.startsWith("data", "he"))
            .execute();

    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files after rewrite", 4, dataFiles1.size());

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(icebergTablePartitioned, Lists.newArrayList(
        SimpleDataUtil.createRecord(1, "hello"),
        SimpleDataUtil.createRecord(1, "hello"),
        SimpleDataUtil.createRecord(1, "world"),
        SimpleDataUtil.createRecord(2, "world"),
        SimpleDataUtil.createRecord(3, "world")
    ));
  }
}
