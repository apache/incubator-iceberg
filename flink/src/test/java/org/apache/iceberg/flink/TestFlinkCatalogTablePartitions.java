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

package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkCatalogTablePartitions extends FlinkTestBase {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private static String warehouse;
  private boolean cacheEnabled;

  public TestFlinkCatalogTablePartitions(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
  }

  @Parameterized.Parameters(name = "cacheEnabled={0}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (Boolean cacheEnabled : new Boolean[] {true, false}) {
      parameters.add(new Object[] {cacheEnabled});
    }
    return parameters;
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s', 'cache-enabled' = '%s')",
        CATALOG_NAME, warehouse, cacheEnabled);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
  }

  @Test
  public void testListPartitionsWithUnpartitionedTable() {
    sql("CREATE TABLE %s (id INT, data VARCHAR) with ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    FlinkCatalog flinkCatalog = (FlinkCatalog) getTableEnv().getCatalog(CATALOG_NAME).get();
    AssertHelpers.assertThrows("Should not list partitions for unpartitioned table.",
        TableNotPartitionedException.class, () -> flinkCatalog.listPartitions(objectPath));
  }

  @Test
  public void testListPartitionsWithPartitionedTable() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data VARCHAR) PARTITIONED BY (data) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s SELECT 1,'a'", TABLE_NAME);
    sql("INSERT INTO %s SELECT 2,'b'", TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));
    CatalogPartitionSpec partitionSpec2 = new CatalogPartitionSpec(ImmutableMap.of("data", "b"));
    expected.add(partitionSpec1);
    expected.add(partitionSpec2);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithStringPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data VARCHAR,t TIMESTAMP) PARTITIONED BY (data,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1, 'a',TO_TIMESTAMP('2021-01-01 12:13:14'))," +
            "(2, 'b',TO_TIMESTAMP('2021-01-01 12:13:14'))," +
            "(3, 'a',TO_TIMESTAMP('2021-01-02 15:16:17'))," +
            "(4, 'b',TO_TIMESTAMP('2021-01-02 15:16:17'))",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("data", "a"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partitions", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec =
        new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "2021-01-01T12:13:14"));
    CatalogPartitionSpec partitionSpec1 =
        new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "2021-01-02T15:16:17"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithIntegerPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, d INT, h INT) PARTITIONED BY (d,h) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES (1,20210101,10),(2,20210101,2),(3,20210102,12),(4,20210102,3)", TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("d", "20210101"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("d", "20210101", "h", "2"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("d", "20210101", "h", "10"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithDoublePartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, d DATE, h DOUBLE) PARTITIONED BY (d,h) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1,DATE '2021-01-01',10)," +
            "(2,DATE '2021-01-01',11)," +
            "(3,DATE '2021-01-02',10)," +
            "(4,DATE '2021-01-02',11)",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("h", "10"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01", "h", "10.0"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-02", "h", "10.0"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithFloatPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, d DATE, h FLOAT) PARTITIONED BY (d,h) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1,DATE '2021-01-01',10)," +
            "(2,DATE '2021-01-01',11)," +
            "(3,DATE '2021-01-02',10)," +
            "(4,DATE '2021-01-02',11)",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("h", "10"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01", "h", "10.0"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-02", "h", "10.0"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithDatePartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, d DATE,h INT) PARTITIONED BY (d,h) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1, DATE '2021-01-01',10)," +
            "(2, DATE '2021-01-02',10)," +
            "(2, DATE '2021-01-01',11)," +
            "(2, DATE '2021-01-02',11)",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01", "h", "10"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01", "h", "11"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithTimePartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, d DATE, t TIME) PARTITIONED BY (d,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1, DATE '2021-01-01',TIME '12:13:14')," +
            "(2, DATE '2021-01-02',TIME '12:13:14')," +
            "(2, DATE '2021-01-01',TIME '15:16:17')," +
            "(2, DATE '2021-01-02',TIME '15:16:17')",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("t", "12:13:14"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-01", "t", "12:13:14"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("d", "2021-01-02", "t", "12:13:14"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithTimestampPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data STRING, t TIMESTAMP) PARTITIONED BY (data,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1, 'a',TO_TIMESTAMP('2021-01-01 12:13:14'))," +
            "(2, 'b',TO_TIMESTAMP('2021-01-01 12:13:14'))," +
            "(3, 'a',TO_TIMESTAMP('2021-01-02 15:16:17'))," +
            "(4, 'b',TO_TIMESTAMP('2021-01-02 15:16:17'))",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("t", "2021-01-01T12:13:14"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec =
        new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "2021-01-01T12:13:14"));
    CatalogPartitionSpec partitionSpec1 =
        new CatalogPartitionSpec(ImmutableMap.of("data", "b", "t", "2021-01-01T12:13:14"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithbBooleanPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data String, t BOOLEAN) PARTITIONED BY (data,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES (1,'a',true), (2,'b',true), (3,'a',false),(4,'b',false)", TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("t", "true"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "true"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "b", "t", "true"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithbDecimalPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data String, t DECIMAL) PARTITIONED BY (data,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES (1,'a',10), (2,'b',10), (3,'a',11),(4,'b',11)", TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("t", "10"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "10"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "b", "t", "10"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }

  @Test
  public void testListPartitionsWithbVarBinaryPartitions() throws TableNotExistException, TableNotPartitionedException {
    sql("CREATE TABLE %s (id INT, data String, t VARBINARY(100)) PARTITIONED BY (data,t) " +
        "with ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES " +
            "(1,'a',ENCODE('hello','UTF-8')), " +
            "(2,'b',ENCODE('hello','UTF-8')), " +
            "(3,'a',ENCODE('world','UTF-8'))," +
            "(4,'b',ENCODE('world','UTF-8'))",
        TABLE_NAME);

    ObjectPath objectPath = new ObjectPath(DATABASE_NAME, TABLE_NAME);
    Optional<Catalog> catalog = getTableEnv().getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();

    CatalogPartitionSpec partitionParam = new CatalogPartitionSpec(ImmutableMap.of("t", "hello"));
    List<CatalogPartitionSpec> list = flinkCatalog.listPartitions(objectPath, partitionParam);
    Assert.assertEquals("Should have 2 partition", 2, list.size());

    List<CatalogPartitionSpec> expected = Lists.newArrayList();
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(ImmutableMap.of("data", "a", "t", "hello"));
    CatalogPartitionSpec partitionSpec1 = new CatalogPartitionSpec(ImmutableMap.of("data", "b", "t", "hello"));
    expected.add(partitionSpec);
    expected.add(partitionSpec1);
    Assert.assertEquals("Should produce the expected catalog partition specs.", expected, list);
  }
}
