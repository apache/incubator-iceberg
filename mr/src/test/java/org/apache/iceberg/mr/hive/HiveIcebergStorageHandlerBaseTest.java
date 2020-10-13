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

package org.apache.iceberg.mr.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(StandaloneHiveRunner.class)
public abstract class HiveIcebergStorageHandlerBaseTest {

  private static final String DEFAULT_DATABASE_NAME = "default";

  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema CUSTOMER_SCHEMA = new Schema(
          required(1, "customer_id", Types.LongType.get()),
          required(2, "first_name", Types.StringType.get())
  );

  private static final List<Record> CUSTOMER_RECORDS = TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
          .add(0L, "Alice")
          .add(1L, "Bob")
          .add(2L, "Trudy")
          .build();

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()));

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d)
          .add(101L, 0L, 22.22d)
          .add(102L, 1L, 33.33d)
          .build();

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  protected static TestHiveMetastore metastore;

  private TestTables testTables;

  public abstract TestTables testTables(Configuration conf, TemporaryFolder tmp) throws IOException;


  @BeforeClass
  public static void beforeClass() {
    metastore = new TestHiveMetastore();
    metastore.start();
  }

  @AfterClass
  public static void afterClass() {
    metastore.stop();
    metastore = null;
  }

  @Before
  public void before() throws IOException {
    String metastoreUris = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);
    // in Hive3, setting this as a system prop ensures that it will be picked up whenever a new HiveConf is created
    System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);

    testTables = testTables(metastore.hiveConf(), temp);

    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveConfValue(property.getKey(), property.getValue());
    }

    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);
    String metastoreWarehouse = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, metastoreWarehouse);

    shell.start();
  }

  @After
  public void after() throws Exception {
    Hive db = Hive.get(metastore.hiveConf());
    for (String dbName : db.getAllDatabases()) {
      for (String tblName : db.getAllTables(dbName)) {
        db.dropTable(dbName, tblName);
      }
      if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
        // Drop cascade, functions dropped by cascade
        db.dropDatabase(dbName, true, true, true);
      }
    }
  }

  // PARQUET

  @Test
  public void testScanEmptyTableParquet() throws IOException {
    testScanEmptyTable(FileFormat.PARQUET);
  }

  @Test
  public void testScanTableParquet() throws IOException {
    testScanTable(FileFormat.PARQUET);
  }

  @Test
  public void testJoinTablesParquet() throws IOException {
    testJoinTables(FileFormat.PARQUET);
  }

  // ORC

  @Test
  public void testScanEmptyTableORC() throws IOException {
    testScanEmptyTable(FileFormat.ORC);
  }

  @Test
  public void testScanTableORC() throws IOException {
    testScanTable(FileFormat.ORC);
  }

  @Test
  public void testJoinTablesORC() throws IOException {
    testJoinTables(FileFormat.ORC);
  }

  // AVRO

  @Test
  public void testScanEmptyTableAvro() throws IOException {
    testScanEmptyTable(FileFormat.AVRO);
  }

  @Test
  public void testScanTableAvro() throws IOException {
    testScanTable(FileFormat.AVRO);
  }

  @Test
  public void testJoinTablesAvro() throws IOException {
    testJoinTables(FileFormat.AVRO);
  }

  public void testScanEmptyTable(FileFormat format) throws IOException {
    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    createTable("empty", emptySchema, format, ImmutableList.of());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  public void testScanTable(FileFormat format) throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, format, CUSTOMER_RECORDS);

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, rows.get(2));

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
  }

  public void testJoinTables(FileFormat format) throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, format, CUSTOMER_RECORDS);
    createTable("orders", ORDER_SCHEMA, format, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
            "SELECT c.customer_id, c.first_name, o.order_id, o.total " +
                    "FROM default.customers c JOIN default.orders o ON c.customer_id = o.customer_id " +
                    "ORDER BY c.customer_id, o.order_id"
    );

    Assert.assertArrayEquals(new Object[] {0L, "Alice", 100L, 11.11d}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", 101L, 22.22d}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", 102L, 33.33d}, rows.get(2));
  }

  protected void createTable(String tableName, Schema schema, FileFormat format, List<Record> records)
          throws IOException {
    Table table = createIcebergTable(tableName, schema, format, records);
    createHiveTable(tableName, table.location());
  }

  protected Table createIcebergTable(String tableName, Schema schema, FileFormat format, List<Record> records)
          throws IOException {
    String identifier = testTables.identifier("default." + tableName);
    TestHelper helper = new TestHelper(
            metastore.hiveConf(), testTables.tables(), identifier, schema, SPEC, format, temp);
    Table table = helper.createTable();

    if (!records.isEmpty()) {
      helper.appendToTable(helper.writeFile(null, records));
    }

    return table;
  }

  protected void createHiveTable(String tableName, String location) {
    shell.execute(String.format(
            "CREATE TABLE default.%s " +
            "STORED BY '%s' " +
            "LOCATION '%s'",
            tableName, HiveIcebergStorageHandler.class.getName(), location));
  }
}
