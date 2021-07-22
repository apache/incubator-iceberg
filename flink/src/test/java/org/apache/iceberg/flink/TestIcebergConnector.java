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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergConnector extends FlinkTestBase {

  private static final String TABLE_NAME = "test_table";

  @ClassRule
  public static final TemporaryFolder WAREHOUSE = new TemporaryFolder();

  private final String catalogName;
  private final Map<String, String> properties;
  private final boolean isStreaming;
  private volatile TableEnvironment tEnv;

  @Parameterized.Parameters(name = "catalogName = {0}, properties = {1}, isStreaming={2}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        // Create iceberg table in the hadoop catalog and default database.
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop"
            ),
            true
        },
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop",
                "catalog-table", "not_existing_table"
            ),
            true
        },
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop"
            ),
            false
        },
        // Create iceberg table in the hadoop catalog and not_existing_db.
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop",
                "catalog-database", "not_existing_db"
            ),
            true
        },
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop",
                "catalog-database", "not_existing_db",
                "catalog-table", "not_existing_table"
            ),
            true
        },
        new Object[] {
            "testhadoop",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hadoop",
                "catalog-database", "not_existing_db"
            ),
            false
        },
        // Create iceberg table in the hive catalog and default database.
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive"
            ),
            true
        },
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive",
                "catalog-table", "not_existing_table"
            ),
            true
        },
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive"
            ),
            false
        },
        // Create iceberg table in the hive catalog and not_existing_db.
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive",
                "catalog-database", "not_existing_db"
            ),
            true
        },
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive",
                "catalog-database", "not_existing_db",
                "catalog-table", "not_existing_table"
            ),
            true
        },
        new Object[] {
            "testhive",
            ImmutableMap.of(
                "connector", "iceberg",
                "catalog-type", "hive",
                "catalog-database", "not_existing_db"
            ),
            false
        }
    );
  }

  public TestIcebergConnector(String catalogName, Map<String, String> properties, boolean isStreaming) {
    this.catalogName = catalogName;
    this.properties = properties;
    this.isStreaming = isStreaming;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
              .newInstance()
              .useBlinkPlanner();
          if (isStreaming) {
            settingsBuilder.inStreamingMode();
            StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
            env.enableCheckpointing(400);
            env.setMaxParallelism(2);
            env.setParallelism(2);
            tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
          } else {
            settingsBuilder.inBatchMode();
            tEnv = TableEnvironment.create(settingsBuilder.build());
          }
          // Set only one parallelism.
          tEnv.getConfig().getConfiguration()
              .set(CoreOptions.DEFAULT_PARALLELISM, 1)
              .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
        }
      }
    }
    return tEnv;
  }

  @After
  public void after() throws TException {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);

    // Clean the created orphan databases and tables from hive-metastore.
    if (isHiveCatalog()) {
      HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
      try {
        metaStoreClient.dropTable(databaseName(), tableName());
        if (!isDefaultDatabaseName()) {
          try {
            metaStoreClient.dropDatabase(databaseName());
          } catch (Exception ignored) {
            // Ignore
          }
        }
      } finally {
        metaStoreClient.close();
      }
    }
  }

  private void testCreateConnectorTable() {
    Map<String, String> tableProps = createTableProps();

    // Create table under the flink's current database.
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    sql("INSERT INTO %s VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')", TABLE_NAME);
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
        sql("SELECT * FROM %s", TABLE_NAME));

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    Catalog flinkCatalog = factory.createCatalog(catalogName, tableProps, new Configuration());
    Assert.assertTrue("Should have created the expected database",
        flinkCatalog.databaseExists(databaseName()));
    Assert.assertTrue("Should have created the expected table",
        flinkCatalog.tableExists(new ObjectPath(databaseName(), tableName())));

    // Drop and create it again.
    sql("DROP TABLE %s", TABLE_NAME);
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
        sql("SELECT * FROM %s", TABLE_NAME));
  }

  @Test
  public void testCreateTableUnderDefaultDatabase() {
    testCreateConnectorTable();
  }

  @Test
  public void testCatalogDatabaseConflictWithFlinkDatabase() {
    sql("CREATE DATABASE IF NOT EXISTS `%s`", databaseName());
    sql("USE `%s`", databaseName());

    try {
      testCreateConnectorTable();
      // Ensure that the table was created under the specific database.
      AssertHelpers.assertThrows("Table should already exists",
          ValidationException.class,
          "Could not execute CreateTable in path",
          () -> sql("CREATE TABLE `default_catalog`.`%s`.`%s`", databaseName(), TABLE_NAME));
    } finally {
      sql("DROP TABLE IF EXISTS `%s`.`%s`", databaseName(), TABLE_NAME);
      if (!isDefaultDatabaseName()) {
        sql("DROP DATABASE `%s`", databaseName());
      }
    }
  }

  @Test
  public void testConnectorTableInIcebergCatalog() {
    // Create the catalog properties
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("type", "iceberg");
    if (isHiveCatalog()) {
      catalogProps.put("catalog-type", "hive");
      catalogProps.put(CatalogProperties.URI, FlinkCatalogTestBase.getURI(hiveConf));
    } else {
      catalogProps.put("catalog-type", "hadoop");
    }
    catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, createWarehouse());

    // Create the table properties
    Map<String, String> tableProps = createTableProps();

    // Create a connector table in an iceberg catalog.
    sql("CREATE CATALOG `test_catalog` WITH %s", toWithClause(catalogProps));
    try {
      AssertHelpers.assertThrowsCause("Cannot create the iceberg connector table in iceberg catalog",
          IllegalArgumentException.class,
          "Cannot create the table with 'connector'='iceberg' table property in an iceberg catalog",
          () -> sql("CREATE TABLE `test_catalog`.`%s`.`%s` (id BIGINT, data STRING) WITH %s",
              FlinkCatalogFactory.DEFAULT_DATABASE_NAME,
              TABLE_NAME,
              toWithClause(tableProps)
          )
      );
    } finally {
      sql("DROP CATALOG IF EXISTS `test_catalog`");
    }
  }

  private Map<String, String> createTableProps() {
    Map<String, String> tableProps = Maps.newHashMap(properties);
    tableProps.put("catalog-name", catalogName);
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, createWarehouse());
    if (isHiveCatalog()) {
      tableProps.put(CatalogProperties.URI, FlinkCatalogTestBase.getURI(hiveConf));
    }
    return tableProps;
  }

  private boolean isHiveCatalog() {
    return "testhive".equalsIgnoreCase(catalogName);
  }

  private boolean isDefaultDatabaseName() {
    return FlinkCatalogFactory.DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName());
  }

  private String tableName() {
    return properties.getOrDefault("catalog-table", TABLE_NAME);
  }

  private String databaseName() {
    return properties.getOrDefault("catalog-database", "default_database");
  }

  private String toWithClause(Map<String, String> props) {
    return FlinkCatalogTestBase.toWithClause(props);
  }

  private static String createWarehouse() {
    try {
      return String.format("file://%s", WAREHOUSE.newFolder().getAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
