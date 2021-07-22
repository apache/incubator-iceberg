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
import java.net.URL;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A test class to migrate hive table to iceberg table.
 * <p>
 * Iceberg orc use orc-core-nohive, flink use orc-core, they will have some conflicts, so I can not use sql to write
 * data into hive tables.
 * <p>
 * We closed the org.apache.flink.table.catalog.hive.HiveCatalog in the MigrateAction, so we use metastoreClient to drop
 * the hive table and database when job finished in the method {@link TestMigrateAction#clean} and {@link
 * TestMigrateAction#dropHiveDB}, if we use sql to do the drop, it will throw a NullPointerException.
 */
@RunWith(Parameterized.class)
public class TestMigrateAction extends FlinkCatalogTestBase {

  private static final String TARGET_ICEBERG_TABLE_NAME = "test_iceberg_table";
  private static final String SOURCE_HIVE_CATALOG_NAME = "myhive";
  private static final String SOURCE_HIVE_DATABASE_NAME = "test_hive_db";
  private static final String SOURCE_HIVE_TABLE_NAME = "test_hive_table";

  private final FileFormat format;
  private static HiveMetaStoreClient metastoreClient;
  private static HiveCatalog flinkHiveCatalog;
  private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private StreamTableEnvironment tEnv;

  @BeforeClass
  public static void createHiveDB() {
    try {
      metastoreClient = new HiveMetaStoreClient(hiveConf);
      String dbPath = metastore.getDatabasePath(SOURCE_HIVE_DATABASE_NAME);
      Database db = new Database(SOURCE_HIVE_DATABASE_NAME, "description", dbPath, Maps.newHashMap());
      metastoreClient.createDatabase(db);

      flinkHiveCatalog = new HiveCatalog(SOURCE_HIVE_CATALOG_NAME, SOURCE_HIVE_DATABASE_NAME, hiveConf, "2.3.6");
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void dropHiveDB() {
    try {
      metastoreClient.dropDatabase(SOURCE_HIVE_DATABASE_NAME);
      metastoreClient.close();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void before() {
    super.before();
    exec("USE CATALOG %s", catalogName);
    exec("CREATE DATABASE IF NOT EXISTS %s", DATABASE);

    getTableEnv().registerCatalog(SOURCE_HIVE_CATALOG_NAME, flinkHiveCatalog);

    exec("USE CATALOG %s", SOURCE_HIVE_CATALOG_NAME);
    exec("USE %s", SOURCE_HIVE_DATABASE_NAME);
  }

  @After
  public void clean() {
    // drop iceberg db and table
    exec("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TARGET_ICEBERG_TABLE_NAME);
    exec("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();

    // drop hive table
    try {
      metastoreClient.dropTable(SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME);
    } catch (TException e) {
      throw new RuntimeException(e);
    }

    exec("DROP CATALOG IF EXISTS %s", SOURCE_HIVE_CATALOG_NAME);
  }

  @Override
  protected StreamTableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv = StreamTableEnvironment.create(env);
        }
      }
    }

    return tEnv;
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.PARQUET}) {
      for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        Namespace baseNamespace = (Namespace) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }

    return parameters;
  }

  public TestMigrateAction(String catalogName, Namespace baseNamespace, FileFormat format) {
    super(catalogName, baseNamespace);
    this.format = format;
  }

  @Test
  public void testMigrateUnpartitionWithoutFile() {
    getTableEnv().getConfig().setSqlDialect(SqlDialect.HIVE);
    sql("CREATE TABLE %s (id INT, data STRING) stored as %s", SOURCE_HIVE_TABLE_NAME, format.name());
    getTableEnv().getConfig().setSqlDialect(SqlDialect.DEFAULT);

    List<ManifestFile> manifestFiles =
        Actions.migrateHive2Iceberg(env, flinkHiveCatalog, SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME,
            validationCatalog, baseNamespace, DATABASE, TARGET_ICEBERG_TABLE_NAME).execute();
    Assert.assertEquals("Should produce the expected manifestFiles count.", 0, manifestFiles.size());

    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    List<Row> list = sql("SELECT * FROM %s", TARGET_ICEBERG_TABLE_NAME);
    Assert.assertEquals("Should produce the expected records count.", 0, list.size());
  }

  @Test
  public void testMigrateUnpartition() throws IOException, TableNotExistException {
    getTableEnv().getConfig().setSqlDialect(SqlDialect.HIVE);
    sql("CREATE TABLE %s (id INT, data STRING) stored as %s", SOURCE_HIVE_TABLE_NAME, format.name());
    getTableEnv().getConfig().setSqlDialect(SqlDialect.DEFAULT);

    String location =
        flinkHiveCatalog.getHiveTable(new ObjectPath(SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME)).getSd()
            .getLocation();

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema);

    URL url = new URL(location + File.separator + "test." + format.name());
    File dataFile = new File(url.getPath());

    List<Row> expected = Lists.newArrayList();
    try (FileAppender<Record> fileAppender = genericAppenderFactory.newAppender(Files.localOutput(dataFile), format)) {
      for (int i = 0; i < 10; i++) {
        Record record = SimpleDataUtil.createRecord(i, "iceberg");
        fileAppender.add(record);
        expected.add(Row.of(i, "iceberg"));
      }
    }

    List<ManifestFile> manifestFiles =
        Actions.migrateHive2Iceberg(env, flinkHiveCatalog, SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME,
            validationCatalog, baseNamespace, DATABASE, TARGET_ICEBERG_TABLE_NAME).execute();
    Assert.assertEquals("Should produce the expected manifestFiles count.", 1, manifestFiles.size());

    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    List<Row> list = sql("SELECT * FROM %s", TARGET_ICEBERG_TABLE_NAME);
    Assert.assertEquals("Should produce the expected records count.", 10, list.size());
    TestHelpers.assertRows(list, expected);
  }

  @Test
  public void testMigratePartitionWithoutFile() {
    getTableEnv().getConfig().setSqlDialect(SqlDialect.HIVE);
    sql("CREATE TABLE %s (id INT, data STRING) PARTITIONED BY (p STRING) STORED AS %s", SOURCE_HIVE_TABLE_NAME,
        format.name());
    sql("ALTER TABLE %s ADD PARTITION (p='flink')", SOURCE_HIVE_TABLE_NAME);
    sql("ALTER TABLE %s ADD PARTITION (p='iceberg')", SOURCE_HIVE_TABLE_NAME);
    getTableEnv().getConfig().setSqlDialect(SqlDialect.DEFAULT);

    List<ManifestFile> manifestFiles =
        Actions.migrateHive2Iceberg(env, flinkHiveCatalog, SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME,
            validationCatalog, baseNamespace, DATABASE, TARGET_ICEBERG_TABLE_NAME).execute();
    Assert.assertEquals("Should produce the expected manifestFiles count.", 0, manifestFiles.size());

    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    List<Row> results = executeSql("SELECT * FROM %s", TARGET_ICEBERG_TABLE_NAME);
    Assert.assertEquals("Should produce the expected records count.", 0, results.size());
  }

  @Test
  public void testMigratePartition() throws IOException, TException, TableNotExistException {
    getTableEnv().getConfig().setSqlDialect(SqlDialect.HIVE);
    sql("CREATE TABLE %s (id INT, data STRING) PARTITIONED BY (p STRING) STORED AS %s", SOURCE_HIVE_TABLE_NAME,
        format.name());
    sql("ALTER TABLE %s ADD PARTITION (p='hello')", SOURCE_HIVE_TABLE_NAME);
    getTableEnv().getConfig().setSqlDialect(SqlDialect.DEFAULT);
    String hiveLocation =
        flinkHiveCatalog.getHiveTable(new ObjectPath(SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME))
            .getSd().getLocation();

    List<Record> expected = Lists.newArrayList();
    String[] partitions = new String[] {"iceberg", "flink"};

    Schema icebergSchema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "p", Types.StringType.get()));

    for (String partitionValue : partitions) {
      String partitionPath = hiveLocation + "/p=" + partitionValue;

      Partition hivePartition = createHivePartition(format, partitionPath, partitionValue);
      metastoreClient.add_partition(hivePartition);

      Partition partition =
          metastoreClient.getPartition(SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME, "p=" + partitionValue);
      String location = partition.getSd().getLocation();

      Schema schema = new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
      GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema);

      URL url = new URL(location + File.separator + "test." + format.name());
      File dataFile = new File(url.getPath());
      try (
          FileAppender<Record> fileAppender = genericAppenderFactory.newAppender(Files.localOutput(dataFile), format)) {
        for (int i = 0; i < 10; i++) {
          Record record = SimpleDataUtil.createRecord(i, "iceberg" + i);
          fileAppender.add(record);

          GenericRecord genericRecord = GenericRecord.create(icebergSchema);
          genericRecord.set(0, i);
          genericRecord.set(1, "iceberg" + i);
          genericRecord.set(2, partitionValue);
          expected.add(genericRecord);
        }
      }
    }

    List<ManifestFile> manifestFiles =
        Actions.migrateHive2Iceberg(env, flinkHiveCatalog, SOURCE_HIVE_DATABASE_NAME, SOURCE_HIVE_TABLE_NAME,
            validationCatalog, baseNamespace, DATABASE, TARGET_ICEBERG_TABLE_NAME).execute();
    Assert.assertEquals("Should produce the expected manifestFiles count.", 2, manifestFiles.size());

    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    List<Row> results = executeSql("SELECT * FROM %s", TARGET_ICEBERG_TABLE_NAME);
    Assert.assertEquals("Should produce the expected records count.", 20, results.size());
    TestHelpers.assertRecords(results, expected, icebergSchema);
  }

  private Partition createHivePartition(FileFormat fileFormat, String hivePartitionPath, String partitionValue) {
    SerDeInfo serDeInfo =
        new SerDeInfo(null, "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", Maps.newHashMap());

    String inputFormat;
    String outputFormat;
    switch (fileFormat) {
      case ORC:
        inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
        outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
        break;

      case PARQUET:
        inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
        break;

      default:
        throw new UnsupportedOperationException("Unsupported file format :" + fileFormat);
    }

    StorageDescriptor sd = new StorageDescriptor(Lists.newArrayList(), hivePartitionPath,
        inputFormat, outputFormat,
        false, -1, serDeInfo, Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap());

    Partition hivePartition = new Partition(
        Lists.newArrayList(partitionValue),
        SOURCE_HIVE_DATABASE_NAME,
        SOURCE_HIVE_TABLE_NAME,
        0,
        0,
        sd,
        Maps.newHashMap());

    return hivePartition;
  }
}
