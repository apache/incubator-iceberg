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

package org.apache.iceberg.hive;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveTableTest extends HiveTableBaseTest {
  static final String NON_DEFAULT_DATABASE =  "nondefault";

  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    // Table should be renamed in hive metastore
    String tableName = TABLE_IDENTIFIER.name();
    org.apache.hadoop.hive.metastore.api.Table table =
        metastoreClient.getTable(TABLE_IDENTIFIER.namespace().level(0), tableName);

    // check parameters are in expected state
    Map<String, String> parameters = table.getParameters();
    Assert.assertNotNull(parameters);
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP)));
    Assert.assertTrue("EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType()));

    // Ensure the table is pointing to empty location
    Assert.assertEquals(getTableLocation(tableName), table.getSd().getLocation());

    // Ensure it is stored as unpartitioned table in hive.
    Assert.assertEquals(0, table.getPartitionKeysSize());

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(1, metadataVersionFiles(tableName).size());
    Assert.assertEquals(0, manifestFiles(tableName).size());

    final Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // Iceberg schema should match the loaded table
    Assert.assertEquals(schema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);
    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue(catalog.tableExists(renameTableIdentifier));

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assert.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
    Assert.assertEquals(original.spec(), renamed.spec());
    Assert.assertEquals(original.location(), renamed.location());
    Assert.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());

    Assert.assertTrue(catalog.dropTable(renameTableIdentifier));
  }

  @Test
  public void testDrop() {
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue("Drop should return true and drop the table", catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
        recordBuilder.set("id", 1L).build(),
        recordBuilder.set("id", 2L).build(),
        recordBuilder.set("id", 3L).build()
    );

    String fileLocation = table.location().replace("file:", "") + "/data/file.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();

    table.newAppend().appendFile(file).commit();

    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assert.assertTrue("Drop should return true and drop the table",
        catalog.dropTable(TABLE_IDENTIFIER, false /* do not delete underlying files */));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertTrue("Table data files should exist",
        new File(fileLocation).exists());
    Assert.assertTrue("Table metadata files should exist",
        new File(manifestListLocation).exists());
  }

  @Test
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
        recordBuilder.set("id", 1L).build(),
        recordBuilder.set("id", 2L).build(),
        recordBuilder.set("id", 3L).build()
    );

    String location1 = table.location().replace("file:", "") + "/data/file1.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location1))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    String location2 = table.location().replace("file:", "") + "/data/file2.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location2))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file1 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location1)
        .withFileSizeInBytes(Files.localInput(location2).getLength())
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location2)
        .withFileSizeInBytes(Files.localInput(location1).getLength())
        .build();

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();

    Assert.assertTrue("Drop (table and data) should return true and drop the table",
        catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertFalse("Table data files should not exist",
        new File(location1).exists());
    Assert.assertFalse("Table data files should not exist",
        new File(location2).exists());
    Assert.assertFalse("Table manifest list files should not exist",
        new File(manifestListLocation).exists());
    for (ManifestFile manifest : manifests) {
      Assert.assertFalse("Table manifest files should not exist",
          new File(manifest.path().replace("file:", "")).exists());
    }
    Assert.assertFalse("Table metadata file should not exist",
        new File(((HasTableOperations) table).operations().current()
            .metadataFileLocation().replace("file:", "")).exists());
  }

  @Test
  public void testExistingTableUpdate() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

    final org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final List<String> hiveColumns = table.getSd().getCols().stream()
        .map(FieldSchema::getName)
        .collect(Collectors.toList());
    final List<String> icebergColumns = altered.columns().stream()
        .map(Types.NestedField::name)
        .collect(Collectors.toList());
    Assert.assertEquals(icebergColumns, hiveColumns);
  }

  @Test
  public void testColumnTypeChangeInMetastore() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    Schema expectedSchema = new Schema(Types.StructType.of(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.LongType.get()),
            optional(3, "string", Types.StringType.get()),
            optional(4, "int", Types.IntegerType.get())).fields());
    // Add columns with different types, then verify we could delete one column in hive metastore
    // as hive conf METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES was set to false. If this was set to true,
    // an InvalidOperationException would thrown in method MetaStoreUtils#throwExceptionIfIncompatibleColTypeChange()
    icebergTable.updateSchema()
            .addColumn("data", Types.LongType.get())
            .addColumn("string", Types.StringType.get())
            .addColumn("int", Types.IntegerType.get())
            .commit();

    Assert.assertEquals("Schema should match expected", expectedSchema.asStruct(), icebergTable.schema().asStruct());

    expectedSchema = new Schema(Types.StructType.of(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.LongType.get()),
            optional(4, "int", Types.IntegerType.get())).fields());
    icebergTable.updateSchema().deleteColumn("string").commit();

    Assert.assertEquals("Schema should match expected", expectedSchema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test(expected = CommitFailedException.class)
  public void testFailure() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    String dummyLocation = "dummylocation";
    table.getParameters().put(METADATA_LOCATION_PROP, dummyLocation);
    metastoreClient.alter_table(DB_NAME, TABLE_NAME, table);
    icebergTable.updateSchema()
        .addColumn("data", Types.LongType.get())
        .commit();
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
        .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
        .collect(Collectors.toList());

    Assert.assertEquals(1, expectedIdents.size());
    Assert.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  public void testNonDefaultDatabaseLocation() throws IOException, TException {
    Namespace namespace = Namespace.of(NON_DEFAULT_DATABASE);
    // Create a new location and a non-default database / namespace for it
    File nonDefaultLocation = createTempDirectory(NON_DEFAULT_DATABASE,
        asFileAttribute(fromString("rwxrwxrwx"))).toFile();
    catalog.createNamespace(namespace, Collections.singletonMap("location", nonDefaultLocation.getPath()));
    Map<String, String> namespaceMeta = catalog.loadNamespaceMetadata(namespace);
    // Make sure that we are testing a namespace with a non default location :)
    Assert.assertEquals(namespaceMeta.get("location"), "file:" + nonDefaultLocation.getPath());

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
    catalog.createTable(tableIdentifier, schema);

    // Let's check the location loaded through the catalog
    Table table = catalog.loadTable(tableIdentifier);
    Assert.assertEquals(namespaceMeta.get("location") + "/" + TABLE_NAME, table.location());

    // Drop the database and purge the files
    metastoreClient.dropDatabase(NON_DEFAULT_DATABASE, true, true, true);
  }

  @Test
  public void testEngineHiveEnabledDefault() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Unset in hive-conf
    hiveConf.unset(ConfigProperties.ENGINE_HIVE_ENABLED);

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  @Test
  public void testEngineHiveEnabledConfig() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Enable by hive-conf
    hiveConf.set(ConfigProperties.ENGINE_HIVE_ENABLED, "true");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, true);

    catalog.dropTable(TABLE_IDENTIFIER);

    // Disable by hive-conf
    hiveConf.set(ConfigProperties.ENGINE_HIVE_ENABLED, "false");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  @Test
  public void testEngineHiveEnabledTableProperty() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Enabled by table property - also check that the hive-conf is ignored
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
    hiveConf.set(ConfigProperties.ENGINE_HIVE_ENABLED, "false");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned(), tableProperties);
    org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, true);

    catalog.dropTable(TABLE_IDENTIFIER);

    // Disabled by table property - also check that the hive-conf is ignored
    tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "false");
    hiveConf.set(ConfigProperties.ENGINE_HIVE_ENABLED, "true");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned(), tableProperties);
    hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  private void assertHiveEnabled(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean expected) {
    if (expected) {
      Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergStorageHandler",
          hmsTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE));
      Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergSerDe",
          hmsTable.getSd().getSerdeInfo().getSerializationLib());
      Assert.assertNull(hmsTable.getSd().getInputFormat());
      Assert.assertNull(hmsTable.getSd().getOutputFormat());
    } else {
      Assert.assertNull(hmsTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE));
      Assert.assertEquals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
          hmsTable.getSd().getSerdeInfo().getSerializationLib());
      Assert.assertEquals("org.apache.hadoop.mapred.FileInputFormat", hmsTable.getSd().getInputFormat());
      Assert.assertEquals("org.apache.hadoop.mapred.FileOutputFormat", hmsTable.getSd().getOutputFormat());
    }
  }
}
