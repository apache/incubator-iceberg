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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.TableProperties.TABLE_DROP_BASE_PATH_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestHiveCatalog extends HiveMetastoreTest {
  private static final String hiveLocalDir = "file:/tmp/hive/" + UUID.randomUUID().toString();
  private static ImmutableMap meta = ImmutableMap.of(
      "owner", "apache",
      "group", "iceberg",
      "comment", "iceberg  hiveCatalog test");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testCreateTableBuilder() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

    try {
      Table table = catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withLocation(location)
          .withProperty("key1", "value1")
          .withProperty("key2", "value2")
          .create();

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertEquals(1, table.spec().fields().size());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithCaching() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();
    ImmutableMap<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Catalog cachingCatalog = CachingCatalog.wrap(catalog);

    try {
      Table table = cachingCatalog.createTable(tableIdent, schema, spec, location, properties);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertEquals(1, table.spec().fields().size());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      cachingCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableTxnBuilder() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

    try {
      Transaction txn = catalog.buildTable(tableIdent, schema)
          .withLocation(location)
          .createTransaction();
      txn.commitTransaction();
      Table table = catalog.loadTable(tableIdent);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertTrue(table.spec().isUnpartitioned());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testReplaceTxnBuilder() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

    try {
      Transaction createTxn = catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withLocation(location)
          .withProperty("key1", "value1")
          .createOrReplaceTransaction();
      createTxn.commitTransaction();

      Table table = catalog.loadTable(tableIdent);
      Assert.assertEquals(1, table.spec().fields().size());

      String newLocation = temp.newFolder("tbl-2").toString();

      Transaction replaceTxn = catalog.buildTable(tableIdent, schema)
          .withProperty("key2", "value2")
          .withLocation(newLocation)
          .replaceTransaction();
      replaceTxn.commitTransaction();

      table = catalog.loadTable(tableIdent);
      Assert.assertEquals(newLocation, table.location());
      Assert.assertNull(table.currentSnapshot());
      Assert.assertTrue(table.spec().isUnpartitioned());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableDefaultSortOrder() {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.createTable(tableIdent, schema, spec);
      Assert.assertEquals("Order ID must match", 0, table.sortOrder().orderId());
      Assert.assertTrue("Order must unsorted", table.sortOrder().isUnsorted());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableCustomSortOrder() {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    SortOrder order = SortOrder.builderFor(schema)
        .asc("id", NULLS_FIRST)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withSortOrder(order)
          .create();
      SortOrder sortOrder = table.sortOrder();
      Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
      Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
      Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
      Assert.assertEquals("Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
      Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
      Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateNamespace() throws TException {
    Namespace namespace1 = Namespace.of("noLocation");
    catalog.createNamespace(namespace1, meta);
    Database database1 = metastoreClient.getDatabase(namespace1.toString());

    Assert.assertTrue(database1.getParameters().get("owner").equals("apache"));
    Assert.assertTrue(database1.getParameters().get("group").equals("iceberg"));

    Assert.assertEquals("There no same location for db and namespace",
        database1.getLocationUri(), defaultUri(namespace1));

    AssertHelpers.assertThrows("Should fail to create when namespace already exist " + namespace1,
        AlreadyExistsException.class, "Namespace '" + namespace1 + "' already exists!", () -> {
          catalog.createNamespace(namespace1);
        });
    ImmutableMap newMeta = ImmutableMap.<String, String>builder()
        .putAll(meta)
        .put("location", hiveLocalDir)
        .build();
    Namespace namespace2 = Namespace.of("haveLocation");

    catalog.createNamespace(namespace2, newMeta);
    Database database2 = metastoreClient.getDatabase(namespace2.toString());
    Assert.assertEquals("There no same location for db and namespace",
        database2.getLocationUri(), hiveLocalDir);
  }

  @Test
  public void testListNamespace() throws TException {
    List<Namespace> namespaces;
    Namespace namespace1 = Namespace.of("dbname1");
    catalog.createNamespace(namespace1, meta);
    namespaces = catalog.listNamespaces(namespace1);
    Assert.assertTrue("Hive db not hive the namespace 'dbname1'", namespaces.isEmpty());

    Namespace namespace2 = Namespace.of("dbname2");
    catalog.createNamespace(namespace2, meta);
    namespaces = catalog.listNamespaces();

    Assert.assertTrue("Hive db not hive the namespace 'dbname2'", namespaces.contains(namespace2));
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");

    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));
    Assert.assertEquals("There no same location for db and namespace",
        nameMata.get("location"), catalog.convertToDatabase(namespace, meta).getLocationUri());
  }

  @Test
  public void testNamespaceExists() throws TException {
    Namespace namespace = Namespace.of("dbname_exists");

    catalog.createNamespace(namespace, meta);

    Assert.assertTrue("Should true to namespace exist",
        catalog.namespaceExists(namespace));
    Assert.assertTrue("Should false to namespace doesn't exist",
        !catalog.namespaceExists(Namespace.of("db2", "db2", "ns2")));
  }

  @Test
  public void testSetNamespaceProperties() throws TException {
    Namespace namespace = Namespace.of("dbname_set");

    catalog.createNamespace(namespace, meta);
    catalog.setProperties(namespace,
        ImmutableMap.of(
            "owner", "alter_apache",
            "test", "test",
            "location", "file:/data/tmp",
            "comment", "iceberg test")
    );

    Database database = metastoreClient.getDatabase(namespace.level(0));
    Assert.assertEquals(database.getParameters().get("owner"), "alter_apache");
    Assert.assertEquals(database.getParameters().get("test"), "test");
    Assert.assertEquals(database.getParameters().get("group"), "iceberg");
    AssertHelpers.assertThrows("Should fail to namespace not exist" + namespace,
        NoSuchNamespaceException.class, "Namespace does not exist: ", () -> {
          catalog.setProperties(Namespace.of("db2", "db2", "ns2"), meta);
        });
  }

  @Test
  public void testRemoveNamespaceProperties() throws TException {
    Namespace namespace = Namespace.of("dbname_remove");

    catalog.createNamespace(namespace, meta);

    catalog.removeProperties(namespace, ImmutableSet.of("comment", "owner"));

    Database database = metastoreClient.getDatabase(namespace.level(0));

    Assert.assertEquals(database.getParameters().get("owner"), null);
    Assert.assertEquals(database.getParameters().get("group"), "iceberg");
    AssertHelpers.assertThrows("Should fail to namespace not exist" + namespace,
        NoSuchNamespaceException.class, "Namespace does not exist: ", () -> {
          catalog.removeProperties(Namespace.of("db2", "db2", "ns2"), ImmutableSet.of("comment", "owner"));
        });
  }

  @Test
  public void testDropNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = new Schema(Types.StructType.of(
        required(1, "id", Types.LongType.get())).fields());

    catalog.createNamespace(namespace, meta);
    catalog.createTable(identifier, schema);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));

    AssertHelpers.assertThrows("Should fail to drop namespace is not empty" + namespace,
        NamespaceNotEmptyException.class,
        "Namespace dbname_drop is not empty. One or more tables exist.", () -> {
          catalog.dropNamespace(namespace);
        });
    Assert.assertTrue(catalog.dropTable(identifier, true));
    Assert.assertTrue("Should fail to drop namespace if it is not empty",
        catalog.dropNamespace(namespace));
    Assert.assertFalse("Should fail to drop when namespace doesn't exist",
        catalog.dropNamespace(Namespace.of("db.ns1")));
    AssertHelpers.assertThrows("Should fail to drop namespace exist" + namespace,
        NoSuchNamespaceException.class, "Namespace does not exist: ", () -> {
          catalog.loadNamespaceMetadata(namespace);
        });
  }

  @Test
  public void testDropTableAndDirectories() throws TException, IOException {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = new Schema(Types.StructType.of(
        required(1, "id", Types.LongType.get())).fields());

    catalog.createNamespace(namespace, meta);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TABLE_DROP_BASE_PATH_ENABLED, "true");
    Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(), null, properties);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertEquals("apache", nameMata.get("owner"));
    Assert.assertEquals("iceberg", nameMata.get("group"));

    String location = table.location();
    Assert.assertTrue(catalog.dropTable(identifier, true));

    Configuration conf = new Configuration();
    FileSystem fs = Util.getFs(new Path(location), conf);
    Assert.assertFalse(fs.isDirectory(new Path(location)));

    Assert.assertTrue("Should fail to drop namespace if it is not empty",
        catalog.dropNamespace(namespace));
  }

  @Test
  public void testDropTableAndNotDropDirectories() throws TException, IOException {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = new Schema(Types.StructType.of(
        required(1, "id", Types.LongType.get())).fields());

    catalog.createNamespace(namespace, meta);
    Table table = catalog.createTable(identifier, schema);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertEquals("apache", nameMata.get("owner"));
    Assert.assertEquals("iceberg", nameMata.get("group"));

    String location = table.location();
    Assert.assertTrue(catalog.dropTable(identifier, true));

    Configuration conf = new Configuration();
    FileSystem fs = Util.getFs(new Path(location), conf);
    // location not delete.
    Assert.assertTrue(fs.isDirectory(new Path(location)));

    Assert.assertTrue("Should fail to drop namespace if it is not empty",
        catalog.dropNamespace(namespace));
  }

  @Test
  public void testTableName() {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 16)
        .build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .create();

      Table table = catalog.loadTable(tableIdent);
      Assert.assertEquals("Name must match", "hive.hivedb.tbl", table.name());

      TableIdentifier snapshotsTableIdent = TableIdentifier.of(DB_NAME, "tbl", "snapshots");
      Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
      Assert.assertEquals("Name must match", "hive.hivedb.tbl.snapshots", snapshotsTable.name());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private String defaultUri(Namespace namespace) throws TException {
    return metastoreClient.getConfigValue(
        "hive.metastore.warehouse.dir", "") + "/" + namespace.level(0) + ".db";
  }

}
