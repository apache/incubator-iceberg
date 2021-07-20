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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private String name;
  private Configuration conf;
  private FileIO fileIO;
  private ClientPool<HiveMetaStoreClient, TException> clients;

  public HiveCatalog() {
  }

  /**
   * Hive Catalog constructor.
   *
   * @deprecated please use the no-arg constructor, setConf and initialize to construct the catalog. Will be removed in
   * v0.13.0
   * @param conf Hadoop Configuration
   */
  @Deprecated
  public HiveCatalog(Configuration conf) {
    this.name = "hive";
    this.conf = conf;
    this.fileIO = new HadoopFileIO(conf);
    Map<String, String> properties = ImmutableMap.of(
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            conf.get(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT)),
            CatalogProperties.CLIENT_POOL_SIZE,
            conf.get(CatalogProperties.CLIENT_POOL_SIZE,
                    String.valueOf(CatalogProperties.CLIENT_POOL_SIZE_DEFAULT))
    );
    this.clients = new CachedClientPool(conf, properties);
  }

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    this.name = inputName;
    if (properties.containsKey(CatalogProperties.URI)) {
      this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get(CatalogProperties.URI));
    }

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    this.clients = new CachedClientPool(conf, properties);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(isValidateNamespace(namespace),
        "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    String filter = String.format("%s%s = \"%s\"",
        hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS,
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));

    try {
      List<String> tableNames = clients.run(client -> client.listTableNamesByFilter(database, filter, (short) -1));
      List<Table> tableObjects = clients.run(client -> client.getTableObjectsByName(database, tableNames));
      List<TableIdentifier> tableIdentifiers = tableObjects.stream()
          .map(table -> TableIdentifier.of(namespace, table.getTableName()))
          .collect(Collectors.toList());

      LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, tableIdentifiers);
      return tableIdentifiers;

    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException("Failed to list all tables under namespace " + namespace, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listTables", e);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }

    String database = identifier.namespace().level(0);

    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    try {
      clients.run(client -> {
        client.dropTable(database, identifier.name(),
            false /* do not delete data */,
            false /* throw NoSuchObjectException if the table doesn't exist */);
        return null;
      });

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }

      LOG.info("Dropped table: {}", identifier);
      return true;

    } catch (NoSuchTableException | NoSuchObjectException e) {
      LOG.info("Skipping drop, table does not exist: {}", identifier, e);
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + identifier, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropTable", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Invalid identifier: %s", from);
    }

    TableIdentifier to = removeCatalogName(originalTo);
    Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);

    String toDatabase = to.namespace().level(0);
    String fromDatabase = from.namespace().level(0);
    String fromName = from.name();

    try {
      Table table = clients.run(client -> client.getTable(fromDatabase, fromName));
      HiveTableOperations.validateTableIsIceberg(table, fullTableName(name, from));

      table.setDbName(toDatabase);
      table.setTableName(to.name());

      clients.run(client -> {
        client.alter_table(fromDatabase, fromName, table);
        return null;
      });

      LOG.info("Renamed table from {}, to {}", from, to);

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException("Table does not exist: %s", from);

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);

    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from + " to " + to, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(
        !namespace.isEmpty(),
        "Cannot create namespace with invalid name: %s", namespace);
    Preconditions.checkArgument(isValidateNamespace(namespace),
        "Cannot support multi part namespace in Hive MetaStore: %s", namespace);

    try {
      clients.run(client -> {
        client.createDatabase(convertToDatabase(namespace, meta));
        return null;
      });

      LOG.info("Created namespace: {}", namespace);

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(e, "Namespace '%s' already exists!",
            namespace);

    } catch (TException e) {
      throw new RuntimeException("Failed to create namespace " + namespace + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to createDatabase(name) " + namespace + " in Hive MataStore", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (!isValidateNamespace(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    if (!namespace.isEmpty()) {
      return ImmutableList.of();
    }
    try {
      List<Namespace> namespaces = clients.run(HiveMetaStoreClient::getAllDatabases)
          .stream()
          .map(Namespace::of)
          .collect(Collectors.toList());

      LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
      return namespaces;

    } catch (TException e) {
      throw new RuntimeException("Failed to list all namespace: " + namespace + " in Hive MataStore",  e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to getAllDatabases() " + namespace + " in Hive MataStore", e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      return false;
    }

    try {
      clients.run(client -> {
        client.dropDatabase(namespace.level(0),
            false /* deleteData */,
            false /* ignoreUnknownDb */,
            false /* cascade */);
        return null;
      });

      LOG.info("Dropped namespace: {}", namespace);
      return true;

    } catch (InvalidOperationException e) {
      throw new NamespaceNotEmptyException(e, "Namespace %s is not empty. One or more tables exist.", namespace);

    } catch (NoSuchObjectException e) {
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to drop dropDatabase(name) " + namespace + " in Hive MataStore", e);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace,  Map<String, String> properties) {
    Map<String, String> parameter = Maps.newHashMap();

    parameter.putAll(loadNamespaceMetadata(namespace));
    parameter.putAll(properties);
    Database database = convertToDatabase(namespace, parameter);

    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);

    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace,  Set<String> properties) {
    Map<String, String> parameter = Maps.newHashMap();

    parameter.putAll(loadNamespaceMetadata(namespace));
    properties.forEach(key -> parameter.put(key, null));
    Database database = convertToDatabase(namespace, parameter);

    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully removed properties {} from {}", properties, namespace);

    // Always successful, otherwise exception is thrown
    return true;
  }

  private void alterHiveDataBase(Namespace namespace,  Database database) {
    try {
      clients.run(client -> {
        client.alterDatabase(namespace.level(0), database);
        return null;
      });

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDatabase(name) " + namespace + " in Hive MataStore", e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      Database database = clients.run(client -> client.getDatabase(namespace.level(0)));
      Map<String, String> metadata = convertToMetadata(database);
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
      return metadata;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException("Failed to list namespace under namespace: " + namespace + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to getDatabase(name) " + namespace + " in Hive MataStore", e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  private TableIdentifier removeCatalogName(TableIdentifier to) {
    if (isValidIdentifier(to)) {
      return to;
    }

    // check if the identifier includes the catalog name and remove it
    if (to.namespace().levels().length == 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
      return TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name());
    }

    // return the original unmodified
    return to;
  }

  private boolean isValidateNamespace(Namespace namespace) {
    return namespace.levels().length == 1;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HiveTableOperations(conf, clients, fileIO, name, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // This is a little edgy since we basically duplicate the HMS location generation logic.
    // Sadly I do not see a good way around this if we want to keep the order of events, like:
    // - Create meta files
    // - Create the metadata in HMS, and this way committing the changes

    // Create a new location based on the namespace / database if it is set on database level
    try {
      Database databaseData = clients.run(client -> client.getDatabase(tableIdentifier.namespace().levels()[0]));
      if (databaseData.getLocationUri() != null) {
        // If the database location is set use it as a base.
        return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
      }

    } catch (TException e) {
      throw new RuntimeException(String.format("Metastore operation failed for %s", tableIdentifier), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);
    }

    // Otherwise stick to the {WAREHOUSE_DIR}/{DB_NAME}.db/{TABLE_NAME} path
    String warehouseLocation = getWarehouseLocation();
    return String.format(
        "%s/%s.db/%s",
        warehouseLocation,
        tableIdentifier.namespace().levels()[0],
        tableIdentifier.name());
  }

  private String getWarehouseLocation() {
    String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return warehouseLocation;
  }

  private Map<String, String> convertToMetadata(Database database) {

    Map<String, String> meta = Maps.newHashMap();

    meta.putAll(database.getParameters());
    meta.put("location", database.getLocationUri());
    if (database.getDescription() != null) {
      meta.put("comment", database.getDescription());
    }

    return meta;
  }

  Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Database database = new Database();
    Map<String, String> parameter = Maps.newHashMap();

    database.setName(namespace.level(0));
    database.setLocationUri(new Path(getWarehouseLocation(), namespace.level(0)).toString() + ".db");

    meta.forEach((key, value) -> {
      if (key.equals("comment")) {
        database.setDescription(value);
      } else if (key.equals("location")) {
        database.setLocationUri(value);
      } else {
        if (value != null) {
          parameter.put(key, value);
        }
      }
    });
    database.setParameters(parameter);

    return database;
  }
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("uri", this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname))
        .toString();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
