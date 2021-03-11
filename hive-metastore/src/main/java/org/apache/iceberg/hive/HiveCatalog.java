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


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Pair;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  public static final String CACHE_CLEANER_INTERVAL = "iceberg.hive.client-pool-cache-cleaner-interval";
  private static final long CACHE_CLEANER_INTERVAL_DEFAULT = TimeUnit.SECONDS.toMillis(30);
  public static final String CACHE_EVICTION_INTERVAL = "iceberg.hive.client-pool-cache-eviction-interval";
  private static final long CACHE_EVICTION_INTERVAL_DEFAULT = TimeUnit.MINUTES.toMillis(5);

  @VisibleForTesting
  static final Cache<String, Pair<HiveClientPool, Long>> CLIENT_POOL_CACHE = Caffeine.newBuilder()
          .softValues().build();

  private static volatile ScheduledExecutorService cleaner;

  private String name;
  private Configuration conf;
  private FileIO fileIO;
  private long cleanerInterval;
  private long evictionInterval;

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
    this.cleanerInterval = conf.getLong(CACHE_CLEANER_INTERVAL, CACHE_CLEANER_INTERVAL_DEFAULT);
    this.evictionInterval = conf.getLong(CACHE_EVICTION_INTERVAL, CACHE_EVICTION_INTERVAL_DEFAULT);
    scheduleCacheCleaner();
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
    scheduleCacheCleaner();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(isValidateNamespace(namespace),
        "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    try {
      List<String> tableNames = clientPool().run(client -> client.getAllTables(database));
      List<Table> tableObjects = clientPool()
              .run(client -> client.getTableObjectsByName(database, tableNames));
      List<TableIdentifier> tableIdentifiers = tableObjects.stream()
          .filter(table -> table.getParameters() == null ? false : BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
              .equalsIgnoreCase(table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)))
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
      clientPool().run(client -> {
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
      Table table = clientPool().run(client -> client.getTable(fromDatabase, fromName));
      HiveTableOperations.validateTableIsIceberg(table, fullTableName(name, from));

      table.setDbName(toDatabase);
      table.setTableName(to.name());

      clientPool().run(client -> {
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
      clientPool().run(client -> {
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
      List<Namespace> namespaces = clientPool().run(HiveMetaStoreClient::getAllDatabases)
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
      clientPool().run(client -> {
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
      clientPool().run(client -> {
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
      Database database = clientPool().run(client -> client.getDatabase(namespace.level(0)));
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
    return new HiveTableOperations(conf, fileIO, this, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // This is a little edgy since we basically duplicate the HMS location generation logic.
    // Sadly I do not see a good way around this if we want to keep the order of events, like:
    // - Create meta files
    // - Create the metadata in HMS, and this way committing the changes

    // Create a new location based on the namespace / database if it is set on database level
    try {
      Database databaseData = clientPool()
              .run(client -> client.getDatabase(tableIdentifier.namespace().levels()[0]));
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

  @VisibleForTesting
  HiveClientPool clientPool() {
    synchronized (CLIENT_POOL_CACHE) {
      String metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
      int clientPoolSize = conf.getInt(CatalogProperties.CLIENT_POOL_SIZE,
              CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
      Pair<HiveClientPool, Long> cacheEntry = CLIENT_POOL_CACHE.getIfPresent(metastoreUri);
      HiveClientPool clientPool = cacheEntry == null ? new HiveClientPool(clientPoolSize, conf) : cacheEntry.first();
      CLIENT_POOL_CACHE.put(metastoreUri, Pair.of(clientPool, System.currentTimeMillis() + evictionInterval));
      return clientPool;
    }
  }

  private void scheduleCacheCleaner() {
    if (cleaner == null) {
      synchronized (HiveCatalog.class) {
        if (cleaner == null) {
          cleaner = Executors.newSingleThreadScheduledExecutor(
                  new ThreadFactoryBuilder()
                          .setDaemon(true)
                          .setNameFormat("iceberg-client-pool-cache-cleaner-%d")
                          .build());
        }
        ScheduledFuture<?> futures = cleaner.scheduleWithFixedDelay(() -> {
          synchronized (CLIENT_POOL_CACHE) {
            long currentTime = System.currentTimeMillis();
            CLIENT_POOL_CACHE.asMap().entrySet().stream()
                    .filter(e -> e.getValue().second() <= currentTime)
                    .forEach(e -> {
                      HiveClientPool pool = e.getValue().first();
                      CLIENT_POOL_CACHE.invalidate(e.getKey());
                      pool.close();
                    });
          }
        }, 0, cleanerInterval, TimeUnit.MILLISECONDS);
      }
    }
  }
}
