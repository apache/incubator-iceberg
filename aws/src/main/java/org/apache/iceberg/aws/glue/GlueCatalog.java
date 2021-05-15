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

package org.apache.iceberg.aws.glue;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;

public class GlueCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

  private GlueClient glue;
  private Configuration hadoopConf;
  private String catalogName;
  private String warehousePath;
  private AwsProperties awsProperties;
  private FileIO fileIO;
  private LockManager lockManager;

  /**
   * No-arg constructor to load the catalog dynamically.
   * <p>
   * All fields are initialized by calling {@link GlueCatalog#initialize(String, Map)} later.
   */
  public GlueCatalog() {
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        new AwsProperties(properties),
        AwsClientFactories.from(properties).glue(),
        LockManagers.from(properties),
        initializeFileIO(properties));
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new S3FileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  @VisibleForTesting
  void initialize(String name, String path, AwsProperties properties, GlueClient client, LockManager lock, FileIO io) {
    this.catalogName = name;
    this.awsProperties = properties;
    this.warehousePath = cleanWarehousePath(path);
    this.glue = client;
    this.lockManager = lock;
    this.fileIO = io;
  }

  private String cleanWarehousePath(String path) {
    Preconditions.checkArgument(path != null && path.length() > 0,
        "Cannot initialize GlueCatalog because warehousePath must not be null");
    int len = path.length();
    if (path.charAt(len - 1) == '/') {
      return path.substring(0, len - 1);
    } else {
      return path;
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new GlueTableOperations(glue, lockManager, catalogName, awsProperties, fileIO, tableIdentifier);
  }

  /**
   * This method produces the same result as using a HiveCatalog.
   * If databaseUri exists for the Glue database URI, the default location is databaseUri/tableName.
   * If not, the default location is warehousePath/databaseName.db/tableName
   * @param tableIdentifier table id
   * @return default warehouse path
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // check if value is set in database
    GetDatabaseResponse response = glue.getDatabase(GetDatabaseRequest.builder()
        .name(IcebergToGlueConverter.getDatabaseName(tableIdentifier))
        .build());
    String dbLocationUri = response.database().locationUri();
    if (dbLocationUri != null) {
      return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
    }

    return String.format(
        "%s/%s.db/%s",
        warehousePath,
        IcebergToGlueConverter.getDatabaseName(tableIdentifier),
        tableIdentifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    namespaceExists(namespace);
    // should be safe to list all before returning the list, instead of dynamically load the list.
    String nextToken = null;
    List<TableIdentifier> results = Lists.newArrayList();
    do {
      GetTablesResponse response = glue.getTables(GetTablesRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(IcebergToGlueConverter.toDatabaseName(namespace))
          .nextToken(nextToken)
          .build());
      nextToken = response.nextToken();
      if (response.hasTableList()) {
        results.addAll(response.tableList().stream()
            .filter(this::isGlueIcebergTable)
            .map(GlueToIcebergConverter::toTableId)
            .collect(Collectors.toList()));
      }
    } while (nextToken != null);

    LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, results);
    return results;
  }

  private boolean isGlueIcebergTable(Table table) {
    return table.parameters() != null &&
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
            table.parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = ops.current();
      glue.deleteTable(DeleteTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(IcebergToGlueConverter.getDatabaseName(identifier))
          .name(identifier.name())
          .build());
      LOG.info("Successfully dropped table {} from Glue", identifier);
      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), ops.encryption(), lastMetadata);
        LOG.info("Glue table {} data purged", identifier);
      }
      LOG.info("Dropped table: {}", identifier);
      return true;
    } catch (EntityNotFoundException e) {
      LOG.error("Cannot drop table {} because table not found or not accessible", identifier, e);
      return false;
    } catch (Exception e) {
      LOG.error("Cannot complete drop table operation for {} due to unexpected exception", identifier, e);
      throw e;
    }
  }

  /**
   * Rename table in Glue is a drop table and create table.
   * @param from identifier of the table to rename
   * @param to new table name
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    // check new namespace exists
    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Cannot rename %s to %s because namespace %s does not exist",
          from, to, to.namespace());
    }
    // keep metadata
    Table fromTable = null;
    String fromTableDbName = IcebergToGlueConverter.getDatabaseName(from);
    String fromTableName = IcebergToGlueConverter.getTableName(from);
    String toTableDbName = IcebergToGlueConverter.getDatabaseName(to);
    String toTableName = IcebergToGlueConverter.getTableName(to);
    try {
      GetTableResponse response = glue.getTable(GetTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(fromTableDbName)
          .name(fromTableName)
          .build());
      fromTable = response.table();
    } catch (EntityNotFoundException e) {
      throw new NoSuchTableException(e, "Cannot rename %s because the table does not exist in Glue", from);
    }

    // use the same Glue info to create the new table, pointing to the old metadata
    TableInput.Builder tableInputBuilder = TableInput.builder()
        .owner(fromTable.owner())
        .tableType(fromTable.tableType())
        .parameters(fromTable.parameters());

    glue.createTable(CreateTableRequest.builder()
        .catalogId(awsProperties.glueCatalogId())
        .databaseName(toTableDbName)
        .tableInput(tableInputBuilder.name(toTableName).build())
        .build());
    LOG.info("created rename destination table {}", to);

    try {
      dropTable(from, false);
    } catch (Exception e) {
      // rollback, delete renamed table
      LOG.error("Fail to drop old table {} after renaming to {}, rollback to use the old table", from, to, e);
      glue.deleteTable(DeleteTableRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseName(toTableDbName)
          .name(toTableName)
          .build());
      throw e;
    }

    LOG.info("Successfully renamed table from {} to {}", from, to);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    try {
      glue.createDatabase(CreateDatabaseRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .databaseInput(IcebergToGlueConverter.toDatabaseInput(namespace, metadata))
          .build());
      LOG.info("Created namespace: {}", namespace);
    } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
      throw new AlreadyExistsException("Cannot create namespace %s because it already exists in Glue", namespace);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespace.isEmpty()) {
      // if it is not a list all op, just check if the namespace exists and return empty.
      if (namespaceExists(namespace)) {
        return Lists.newArrayList();
      }
      throw new NoSuchNamespaceException(
          "Glue does not support nested namespace, cannot list namespaces under %s", namespace);
    }

    // should be safe to list all before returning the list, instead of dynamically load the list.
    String nextToken = null;
    List<Namespace> results = Lists.newArrayList();
    do {
      GetDatabasesResponse response = glue.getDatabases(GetDatabasesRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .nextToken(nextToken)
          .build());
      nextToken = response.nextToken();
      if (response.hasDatabaseList()) {
        results.addAll(response.databaseList().stream()
            .map(GlueToIcebergConverter::toNamespace)
            .collect(Collectors.toList()));
      }
    } while (nextToken != null);

    LOG.debug("Listing namespace {} returned namespaces: {}", namespace, results);
    return results;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String databaseName = IcebergToGlueConverter.toDatabaseName(namespace);
    try {
      GetDatabaseResponse response = glue.getDatabase(GetDatabaseRequest.builder()
          .catalogId(awsProperties.glueCatalogId())
          .name(databaseName)
          .build());
      Map<String, String> result = response.database().parameters();
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, result);
      return result;
    } catch (InvalidInputException e) {
      throw new NoSuchNamespaceException("invalid input for namespace %s, error message: %s",
          namespace, e.getMessage());
    } catch (EntityNotFoundException e) {
      throw new NoSuchNamespaceException("fail to find Glue database for namespace %s, error message: %s",
          databaseName, e.getMessage());
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    namespaceExists(namespace);

    GetTablesResponse response = glue.getTables(GetTablesRequest.builder()
        .catalogId(awsProperties.glueCatalogId())
        .databaseName(IcebergToGlueConverter.toDatabaseName(namespace))
        .build());

    if (response.hasTableList() && !response.tableList().isEmpty()) {
      Table table = response.tableList().get(0);
      if (isGlueIcebergTable(table)) {
        throw new NamespaceNotEmptyException(
            "Cannot drop namespace %s because it still contains Iceberg tables", namespace);
      } else {
        throw new NamespaceNotEmptyException(
            "Cannot drop namespace %s because it still contains non-Iceberg tables", namespace);
      }
    }

    glue.deleteDatabase(DeleteDatabaseRequest.builder()
        .catalogId(awsProperties.glueCatalogId())
        .name(IcebergToGlueConverter.toDatabaseName(namespace))
        .build());
    LOG.info("Dropped namespace: {}", namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(loadNamespaceMetadata(namespace));
    newProperties.putAll(properties);
    glue.updateDatabase(UpdateDatabaseRequest.builder()
        .catalogId(awsProperties.glueCatalogId())
        .name(IcebergToGlueConverter.toDatabaseName(namespace))
        .databaseInput(IcebergToGlueConverter.toDatabaseInput(namespace, newProperties))
        .build());
    LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    Map<String, String> metadata = Maps.newHashMap(loadNamespaceMetadata(namespace));
    for (String property : properties) {
      metadata.remove(property);
    }

    glue.updateDatabase(UpdateDatabaseRequest.builder()
        .catalogId(awsProperties.glueCatalogId())
        .name(IcebergToGlueConverter.toDatabaseName(namespace))
        .databaseInput(IcebergToGlueConverter.toDatabaseInput(namespace, metadata))
        .build());
    LOG.debug("Successfully removed properties {} from {}", properties, namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return IcebergToGlueConverter.isValidNamespace(tableIdentifier.namespace()) &&
        IcebergToGlueConverter.isValidTableName(tableIdentifier.name());
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void close() throws IOException {
    glue.close();
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }
}
