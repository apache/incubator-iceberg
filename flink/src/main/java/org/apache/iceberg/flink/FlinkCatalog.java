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

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * A Flink Catalog implementation that wraps an Iceberg {@link Catalog}.
 * <p>
 * The mapping between Flink database and Iceberg namespace:
 * Supplying a base namespace for a given catalog, so if you have a catalog that supports a 2-level namespace, you
 * would supply the first level in the catalog configuration and the second level would be exposed as Flink databases.
 * <p>
 * The Iceberg table manages its partitions by itself. The partition of the Iceberg table is independent of the
 * partition of Flink.
 */
public class FlinkCatalog extends AbstractCatalog {

  private final CatalogLoader catalogLoader;
  private final Catalog icebergCatalog;
  private final String[] baseNamespace;
  private final SupportsNamespaces asNamespaceCatalog;
  private final Closeable closeable;
  private final boolean cacheEnabled;

  // TODO - Update baseNamespace to use Namespace class
  // https://github.com/apache/iceberg/issues/1541
  public FlinkCatalog(
      String catalogName,
      String defaultDatabase,
      String[] baseNamespace,
      CatalogLoader catalogLoader,
      boolean cacheEnabled) {
    super(catalogName, defaultDatabase);
    this.catalogLoader = catalogLoader;
    this.baseNamespace = baseNamespace;
    this.cacheEnabled = cacheEnabled;

    Catalog originalCatalog = catalogLoader.loadCatalog();
    icebergCatalog = cacheEnabled ? CachingCatalog.wrap(originalCatalog) : originalCatalog;
    asNamespaceCatalog = originalCatalog instanceof SupportsNamespaces ? (SupportsNamespaces) originalCatalog : null;
    closeable = originalCatalog instanceof Closeable ? (Closeable) originalCatalog : null;
  }

  @Override
  public void open() throws CatalogException {
    // Create the default database if it does not exist.
    try {
      createDatabase(getDefaultDatabase(), ImmutableMap.of(), true);
    } catch (DatabaseAlreadyExistException e) {
      // Ignore the exception if it's already exist.
    }
  }

  @Override
  public void close() throws CatalogException {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new CatalogException(e);
      }
    }
  }

  private Namespace toNamespace(String database) {
    String[] namespace = new String[baseNamespace.length + 1];
    System.arraycopy(baseNamespace, 0, namespace, 0, baseNamespace.length);
    namespace[baseNamespace.length] = database;
    return Namespace.of(namespace);
  }

  TableIdentifier toIdentifier(ObjectPath path) {
    return TableIdentifier.of(toNamespace(path.getDatabaseName()), path.getObjectName());
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    if (asNamespaceCatalog == null) {
      return Collections.singletonList(getDefaultDatabase());
    }

    return asNamespaceCatalog.listNamespaces(Namespace.of(baseNamespace)).stream()
        .map(n -> n.level(n.levels().length - 1))
        .collect(Collectors.toList());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
    if (asNamespaceCatalog == null) {
      if (!getDefaultDatabase().equals(databaseName)) {
        throw new DatabaseNotExistException(getName(), databaseName);
      } else {
        return new CatalogDatabaseImpl(Maps.newHashMap(), "");
      }
    } else {
      try {
        Map<String, String> metadata =
            Maps.newHashMap(asNamespaceCatalog.loadNamespaceMetadata(toNamespace(databaseName)));
        String comment = metadata.remove("comment");
        return new CatalogDatabaseImpl(metadata, comment);
      } catch (NoSuchNamespaceException e) {
        throw new DatabaseNotExistException(getName(), databaseName, e);
      }
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    try {
      getDatabase(databaseName);
      return true;
    } catch (DatabaseNotExistException ignore) {
      return false;
    }
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    createDatabase(name, mergeComment(database.getProperties(), database.getComment()), ignoreIfExists);
  }

  private void createDatabase(String databaseName, Map<String, String> metadata, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (asNamespaceCatalog != null) {
      try {
        asNamespaceCatalog.createNamespace(toNamespace(databaseName), metadata);
      } catch (AlreadyExistsException e) {
        if (!ignoreIfExists) {
          throw new DatabaseAlreadyExistException(getName(), databaseName, e);
        }
      }
    } else {
      throw new UnsupportedOperationException("Namespaces are not supported by catalog: " + getName());
    }
  }

  private Map<String, String> mergeComment(Map<String, String> metadata, String comment) {
    Map<String, String> ret = Maps.newHashMap(metadata);
    if (metadata.containsKey("comment")) {
      throw new CatalogException("Database properties should not contain key: 'comment'.");
    }

    if (!StringUtils.isNullOrWhitespaceOnly(comment)) {
      ret.put("comment", comment);
    }
    return ret;
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (asNamespaceCatalog != null) {
      try {
        boolean success = asNamespaceCatalog.dropNamespace(toNamespace(name));
        if (!success && !ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name);
        }
      } catch (NoSuchNamespaceException e) {
        if (!ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name, e);
        }
      } catch (NamespaceNotEmptyException e) {
        throw new DatabaseNotEmptyException(getName(), name, e);
      }
    } else {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
    }
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    if (asNamespaceCatalog != null) {
      Namespace namespace = toNamespace(name);
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();

      try {
        Map<String, String> oldProperties = asNamespaceCatalog.loadNamespaceMetadata(namespace);
        Map<String, String> newProperties = mergeComment(newDatabase.getProperties(), newDatabase.getComment());

        for (String key : oldProperties.keySet()) {
          if (!newProperties.containsKey(key)) {
            removals.add(key);
          }
        }

        for (Map.Entry<String, String> entry : newProperties.entrySet()) {
          if (!entry.getValue().equals(oldProperties.get(entry.getKey()))) {
            updates.put(entry.getKey(), entry.getValue());
          }
        }

        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(namespace, updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(namespace, removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        if (!ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name, e);
        }
      }
    } else {
      if (getDefaultDatabase().equals(name)) {
        throw new CatalogException(
            "Can not alter the default database when the iceberg catalog doesn't support namespaces.");
      }
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
    }
  }

  @Override
  public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
    try {
      return icebergCatalog.listTables(toNamespace(databaseName)).stream()
          .map(TableIdentifier::name)
          .collect(Collectors.toList());
    } catch (NoSuchNamespaceException e) {
      throw new DatabaseNotExistException(getName(), databaseName, e);
    }
  }

  @Override
  public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    Table table = loadIcebergTable(tablePath);
    return toCatalogTable(table);
  }

  private Table loadIcebergTable(ObjectPath tablePath) throws TableNotExistException {
    try {
      Table table = icebergCatalog.loadTable(toIdentifier(tablePath));
      if (cacheEnabled) {
        table.refresh();
      }

      return table;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new TableNotExistException(getName(), tablePath, e);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return icebergCatalog.tableExists(toIdentifier(tablePath));
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    try {
      icebergCatalog.dropTable(toIdentifier(tablePath));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath, e);
      }
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    try {
      icebergCatalog.renameTable(
          toIdentifier(tablePath),
          toIdentifier(new ObjectPath(tablePath.getDatabaseName(), newTableName)));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath, e);
      }
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistException(getName(), tablePath, e);
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws CatalogException, TableAlreadyExistException {
    validateFlinkTable(table);

    Schema icebergSchema = FlinkSchemaUtil.convert(table.getSchema());
    PartitionSpec spec = toPartitionSpec(((CatalogTable) table).getPartitionKeys(), icebergSchema);

    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    String location = null;
    for (Map.Entry<String, String> entry : table.getOptions().entrySet()) {
      if ("location".equalsIgnoreCase(entry.getKey())) {
        location = entry.getValue();
      } else {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    try {
      icebergCatalog.createTable(
          toIdentifier(tablePath),
          icebergSchema,
          spec,
          location,
          properties.build());
    } catch (AlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(getName(), tablePath, e);
      }
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws CatalogException, TableNotExistException {
    validateFlinkTable(newTable);

    Table icebergTable;
    try {
      icebergTable = loadIcebergTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      } else {
        return;
      }
    }

    CatalogTable table = toCatalogTable(icebergTable);

    // Currently, Flink SQL only support altering table properties.

    // For current Flink Catalog API, support for adding/removing/renaming columns cannot be done by comparing
    // CatalogTable instances, unless the Flink schema contains Iceberg column IDs.
    if (!table.getSchema().equals(newTable.getSchema())) {
      throw new UnsupportedOperationException("Altering schema is not supported yet.");
    }

    if (!table.getPartitionKeys().equals(((CatalogTable) newTable).getPartitionKeys())) {
      throw new UnsupportedOperationException("Altering partition keys is not supported yet.");
    }

    Map<String, String> oldProperties = table.getOptions();
    Map<String, String> setProperties = Maps.newHashMap();

    String setLocation = null;
    String setSnapshotId = null;
    String pickSnapshotId = null;

    for (Map.Entry<String, String> entry : newTable.getOptions().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if (Objects.equals(value, oldProperties.get(key))) {
        continue;
      }

      if ("location".equalsIgnoreCase(key)) {
        setLocation = value;
      } else if ("current-snapshot-id".equalsIgnoreCase(key)) {
        setSnapshotId = value;
      } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(key)) {
        pickSnapshotId = value;
      } else {
        setProperties.put(key, value);
      }
    }

    oldProperties.keySet().forEach(k -> {
      if (!newTable.getOptions().containsKey(k)) {
        setProperties.put(k, null);
      }
    });

    commitChanges(icebergTable, setLocation, setSnapshotId, pickSnapshotId, setProperties);
  }

  private static void validateFlinkTable(CatalogBaseTable table) {
    Preconditions.checkArgument(table instanceof CatalogTable, "The Table should be a CatalogTable.");

    TableSchema schema = table.getSchema();
    schema.getTableColumns().forEach(column -> {
      if (column.isGenerated()) {
        throw new UnsupportedOperationException("Creating table with computed columns is not supported yet.");
      }
    });

    if (!schema.getWatermarkSpecs().isEmpty()) {
      throw new UnsupportedOperationException("Creating table with watermark specs is not supported yet.");
    }

    if (schema.getPrimaryKey().isPresent()) {
      throw new UnsupportedOperationException("Creating table with primary key is not supported yet.");
    }
  }

  private static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    partitionKeys.forEach(builder::identity);
    return builder.build();
  }

  private static List<String> toPartitionKeys(PartitionSpec spec, Schema icebergSchema) {
    List<String> partitionKeys = Lists.newArrayList();
    for (PartitionField field : spec.fields()) {
      if (field.transform().isIdentity()) {
        partitionKeys.add(icebergSchema.findColumnName(field.sourceId()));
      } else {
        // Not created by Flink SQL.
        // For compatibility with iceberg tables, return empty.
        // TODO modify this after Flink support partition transform.
        return Collections.emptyList();
      }
    }
    return partitionKeys;
  }

  private static void commitChanges(Table table, String setLocation, String setSnapshotId,
                                    String pickSnapshotId, Map<String, String> setProperties) {
    // don't allow setting the snapshot and picking a commit at the same time because order is ambiguous and choosing
    // one order leads to different results
    Preconditions.checkArgument(setSnapshotId == null || pickSnapshotId == null,
        "Cannot set the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId);
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (pickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(pickSnapshotId);
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation()
          .setLocation(setLocation)
          .commit();
    }

    if (!setProperties.isEmpty()) {
      UpdateProperties updateProperties = transaction.updateProperties();
      setProperties.forEach((k, v) -> {
        if (v == null) {
          updateProperties.remove(k);
        } else {
          updateProperties.set(k, v);
        }
      });
      updateProperties.commit();
    }

    transaction.commitTransaction();
  }

  static CatalogTable toCatalogTable(Table table) {
    TableSchema schema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(table.schema()));
    List<String> partitionKeys = toPartitionKeys(table.spec(), table.schema());

    // NOTE: We can not create a IcebergCatalogTable extends CatalogTable, because Flink optimizer may use
    // CatalogTableImpl to copy a new catalog table.
    // Let's re-loading table from Iceberg catalog when creating source/sink operators.
    // Iceberg does not have Table comment, so pass a null (Default comment value in Flink).
    return new CatalogTableImpl(schema, partitionKeys, table.properties(), null);
  }

  @Override
  public Optional<TableFactory> getTableFactory() {
    return Optional.of(new FlinkTableFactory(this));
  }

  CatalogLoader getCatalogLoader() {
    return catalogLoader;
  }

  // ------------------------------ Unsupported methods ---------------------------------------------

  @Override
  public List<String> listViews(String databaseName) throws CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
      boolean ignoreIfExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String dbName) throws CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return getPartitions(tablePath, Expressions.alwaysTrue());
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    Table table = loadIcebergTable(tablePath);
    Expression filter = Expressions.alwaysTrue();
    if (partitionSpec != null) {
      Schema schema = table.schema();
      filter = getPartitionFilter(schema, partitionSpec);
    }

    return getPartitions(tablePath, filter);
  }

  private List<CatalogPartitionSpec> getPartitions(ObjectPath tablePath, Expression filter)
      throws TableNotPartitionedException, CatalogException, TableNotExistException {
    Table table = loadIcebergTable(tablePath);

    if (table.spec().isUnpartitioned()) {
      throw new TableNotPartitionedException(icebergCatalog.name(), tablePath);
    }

    TableScan scan = table.newScan().filter(filter);
    Set<Map<String, Object>> setMap = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (DataFile dataFile : CloseableIterable.transform(tasks, FileScanTask::file)) {
        Map<String, Object> map = Maps.newHashMap();
        StructLike structLike = dataFile.partition();
        PartitionSpec spec = table.specs().get(dataFile.specId());
        for (int i = 0; i < structLike.size(); i++) {
          String name = spec.fields().get(i).name();
          map.put(name, getValue(table.schema(), name, i, structLike));
        }

        setMap.add(map);
      }
    } catch (IOException e) {
      throw new CatalogException(String.format("Failed to list partitions of table %s", tablePath), e);
    }

    List<Map<String, Object>> list = Lists.newArrayList(setMap);
    sortPartitions(list);
    List<CatalogPartitionSpec> partitionSpecs = Lists.newArrayListWithCapacity(list.size());
    for (Map<String, Object> partitionMap : list) {
      Map<String, String> partitions = partitionMap.entrySet().stream().collect(Collectors.toMap(
          Map.Entry::getKey,
          e -> String.valueOf(e.getValue())
      ));
      partitionSpecs.add(new CatalogPartitionSpec(partitions));
    }

    return partitionSpecs;
  }

  /**
   * Sort the partition map by the value
   *
   * @param list the partition list
   */
  @SuppressWarnings("unchecked")
  private void sortPartitions(List<Map<String, Object>> list) {
    list.sort((map1, map2) -> {
      for (Map.Entry<String, Object> entry : map1.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        Object value1 = map2.get(key);
        if (value instanceof Comparable && value1 instanceof Comparable && !value.equals(value1)) {
          return ((Comparable<Object>) value).compareTo(value1);
        }
      }

      return 0;
    });
  }

  private Object getValue(Schema schema, String name, int index, StructLike structLike) {
    Type type = schema.findType(name);
    if (type instanceof Types.DateType) {
      return DateTimeUtil.dateFromDays(structLike.get(index, Integer.class));
    } else if (type instanceof Types.TimeType) {
      return DateTimeUtil.timeFromMicros(structLike.get(index, Long.class));
    } else if (type instanceof Types.TimestampType) {
      return DateTimeUtil.timestampFromMicros(structLike.get(index, Long.class));
    } else if (type instanceof Types.BinaryType) {
      return new String(structLike.get(index, ByteBuffer.class).array());
    } else {
      return structLike.get(index, Object.class);
    }
  }

  private Expression getPartitionFilter(Schema schema, CatalogPartitionSpec partitionSpec) {
    Map<String, String> partitions = partitionSpec.getPartitionSpec();

    Expression filter = Expressions.alwaysTrue();
    for (Map.Entry<String, String> entry : partitions.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      Type type = schema.findType(name);
      Expression equalExpression = Expressions.alwaysTrue();
      // Long, MAP, ROW, ARRAY type are not supported by flink,so we do not add them  here.
      if (type instanceof Types.IntegerType) {
        equalExpression = Expressions.equal(name, Integer.valueOf(value));
      } else if (type instanceof Types.StringType) {
        equalExpression = Expressions.equal(name, value);
      } else if (type instanceof Types.DoubleType) {
        equalExpression = Expressions.equal(name, Double.valueOf(value));
      } else if (type instanceof Types.FloatType) {
        equalExpression = Expressions.equal(name, Float.valueOf(value));
      } else if (type instanceof Types.DateType) {
        equalExpression = Expressions.equal(name, DateTimeUtil.daysFromDate(LocalDate.parse(value)));
      } else if (type instanceof Types.TimeType) {
        equalExpression = Expressions.equal(name, DateTimeUtil.microsFromTime(LocalTime.parse(value)));
      } else if (type instanceof Types.TimestampType) {
        equalExpression = Expressions.equal(name, DateTimeUtil.microsFromTimestamp(LocalDateTime.parse(value)));
      } else if (type instanceof Types.BooleanType) {
        equalExpression = Expressions.equal(name, Boolean.valueOf(value));
      } else if (type instanceof Types.DecimalType) {
        equalExpression = Expressions.equal(name, new BigDecimal(value));
      } else if (type instanceof Types.BinaryType) {
        equalExpression = Expressions.equal(name, value.getBytes());
      }

      filter = Expressions.and(filter, equalExpression);
    }

    return filter;
  }


  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
      List<org.apache.flink.table.expressions.Expression> filters) throws CatalogException {
    // TODO: call getPartitions method to get the partitions
    throw new UnsupportedOperationException();
  }

  // After partition pruning and filter push down, the statistics have become very inaccurate, so the statistics from
  // here are of little significance.
  // Flink will support something like SupportsReportStatistics in future.

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }
}
