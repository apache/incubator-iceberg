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

package org.apache.iceberg;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreCatalog implements Catalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {

    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .create();
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {

    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .createTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties,
      boolean orCreate) {

    TableBuilder tableBuilder = buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties);

    if (orCreate) {
      return tableBuilder.createOrReplaceTransaction();
    } else {
      return tableBuilder.replaceTransaction();
    }
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table result;
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        // the identifier may be valid for both tables and metadata tables
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);

        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }

      } else {
        result = new BaseTable(ops, fullTableName(name(), identifier));
      }

    } else if (isValidMetadataIdentifier(identifier)) {
      result = loadMetadataTable(identifier);

    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    LOG.info("Table loaded by catalog: {}", result);
    return result;
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new BaseMetastoreCatalogTableBuilder(identifier, schema);
  }

  private Table loadMetadataTable(TableIdentifier identifier) {
    String tableName = identifier.name();
    MetadataTableType type = MetadataTableType.from(tableName);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      TableOperations ops = newTableOps(baseTableIdentifier);
      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", baseTableIdentifier);
      }

      return MetadataTableUtils.createMetadataTableInstance(ops, name(), baseTableIdentifier, identifier, type);
    } else {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null &&
        isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    // by default allow all identifiers
    return true;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + name() + ")";
  }

  protected abstract TableOperations newTableOps(TableIdentifier tableIdentifier);

  protected abstract String defaultWarehouseLocation(TableIdentifier tableIdentifier);

  protected class BaseMetastoreCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public BaseMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        propertiesBuilder.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new BaseTable(ops, fullTableName(name(), identifier));
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
    }

    @Override
    public Transaction replaceTransaction() {
      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(identifier);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("No such table: %s", identifier);
      }

      TableMetadata metadata;
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata = ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }
  }

  /**
   * Drops all data and metadata files referenced by TableMetadata.
   * <p>
   * This should be called by dropTable implementations to clean up table files once the table has been dropped in the
   * metastore.
   *
   * @param io a FileIO to use for deletes
   * @param metadata the last valid TableMetadata instance for a dropped table.
   * @deprecated will be removed in 0.11.0; use CatalogUtil.dropTableData instead.
   */
  @Deprecated
  protected static void dropTableData(FileIO io, TableMetadata metadata) {
    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToDelete = Sets.newHashSet();
    for (Snapshot snapshot : metadata.snapshots()) {
      // add all manifests to the delete set because both data and delete files should be removed
      Iterables.addAll(manifestsToDelete, snapshot.allManifests());
      // add the manifest list to the delete set, if present
      if (snapshot.manifestListLocation() != null) {
        manifestListsToDelete.add(snapshot.manifestListLocation());
      }
    }

    LOG.info("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));

    // run all of the deletes

    deleteFiles(io, manifestsToDelete);

    Tasks.foreach(Iterables.transform(manifestsToDelete, ManifestFile::path))
        .noRetry().suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(io::deleteFile);

    Tasks.foreach(manifestListsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(io::deleteFile);

    Tasks.foreach(metadata.metadataFileLocation())
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for metadata file: {}", list, exc))
        .run(io::deleteFile);
  }

  @SuppressWarnings("DangerousStringInternUsage")
  private static void deleteFiles(FileIO io, Set<ManifestFile> allManifests) {
    // keep track of deleted files in a map that can be cleaned up when memory runs low
    Map<String, Boolean> deletedFiles = new MapMaker()
        .concurrencyLevel(ThreadPools.WORKER_THREAD_POOL_SIZE)
        .weakKeys()
        .makeMap();

    Tasks.foreach(allManifests)
        .noRetry().suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(manifest -> {
          try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
            for (ManifestEntry<?> entry : reader.entries()) {
              // intern the file path because the weak key map uses identity (==) instead of equals
              String path = entry.file().path().toString().intern();
              Boolean alreadyDeleted = deletedFiles.putIfAbsent(path, true);
              if (alreadyDeleted == null || !alreadyDeleted) {
                try {
                  io.deleteFile(path);
                } catch (RuntimeException e) {
                  // this may happen if the map of deleted files gets cleaned up by gc
                  LOG.warn("Delete failed for data file: {}", path, e);
                }
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest.path());
          }
        });
  }

  protected static String fullTableName(String catalogName, TableIdentifier identifier) {
    StringBuilder sb = new StringBuilder();

    if (catalogName.contains("/") || catalogName.contains(":")) {
      // use / for URI-like names: thrift://host:port/db.table
      sb.append(catalogName);
      if (!catalogName.endsWith("/")) {
        sb.append("/");
      }
    } else {
      // use . for non-URI named catalogs: prod.db.table
      sb.append(catalogName).append(".");
    }

    for (String level : identifier.namespace().levels()) {
      sb.append(level).append(".");
    }

    sb.append(identifier.name());

    return sb.toString();
  }
}
