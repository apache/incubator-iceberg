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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreCatalog implements Catalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  enum TableType {
    ENTRIES,
    FILES,
    HISTORY,
    SNAPSHOTS,
    MANIFESTS;

    static TableType from(String name) {
      try {
        return TableType.valueOf(name.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException ignored) {
        return null;
      }
    }
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    TableOperations ops = newTableOps(identifier);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + identifier);
    }

    String baseLocation;
    if (location != null) {
      baseLocation = location;
    } else {
      baseLocation = defaultWarehouseLocation(identifier);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(
        ops, schema, spec, baseLocation, properties == null ? Maps.newHashMap() : properties);

    ops.commit(null, metadata);

    try {
      return new BaseTable(ops, identifier.toString());
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException("Table was created concurrently: " + identifier);
    }
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    TableOperations ops = newTableOps(identifier);
    if (ops.current() == null) {
      String name = identifier.name();
      TableType type = TableType.from(name);
      if (type != null) {
        return loadMetadataTable(TableIdentifier.of(identifier.namespace().levels()), type);
      } else {
        throw new NoSuchTableException("Table does not exist: " + identifier);
      }
    }

    return new BaseTable(ops, identifier.toString());
  }

  @Override
  public boolean dropTableAndData(TableIdentifier identifier) {
    // load the table state to get the data files to delete
    TableOperations ops = newTableOps(identifier);
    if (ops.current() == null) {
      return false;
    }

    // drop the table
    if (!dropTable(identifier)) {
      // the table was dropped by another process after it was loaded, do nothing
      return false;
    }

    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToDelete = Sets.newHashSet();
    for (Snapshot snapshot : ops.current().snapshots()) {
      manifestsToDelete.addAll(snapshot.manifests());
      // add the manifest list to the delete set, if present
      if (snapshot.manifestListLocation() != null) {
        manifestListsToDelete.add(snapshot.manifestListLocation());
      }
    }

    LOG.info("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));

    // run all of the deletes

    FileIO io = ops.io();
    deleteFiles(io, manifestsToDelete);

    Tasks.foreach(Iterables.transform(manifestsToDelete, ManifestFile::path))
        .noRetry().suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(io::deleteFile);

    Tasks.foreach(manifestListsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(io::deleteFile);

    Tasks.foreach(ops.current().file().location())
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for metadata file: {}", list, exc))
        .run(io::deleteFile);

    return true;
  }

  private void deleteFiles(FileIO io, Set<ManifestFile> allManifests) {
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
          try (ManifestReader reader = ManifestReader.read(io.newInputFile(manifest.path()))) {
            for (ManifestEntry entry : reader.entries()) {
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
            throw new RuntimeIOException(e, "Failed to read manifest file: " + manifest.path());
          }
        });
  }

  private Table loadMetadataTable(TableIdentifier identifier, TableType type) {
    TableOperations ops = newTableOps(identifier);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: " + identifier);
    }

    Table baseTable = new BaseTable(ops, identifier.toString());

    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(ops, baseTable);
      case FILES:
        return new DataFilesTable(ops, baseTable);
      case HISTORY:
        return new HistoryTable(ops, baseTable);
      case SNAPSHOTS:
        return new SnapshotsTable(ops, baseTable);
      case MANIFESTS:
        return new ManifestsTable(ops, baseTable);
      default:
        throw new NoSuchTableException(String.format("Unknown metadata table type: %s for %s", type, identifier));
    }
  }

  protected abstract TableOperations newTableOps(TableIdentifier tableIdentifier);

  protected abstract String defaultWarehouseLocation(TableIdentifier tableIdentifier);
}
