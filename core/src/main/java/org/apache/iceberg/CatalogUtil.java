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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogUtil.class);

  private CatalogUtil() {
  }

  /**
   * Drops all data and metadata files referenced by TableMetadata.
   * <p>
   * This should be called by dropTable implementations to clean up table files once the table has been dropped in the
   * metastore.
   *
   * @param io a FileIO to use for deletes
   * @param metadata the last valid TableMetadata instance for a dropped table.
   */
  public static void dropTableData(FileIO io, TableMetadata metadata) {
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
}
