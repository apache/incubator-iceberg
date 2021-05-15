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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionManagers;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

public class CatalogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogUtil.class);
  public static final String ICEBERG_CATALOG_TYPE = "type";
  public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
  public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";
  public static final String ICEBERG_CATALOG_HIVE = "org.apache.iceberg.hive.HiveCatalog";
  public static final String ICEBERG_CATALOG_HADOOP = "org.apache.iceberg.hadoop.HadoopCatalog";

  private CatalogUtil() {
  }

  /**
   * @deprecated please use {@link #dropTableData(FileIO, EncryptionManager, TableMetadata)}
   */
  @Deprecated
  public static void dropTableData(FileIO io, TableMetadata metadata) {
    dropTableData(io, EncryptionManagers.plainText(), metadata);
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
  public static void dropTableData(FileIO io, EncryptionManager encryption, TableMetadata metadata) {
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

    boolean gcEnabled = PropertyUtil.propertyAsBoolean(metadata.properties(), GC_ENABLED, GC_ENABLED_DEFAULT);

    if (gcEnabled) {
      // delete data files only if we are sure this won't corrupt other tables
      deleteFiles(io, encryption, manifestsToDelete);
    }

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
  private static void deleteFiles(FileIO io, EncryptionManager encryption, Set<ManifestFile> allManifests) {
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
          try (ManifestReader<?> reader = ManifestFiles.open(manifest, io, encryption)) {
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

  /**
   * Load a custom catalog implementation.
   * <p>
   * The catalog must have a no-arg constructor.
   * If the class implements {@link Configurable},
   * a Hadoop config will be passed using {@link Configurable#setConf(Configuration)}.
   * {@link Catalog#initialize(String catalogName, Map options)} is called to complete the initialization.
   *
   * @param impl catalog implementation full class name
   * @param catalogName catalog name
   * @param properties catalog properties
   * @param hadoopConf hadoop configuration if needed
   * @return initialized catalog object
   * @throws IllegalArgumentException if no-arg constructor not found or error during initialization
   */
  public static Catalog loadCatalog(
      String impl,
      String catalogName,
      Map<String, String> properties,
      Configuration hadoopConf) {
    Preconditions.checkNotNull(impl, "Cannot initialize custom Catalog, impl class name is null");
    DynConstructors.Ctor<Catalog> ctor;
    try {
      ctor = DynConstructors.builder(Catalog.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize Catalog implementation %s: %s", impl, e.getMessage()), e);
    }

    Catalog catalog;
    try {
      catalog = ctor.newInstance();

    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Catalog, %s does not implement Catalog.", impl), e);
    }

    if (catalog instanceof Configurable) {
      ((Configurable) catalog).setConf(hadoopConf);
    }

    catalog.initialize(catalogName, properties);
    return catalog;
  }

  public static Catalog buildIcebergCatalog(String name, Map<String, String> options, Configuration conf) {
    String catalogImpl = options.get(CatalogProperties.CATALOG_IMPL);
    if (catalogImpl == null) {
      String catalogType = options.getOrDefault(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
      switch (catalogType.toLowerCase(Locale.ENGLISH)) {
        case ICEBERG_CATALOG_TYPE_HIVE:
          catalogImpl = ICEBERG_CATALOG_HIVE;
          break;
        case ICEBERG_CATALOG_TYPE_HADOOP:
          catalogImpl = ICEBERG_CATALOG_HADOOP;
          break;
        default:
          throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
      }
    }

    return CatalogUtil.loadCatalog(catalogImpl, name, options, conf);
  }

  /**
   * Load a custom {@link FileIO} implementation.
   * <p>
   * The implementation must have a no-arg constructor.
   * If the class implements {@link Configurable},
   * a Hadoop config will be passed using {@link Configurable#setConf(Configuration)}.
   * {@link FileIO#initialize(Map properties)} is called to complete the initialization.
   *
   * @param impl full class name of a custom FileIO implementation
   * @param hadoopConf hadoop configuration
   * @return FileIO class
   * @throws IllegalArgumentException if class path not found or
   *  right constructor not found or
   *  the loaded class cannot be casted to the given interface type
   */
  public static FileIO loadFileIO(
      String impl,
      Map<String, String> properties,
      Configuration hadoopConf) {
    LOG.info("Loading custom FileIO implementation: {}", impl);
    DynConstructors.Ctor<FileIO> ctor;
    try {
      ctor = DynConstructors.builder(FileIO.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize FileIO, missing no-arg constructor: %s", impl), e);
    }

    FileIO fileIO;
    try {
      fileIO = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize FileIO, %s does not implement FileIO.", impl), e);
    }

    if (fileIO instanceof Configurable) {
      ((Configurable) fileIO).setConf(hadoopConf);
    }

    fileIO.initialize(properties);
    return fileIO;
  }
}
