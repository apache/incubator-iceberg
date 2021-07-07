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

package org.apache.iceberg.spark.actions;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseManifestSparkAction<ThisT, R>
    extends BaseSnapshotUpdateSparkAction<ThisT, R> {
  private final Table table;
  private final FileIO fileIO;
  private static final Logger LOG = LoggerFactory.getLogger(BaseManifestSparkAction.class);

  protected BaseManifestSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.fileIO = SparkUtil.serializableFileIO(table);
  }

  protected Table getTable() {
    return table;
  }

  protected FileIO getFileIO() {
    return fileIO;
  }

  /**
   * Replaces manifests.
   * @param deletedManifests manifests to delete
   * @param addedManifests manifests to add
   * @return added manifests (will be rewritten manifests if snapshotIdInheritanceEnabled is disabled)
   */
  protected Iterable<ManifestFile> replaceManifests(Iterable<ManifestFile> deletedManifests,
                                                    Iterable<ManifestFile> addedManifests) {
    try {
      boolean snapshotIdInheritanceEnabled = PropertyUtil.propertyAsBoolean(
          getTable().properties(),
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);

      org.apache.iceberg.BaseRewriteManifests rewriteManifests =
          (org.apache.iceberg.BaseRewriteManifests) getTable().rewriteManifests();
      deletedManifests.forEach(rewriteManifests::deleteManifest);
      addedManifests.forEach(rewriteManifests::addManifest);
      commit(rewriteManifests);

      if (!snapshotIdInheritanceEnabled) {
        // delete new manifests as they were rewritten before the commit
        deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
        return rewriteManifests.getRewrittenAddedManifests();
      }
      return addedManifests;

    } catch (Exception e) {
      // delete all new manifests because the rewrite failed
      deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      throw e;
    }
  }

  private void deleteFiles(Iterable<String> locations) {
    Tasks.foreach(locations)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
        .run(fileIO::deleteFile);
  }
}
