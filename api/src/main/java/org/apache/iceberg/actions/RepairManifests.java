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

package org.apache.iceberg.actions;

import java.util.List;
import java.util.function.Predicate;
import org.apache.iceberg.ManifestFile;

/**
 * An action that repairs manifests by reading data files.
 */
public interface RepairManifests extends SnapshotUpdate<RepairManifests, RepairManifests.Result> {

  /**
   * Repairs only manifests that match the given predicate.
   * <p>
   * If not set, all manifests will be repaired.
   *
   * @param predicate a predicate
   * @return this for method chaining
   */
  RepairManifests repairIf(Predicate<ManifestFile> predicate);

  /**
   * Passes a location where the staged manifests should be written.
   * <p>
   * If not set, defaults to the table's metadata location.
   *
   * @param stagingLocation a staging location
   * @return this for method chaining
   */
  RepairManifests stagingLocation(String stagingLocation);


  /**
   * Enable or disable repairing metrics in manifest entries by re-reading the file.
   * @return this for method chaining.
   */
  RepairManifests repairMetrics(boolean repair);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Manifests with oudated entries, deleted as part of repair.
     */
    List<ManifestFile> deletedManifests();

    /**
     * Manifests added as part of the repair.
     */
    List<ManifestFile> addedManifests();
  }
}
