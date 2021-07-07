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

import org.apache.iceberg.Table;

/**
 * An API that should be implemented by query engine integrations for providing actions.
 */
public interface ActionsProvider {

  /**
   * Instantiates an action to snapshot an existing table as a new Iceberg table.
   */
  default SnapshotTable snapshotTable(String sourceTableIdent) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement snapshotTable");
  }

  /**
   * Instantiates an action to migrate an existing table to Iceberg.
   */
  default MigrateTable migrateTable(String tableIdent) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement migrateTable");
  }

  /**
   * Instantiates an action to remove orphan files.
   */
  default RemoveOrphanFiles removeOrphanFiles(Table table) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement removeOrphanFiles");
  }

  /**
   * Instantiates an action to rewrite manifests.
   */
  default RewriteManifests rewriteManifests(Table table) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement rewriteManifests");
  }

  /**
   * Instantiates an action to rewrite data files.
   */
  default RewriteDataFiles rewriteDataFiles(Table table) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement rewriteDataFiles");
  }

  /**
   * Instantiates an action to expire snapshots.
   */
  default ExpireSnapshots expireSnapshots(Table table) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement expireSnapshots");
  }

  /**
   * Instantiates an action to remove all the files reachable from given metadata location.
   */
  default RemoveReachableFiles removeReachableFiles(String metadataLocation) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement removeReachableFiles");
  }

  /*
   * Instantiates an action to repair manfests.
   */
  default RepairManifests repairManifests(Table table) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement repairManifests");
  }
}
