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

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.Snapshot;

/**
 * An action that expires snapshots in a table.
 * <p>
 * Similar to {@link org.apache.iceberg.ExpireSnapshots} but may use a query engine to distribute
 * parts of the work.
 */
public interface ExpireSnapshots extends Action<ExpireSnapshots, ExpireSnapshots.Result> {
  /**
   * Expires a specific {@link Snapshot} identified by id.
   * <p>
   * Identical to {@link org.apache.iceberg.ExpireSnapshots#expireSnapshotId(long)}
   *
   * @param snapshotId id of the snapshot to expire
   * @return this for method chaining
   */
  ExpireSnapshots expireSnapshotId(long snapshotId);

  /**
   * Expires all snapshots older than the given timestamp.
   * <p>
   * Identical to {@link org.apache.iceberg.ExpireSnapshots#expireOlderThan(long)}
   *
   * @param timestampMillis a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  ExpireSnapshots expireOlderThan(long timestampMillis);

  /**
   * Retains the most recent ancestors of the current snapshot.
   * <p>
   * If a snapshot would be expired because it is older than the expiration timestamp, but is one of
   * the {@code numSnapshots} most recent ancestors of the current state, it will be retained. This
   * will not cause snapshots explicitly identified by id from expiring.
   * <p>
   * Identical to {@link org.apache.iceberg.ExpireSnapshots#retainLast(int)}
   *
   * @param numSnapshots the number of snapshots to retain
   * @return this for method chaining
   */
  ExpireSnapshots retainLast(int numSnapshots);

  /**
   * Passes an alternative delete implementation that will be used for manifests and data files.
   * <p>
   * Manifest files that are no longer used by valid snapshots will be deleted. Data files that were
   * deleted by snapshots that are expired will be deleted.
   * <p>
   * If this method is not called, unnecessary manifests and data files will still be deleted.
   * <p>
   * Identical to {@link org.apache.iceberg.ExpireSnapshots#deleteWith(Consumer)}
   *
   * @param deleteFunc a function that will be called to delete manifests and data files
   * @return this for method chaining
   */
  ExpireSnapshots deleteWith(Consumer<String> deleteFunc);

  /**
   * Passes an alternative executor service that will be used for manifests and data files deletion.
   * <p>
   * If this method is not called, unnecessary manifests and data files will still be deleted in
   * the current thread.
   * <p>
   * Identical to {@link org.apache.iceberg.ExpireSnapshots#executeDeleteWith(ExecutorService)}
   *
   * @param executorService the service to use
   * @return this for method chaining
   */
  ExpireSnapshots executeDeleteWith(ExecutorService executorService);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns the number of deleted data files.
     */
    long deletedDataFilesCount();

    /**
     * Return the number that deleted delete files.
     */
    long deletedDeleteFilesCount();

    /**
     * Returns the number of deleted manifests.
     */
    long deletedManifestsCount();

    /**
     * Returns the number of deleted manifest lists.
     */
    long deletedManifestListsCount();
  }
}
