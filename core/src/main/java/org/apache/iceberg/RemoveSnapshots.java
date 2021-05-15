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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;

@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = MoreExecutors.newDirectExecutorService();

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private boolean cleanExpiredFiles = true;
  private TableMetadata base;
  private long expireOlderThan;
  private int minNumSnapshots;
  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();

    long maxSnapshotAgeMs = PropertyUtil.propertyAsLong(
        base.properties(),
        MAX_SNAPSHOT_AGE_MS,
        MAX_SNAPSHOT_AGE_MS_DEFAULT);
    this.expireOlderThan = System.currentTimeMillis() - maxSnapshotAgeMs;

    this.minNumSnapshots = PropertyUtil.propertyAsInt(
        base.properties(),
        MIN_SNAPSHOTS_TO_KEEP,
        MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  public ExpireSnapshots cleanExpiredFiles(boolean clean) {
    this.cleanExpiredFiles = clean;
    return this;
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long expireSnapshotId) {
    LOG.info("Expiring snapshot with id: {}", expireSnapshotId);
    idsToRemove.add(expireSnapshotId);
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    LOG.info("Expiring snapshots older than: {} ({})", new Date(timestampMillis), timestampMillis);
    this.expireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots retainLast(int numSnapshots) {
    Preconditions.checkArgument(1 <= numSnapshots,
            "Number of snapshots to retain must be at least 1, cannot be: %s", numSnapshots);
    this.minNumSnapshots = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshots executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public List<Snapshot> apply() {
    TableMetadata updated = internalApply();
    List<Snapshot> removed = Lists.newArrayList(base.snapshots());
    removed.removeAll(updated.snapshots());

    return removed;
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();

    Set<Long> idsToRetain = Sets.newHashSet();
    List<Long> ancestorIds = SnapshotUtil.ancestorIds(base.currentSnapshot(), base::snapshot);
    if (minNumSnapshots >= ancestorIds.size()) {
      idsToRetain.addAll(ancestorIds);
    } else {
      idsToRetain.addAll(ancestorIds.subList(0, minNumSnapshots));
    }

    TableMetadata updateMeta = base.removeSnapshotsIf(snapshot ->
        idsToRemove.contains(snapshot.snapshotId()) ||
        (snapshot.timestampMillis() < expireOlderThan && !idsToRetain.contains(snapshot.snapshotId())));
    List<Snapshot> updateSnapshots = updateMeta.snapshots();
    List<Snapshot> baseSnapshots = base.snapshots();
    return updateSnapshots.size() != baseSnapshots.size() ? updateMeta : base;
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(item -> {
          TableMetadata updated = internalApply();
          ops.commit(base, updated);
        });
    LOG.info("Committed snapshot changes");

    if (cleanExpiredFiles) {
      cleanExpiredSnapshots();
    } else {
      LOG.info("Cleaning up manifest and data files disabled, leaving them in place");
    }
  }

  private void cleanExpiredSnapshots() {
    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists

    TableMetadata current = ops.refresh();

    Set<Long> validIds = Sets.newHashSet();
    for (Snapshot snapshot : current.snapshots()) {
      validIds.add(snapshot.snapshotId());
    }

    Set<Long> expiredIds = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      if (!validIds.contains(snapshotId)) {
        // the snapshot was expired
        LOG.info("Expired snapshot: {}", snapshot);
        expiredIds.add(snapshotId);
      }
    }

    if (expiredIds.isEmpty()) {
      // if no snapshots were expired, skip cleanup
      return;
    }

    LOG.info("Committed snapshot changes; cleaning up expired manifests and data files.");

    removeExpiredFiles(current.snapshots(), validIds, expiredIds);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void removeExpiredFiles(List<Snapshot> snapshots, Set<Long> validIds, Set<Long> expiredIds) {
    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds = Sets.newHashSet(SnapshotUtil.ancestorIds(base.currentSnapshot(), base::snapshot));

    Set<Long> pickedAncestorSnapshotIds = Sets.newHashSet();
    for (long snapshotId : ancestorIds) {
      String sourceSnapshotId = base.snapshot(snapshotId).summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
      if (sourceSnapshotId != null) {
        // protect any snapshot that was cherry-picked into the current table state
        pickedAncestorSnapshotIds.add(Long.parseLong(sourceSnapshotId));
      }
    }

    // find manifests to clean up that are still referenced by a valid snapshot, but written by an expired snapshot
    Set<String> validManifests = Sets.newHashSet();
    Set<ManifestFile> manifestsToScan = Sets.newHashSet();
    Tasks.foreach(snapshots).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}", snapshot.snapshotId(),
                snapshot.manifestListLocation(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot)) {
                for (ManifestFile manifest : manifests) {
                  validManifests.add(manifest.path());

                  long snapshotId = manifest.snapshotId();
                  // whether the manifest was created by a valid snapshot (true) or an expired snapshot (false)
                  boolean fromValidSnapshots = validIds.contains(snapshotId);
                  // whether the snapshot that created the manifest was an ancestor of the table state
                  boolean isFromAncestor = ancestorIds.contains(snapshotId);
                  // whether the changes in this snapshot have been picked into the current table state
                  boolean isPicked = pickedAncestorSnapshotIds.contains(snapshotId);
                  // if the snapshot that wrote this manifest is no longer valid (has expired),
                  // then delete its deleted files. note that this is only for expired snapshots that are in the
                  // current table state
                  if (!fromValidSnapshots && (isFromAncestor || isPicked) && manifest.hasDeletedFiles()) {
                    manifestsToScan.add(manifest.copy());
                  }
                }

              } catch (IOException e) {
                throw new RuntimeIOException(e,
                    "Failed to close manifest list: %s", snapshot.manifestListLocation());
              }
            });

    // find manifests to clean up that were only referenced by snapshots that have expired
    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToRevert = Sets.newHashSet();
    Tasks.foreach(base.snapshots()).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}", snapshot.snapshotId(),
                snapshot.manifestListLocation(), exc))
        .run(
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              if (!validIds.contains(snapshotId)) {
                // determine whether the changes in this snapshot are in the current table state
                if (pickedAncestorSnapshotIds.contains(snapshotId)) {
                  // this snapshot was cherry-picked into the current table state, so skip cleaning it up.
                  // its changes will expire when the picked snapshot expires.
                  // A -- C -- D (source=B)
                  //  `- B <-- this commit
                  return;
                }

                long sourceSnapshotId = PropertyUtil.propertyAsLong(
                    snapshot.summary(), SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, -1);
                if (ancestorIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
                  // changes because it would revert data file additions that are in the current table.
                  // A -- B -- C
                  //  `- D (source=B) <-- this commit
                  return;
                }

                if (pickedAncestorSnapshotIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
                  // changes because it would revert data file additions that are in the current table.
                  // A -- C -- E (source=B)
                  //  `- B `- D (source=B) <-- this commit
                  return;
                }

                // find any manifests that are no longer needed
                try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot)) {
                  for (ManifestFile manifest : manifests) {
                    if (!validManifests.contains(manifest.path())) {
                      manifestsToDelete.add(manifest.path());

                      boolean isFromAncestor = ancestorIds.contains(manifest.snapshotId());
                      boolean isFromExpiringSnapshot = expiredIds.contains(manifest.snapshotId());

                      if (isFromAncestor && manifest.hasDeletedFiles()) {
                        // Only delete data files that were deleted in by an expired snapshot if that
                        // snapshot is an ancestor of the current table state. Otherwise, a snapshot that
                        // deleted files and was rolled back will delete files that could be in the current
                        // table state.
                        manifestsToScan.add(manifest.copy());
                      }

                      if (!isFromAncestor && isFromExpiringSnapshot && manifest.hasAddedFiles()) {
                        // Because the manifest was written by a snapshot that is not an ancestor of the
                        // current table state, the files added in this manifest can be removed. The extra
                        // check whether the manifest was written by a known snapshot that was expired in
                        // this commit ensures that the full ancestor list between when the snapshot was
                        // written and this expiration is known and there is no missing history. If history
                        // were missing, then the snapshot could be an ancestor of the table state but the
                        // ancestor ID set would not contain it and this would be unsafe.
                        manifestsToRevert.add(manifest.copy());
                      }
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeIOException(e,
                      "Failed to close manifest list: %s", snapshot.manifestListLocation());
                }

                // add the manifest list to the delete set, if present
                if (snapshot.manifestListLocation() != null) {
                  manifestListsToDelete.add(snapshot.manifestListLocation());
                }
              }
            });
    deleteDataFiles(manifestsToScan, manifestsToRevert, validIds);
    deleteMetadataFiles(manifestsToDelete, manifestListsToDelete);
  }

  private void deleteMetadataFiles(Set<String> manifestsToDelete, Set<String> manifestListsToDelete) {
    LOG.warn("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));
    LOG.warn("Manifests Lists to delete: {}", Joiner.on(", ").join(manifestListsToDelete));

    Tasks.foreach(manifestsToDelete)
        .executeWith(deleteExecutorService)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(deleteFunc::accept);

    Tasks.foreach(manifestListsToDelete)
        .executeWith(deleteExecutorService)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(deleteFunc::accept);
  }

  private void deleteDataFiles(Set<ManifestFile> manifestsToScan, Set<ManifestFile> manifestsToRevert,
                               Set<Long> validIds) {
    Set<String> filesToDelete = findFilesToDelete(manifestsToScan, manifestsToRevert, validIds);
    Tasks.foreach(filesToDelete)
        .executeWith(deleteExecutorService)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Delete failed for data file: {}", file, exc))
        .run(file -> deleteFunc.accept(file));
  }

  private Set<String> findFilesToDelete(Set<ManifestFile> manifestsToScan, Set<ManifestFile> manifestsToRevert,
                                        Set<Long> validIds) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();
    Tasks.foreach(manifestsToScan)
        .retry(3).suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(manifest -> {
          // the manifest has deletes, scan it to find files to delete
          try (ManifestReader<?> reader = ManifestFiles.open(manifest, ops.io(), ops.encryption(),
              ops.current().specsById())) {
            for (ManifestEntry<?> entry : reader.entries()) {
              // if the snapshot ID of the DELETE entry is no longer valid, the data can be deleted
              if (entry.status() == ManifestEntry.Status.DELETED &&
                  !validIds.contains(entry.snapshotId())) {
                // use toString to ensure the path will not change (Utf8 is reused)
                filesToDelete.add(entry.file().path().toString());
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
          }
        });

    Tasks.foreach(manifestsToRevert)
        .retry(3).suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get added files: this may cause orphaned data files", exc))
        .run(manifest -> {
          // the manifest has deletes, scan it to find files to delete
          try (ManifestReader<?> reader = ManifestFiles.open(manifest, ops.io(), ops.encryption(),
              ops.current().specsById())) {
            for (ManifestEntry<?> entry : reader.entries()) {
              // delete any ADDED file from manifests that were reverted
              if (entry.status() == ManifestEntry.Status.ADDED) {
                // use toString to ensure the path will not change (Utf8 is reused)
                filesToDelete.add(entry.file().path().toString());
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
          }
        });

    return filesToDelete;
  }

  private static final Schema MANIFEST_PROJECTION = ManifestFile.schema()
      .select("manifest_path", "added_snapshot_id", "deleted_data_files_count");

  private CloseableIterable<ManifestFile> readManifestFiles(Snapshot snapshot) {
    if (snapshot.manifestListLocation() != null) {
      return Avro.read(ops.io().newInputFile(snapshot.manifestListLocation()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .classLoader(GenericManifestFile.class.getClassLoader())
          .project(MANIFEST_PROJECTION)
          .reuseContainers(true)
          .build();

    } else {
      return CloseableIterable.withNoopClose(snapshot.allManifests());
    }
  }
}
