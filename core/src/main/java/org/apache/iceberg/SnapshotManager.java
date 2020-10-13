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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.WapUtil;

public class SnapshotManager extends MergingSnapshotProducer<ManageSnapshots> implements ManageSnapshots {

  private enum SnapshotManagerOperation {
    CHERRYPICK,
    ROLLBACK
  }

  private final Map<Integer, PartitionSpec> specsById;
  private SnapshotManagerOperation managerOperation = null;
  private Long targetSnapshotId = null;
  private String snapshotOperation = null;
  private Long requiredCurrentSnapshotId = null;
  private Long overwriteParentId = null;
  private PartitionSet replacedPartitions = null;

  SnapshotManager(String tableName, TableOperations ops) {
    super(tableName, ops);
    this.specsById = ops.current().specsById();
  }

  @Override
  protected ManageSnapshots self() {
    return this;
  }

  @Override
  protected String operation() {
    // snapshotOperation is used by SnapshotProducer when building and writing a new snapshot for cherrypick
    Preconditions.checkNotNull(snapshotOperation, "[BUG] Detected uninitialized operation");
    return snapshotOperation;
  }

  @Override
  public ManageSnapshots cherrypick(long snapshotId) {
    TableMetadata current = current();
    ValidationException.check(current.snapshot(snapshotId) != null,
        "Cannot cherry pick unknown snapshot id: %s", snapshotId);

    Snapshot cherryPickSnapshot = current.snapshot(snapshotId);
    // only append operations are currently supported
    if (cherryPickSnapshot.operation().equals(DataOperations.APPEND)) {
      this.managerOperation = SnapshotManagerOperation.CHERRYPICK;
      this.targetSnapshotId = snapshotId;
      this.snapshotOperation = cherryPickSnapshot.operation();

      // Pick modifications from the snapshot
      for (DataFile addedFile : cherryPickSnapshot.addedFiles()) {
        add(addedFile);
      }

      // this property is set on target snapshot that will get published
      String wapId = WapUtil.validateWapPublish(current, targetSnapshotId);
      if (wapId != null) {
        set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
      }

      // link the snapshot about to be published on commit with the picked snapshot
      set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(targetSnapshotId));

    } else if (cherryPickSnapshot.operation().equals(DataOperations.OVERWRITE) &&
        PropertyUtil.propertyAsBoolean(cherryPickSnapshot.summary(), SnapshotSummary.REPLACE_PARTITIONS_PROP, false)) {
      // the operation was ReplacePartitions. this can be cherry-picked iff the partitions have not been modified.
      // detecting modification requires finding the new files since the parent was committed, so the parent must be an
      // ancestor of the current state, or null if the overwrite was based on an empty table.
      this.overwriteParentId = cherryPickSnapshot.parentId();
      ValidationException.check(overwriteParentId == null || isCurrentAncestor(current, overwriteParentId),
          "Cannot cherry-pick overwrite not based on an ancestor of the current state: %s", snapshotId);

      this.managerOperation = SnapshotManagerOperation.CHERRYPICK;
      this.targetSnapshotId = snapshotId;
      this.snapshotOperation = cherryPickSnapshot.operation();
      this.replacedPartitions = PartitionSet.create(specsById);

      // check that all deleted files are still in the table
      failMissingDeletePaths();

      // copy adds from the picked snapshot
      for (DataFile addedFile : cherryPickSnapshot.addedFiles()) {
        add(addedFile);
        replacedPartitions.add(addedFile.specId(), addedFile.partition());
      }

      // copy deletes from the picked snapshot
      for (DataFile deletedFile : cherryPickSnapshot.deletedFiles()) {
        delete(deletedFile);
      }

      // this property is set on target snapshot that will get published
      String overwriteWapId = WapUtil.validateWapPublish(current, targetSnapshotId);
      if (overwriteWapId != null) {
        set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, overwriteWapId);
      }

      // link the snapshot about to be published on commit with the picked snapshot
      set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(targetSnapshotId));

    } else {
      // cherry-pick should work if the table can be fast-forwarded
      this.managerOperation = SnapshotManagerOperation.ROLLBACK;
      this.requiredCurrentSnapshotId = cherryPickSnapshot.parentId();
      this.targetSnapshotId = snapshotId;
      validateCurrentSnapshot(current, requiredCurrentSnapshotId);
    }

    return this;
  }

  @Override
  public ManageSnapshots setCurrentSnapshot(long snapshotId) {
    ValidationException.check(current().snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);

    this.managerOperation = SnapshotManagerOperation.ROLLBACK;
    this.targetSnapshotId = snapshotId;

    return this;
  }

  @Override
  public ManageSnapshots rollbackToTime(long timestampMillis) {
    // find the latest snapshot by timestamp older than timestampMillis
    Snapshot snapshot = findLatestAncestorOlderThan(current(), timestampMillis);
    Preconditions.checkArgument(snapshot != null,
        "Cannot roll back, no valid snapshot older than: %s", timestampMillis);

    this.managerOperation = SnapshotManagerOperation.ROLLBACK;
    this.targetSnapshotId = snapshot.snapshotId();

    return this;
  }

  @Override
  public ManageSnapshots rollbackTo(long snapshotId) {
    TableMetadata current = current();
    ValidationException.check(current.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);
    ValidationException.check(
        isCurrentAncestor(current, snapshotId),
        "Cannot roll back to snapshot, not an ancestor of the current state: %s", snapshotId);
    return setCurrentSnapshot(snapshotId);
  }

  @Override
  public Object updateEvent() {
    if (targetSnapshotId == null) {
      // NOOP operation, no snapshot created
      return null;
    }

    switch (managerOperation) {
      case ROLLBACK:
        // rollback does not create a new snapshot
        return null;
      case CHERRYPICK:
        TableMetadata tableMetadata = refresh();
        long snapshotId = tableMetadata.currentSnapshot().snapshotId();
        if (targetSnapshotId == snapshotId) {
          // No new snapshot is created for fast-forward
          return null;
        } else {
          // New snapshot created, we rely on super class to fire a CreateSnapshotEvent
          return super.updateEvent();
        }
      default:
        throw new UnsupportedOperationException(managerOperation + " is not supported");
    }
  }

  @Override
  protected void validate(TableMetadata base) {
    validateCurrentSnapshot(base, requiredCurrentSnapshotId);
    validateNonAncestor(base, targetSnapshotId);
    validateReplacedPartitions(base, overwriteParentId, replacedPartitions);
    WapUtil.validateWapPublish(base, targetSnapshotId);
  }

  @Override
  public Snapshot apply() {
    TableMetadata base = refresh();

    if (targetSnapshotId == null) {
      // if no target snapshot was configured then NOOP by returning current state
      return base.currentSnapshot();
    }

    switch (managerOperation) {
      case CHERRYPICK:
        if (base.snapshot(targetSnapshotId).parentId() != null &&
            base.currentSnapshot().snapshotId() == base.snapshot(targetSnapshotId).parentId()) {
          // the snapshot to cherrypick is already based on the current state: fast-forward
          validate(base);
          return base.snapshot(targetSnapshotId);
        } else {
          // validate(TableMetadata) is called in apply(TableMetadata) after this apply refreshes the table state
          return super.apply();
        }

      case ROLLBACK:
        return base.snapshot(targetSnapshotId);

      default:
        throw new ValidationException("Invalid SnapshotManagerOperation: only cherrypick, rollback are supported");
    }
  }

  private static void validateCurrentSnapshot(TableMetadata meta, Long requiredSnapshotId) {
    if (requiredSnapshotId != null) {
      ValidationException.check(meta.currentSnapshot().snapshotId() == requiredSnapshotId,
          "Cannot fast-forward to non-append snapshot; current has changed: current=%s != required=%s",
          meta.currentSnapshot().snapshotId(), requiredSnapshotId);
    }
  }

  private static void validateNonAncestor(TableMetadata meta, long snapshotId) {
    if (isCurrentAncestor(meta, snapshotId)) {
      throw new CherrypickAncestorCommitException(snapshotId);
    }

    Long ancestorId = lookupAncestorBySourceSnapshot(meta, snapshotId);
    if (ancestorId != null) {
      throw new CherrypickAncestorCommitException(snapshotId, ancestorId);
    }
  }

  private static void validateReplacedPartitions(TableMetadata meta, Long parentId,
                                                 PartitionSet replacedPartitions) {
    if (replacedPartitions != null) {
      ValidationException.check(parentId == null || isCurrentAncestor(meta, parentId),
          "Cannot cherry-pick overwrite, based on non-ancestor of the current state: %s", parentId);
      List<DataFile> newFiles = SnapshotUtil.newFiles(parentId, meta.currentSnapshot().snapshotId(), meta::snapshot);
      for (DataFile newFile : newFiles) {
        ValidationException.check(!replacedPartitions.contains(newFile.specId(), newFile.partition()),
            "Cannot cherry-pick replace partitions with changed partition: %s",
            newFile.partition());
      }
    }
  }

  private static Long lookupAncestorBySourceSnapshot(TableMetadata meta, long snapshotId) {
    String snapshotIdStr = String.valueOf(snapshotId);
    for (long ancestorId : currentAncestors(meta)) {
      Map<String, String> summary = meta.snapshot(ancestorId).summary();
      if (summary != null && snapshotIdStr.equals(summary.get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))) {
        return ancestorId;
      }
    }

    return null;
  }

  /**
   * Return the latest snapshot whose timestamp is before the provided timestamp.
   *
   * @param meta {@link TableMetadata} for a table
   * @param timestampMillis lookup snapshots before this timestamp
   * @return the ID of the snapshot that was current at the given timestamp, or null
   */
  private static Snapshot findLatestAncestorOlderThan(TableMetadata meta, long timestampMillis) {
    long snapshotTimestamp = 0;
    Snapshot result = null;
    for (Long snapshotId : currentAncestors(meta)) {
      Snapshot snapshot = meta.snapshot(snapshotId);
      if (snapshot.timestampMillis() < timestampMillis &&
          snapshot.timestampMillis() > snapshotTimestamp) {
        result = snapshot;
        snapshotTimestamp = snapshot.timestampMillis();
      }
    }
    return result;
  }

  private static List<Long> currentAncestors(TableMetadata meta) {
    return SnapshotUtil.ancestorIds(meta.currentSnapshot(), meta::snapshot);
  }

  private static boolean isCurrentAncestor(TableMetadata meta, long snapshotId) {
    return currentAncestors(meta).contains(snapshotId);
  }
}
