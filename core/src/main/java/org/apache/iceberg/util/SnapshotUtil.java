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

package org.apache.iceberg.util;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class SnapshotUtil {
  private SnapshotUtil() {
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of snapshotId.
   */
  public static boolean ancestorOf(Table table, long snapshotId, long ancestorSnapshotId) {
    Snapshot current = table.snapshot(snapshotId);
    while (current != null) {
      long id = current.snapshotId();
      if (ancestorSnapshotId == id) {
        return true;
      } else if (current.parentId() != null) {
        current = table.snapshot(current.parentId());
      } else {
        return false;
      }
    }
    return false;
  }

  /**
   * Return the snapshot IDs for the ancestors of the current table state.
   * <p>
   * Ancestor IDs are ordered by commit time, descending. The first ID is the current snapshot, followed by its parent,
   * and so on.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static List<Long> currentAncestors(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Returns list of snapshot ids in the range - (fromSnapshotId, toSnapshotId]
   * <p>
   * This method assumes that fromSnapshotId is an ancestor of toSnapshotId.
   */
  public static List<Long> snapshotIdsBetween(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = Lists.newArrayList(ancestorIds(table.snapshot(toSnapshotId),
        snapshotId -> snapshotId != fromSnapshotId ? table.snapshot(snapshotId) : null));
    return snapshotIds;
  }

  public static List<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    List<Long> ancestorIds = Lists.newArrayList();
    Snapshot current = snapshot;
    while (current != null) {
      ancestorIds.add(current.snapshotId());
      if (current.parentId() != null) {
        current = lookup.apply(current.parentId());
      } else {
        current = null;
      }
    }
    return ancestorIds;
  }

  public static List<DataFile> newFiles(Long baseSnapshotId, long latestSnapshotId, Function<Long, Snapshot> lookup) {
    List<DataFile> newFiles = Lists.newArrayList();

    Long currentSnapshotId = latestSnapshotId;
    while (currentSnapshotId != null && !currentSnapshotId.equals(baseSnapshotId)) {
      Snapshot currentSnapshot = lookup.apply(currentSnapshotId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Cannot determine history between read snapshot %s and current %s",
            baseSnapshotId, currentSnapshotId);
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
      currentSnapshotId = currentSnapshot.parentId();
    }

    return newFiles;
  }

  public static List<Snapshot> snapshotsWithin(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = SnapshotUtil.snapshotIdsBetween(table, fromSnapshotId, toSnapshotId);
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Long snapshotId : snapshotIds) {
      Snapshot snapshot = table.snapshot(snapshotId);
      // for now, incremental scan supports only appends
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        snapshots.add(snapshot);
      } else if (snapshot.operation().equals(DataOperations.OVERWRITE)) {
        throw new UnsupportedOperationException(
            String.format("Found %s operation, cannot support incremental data in snapshots (%s, %s]",
                DataOperations.OVERWRITE, fromSnapshotId, toSnapshotId));
      }
    }
    return snapshots;
  }

  /**
   * Checks whether or not the bounds presented in the form of snapshotIds both reside within the table in question
   * and that there exists a valid set of snapshots between them. Uses the passed in scan context to verify that this
   * range also resides within any previously defined snapshot range in the scan. Throws exceptions if the arguments
   * passed cannot be used to generate a legitimate snapshot range within the previously defined range.
   * ＜p>
   * Used for validating incremental scan parameters
   *
   * @param newFromSnapshotId beginning of snapshot range
   * @param newToSnapshotId   end of snapshot range
   * @param table             containing the snapshots we are building a range for
   * @param context           containing current scan restrictions
   */
  public static void validateSnapshotIdsRefinement(long newFromSnapshotId, long newToSnapshotId, Table table,
                                                   TableScanContext context) {
    Set<Long> snapshotIdsRange = Sets.newHashSet(
        SnapshotUtil.snapshotIdsBetween(table, context.fromSnapshotId(), context.toSnapshotId()));
    // since snapshotIdsBetween return ids in range (fromSnapshotId, toSnapshotId]
    snapshotIdsRange.add(context.fromSnapshotId());
    Preconditions.checkArgument(
        snapshotIdsRange.contains(newFromSnapshotId),
        "from snapshot id %s not in existing snapshot ids range (%s, %s]",
        newFromSnapshotId, context.fromSnapshotId(), newToSnapshotId);
    Preconditions.checkArgument(
        snapshotIdsRange.contains(newToSnapshotId),
        "to snapshot id %s not in existing snapshot ids range (%s, %s]",
        newToSnapshotId, context.fromSnapshotId(), context.toSnapshotId());
  }

  /**
   * Validates whether two snapshots represent the beginning and end of a continuous range of snapshots in a given
   * table. Throws exceptions if this is not the case.
   * ＜p>
   * Used for validating incremental scan parameters
   *
   * @param table          containing snapshots
   * @param fromSnapshotId beginning of snapshot range
   * @param toSnapshotId   end of snapshot range
   */
  public static void validateSnapshotIds(Table table, long fromSnapshotId, long toSnapshotId) {
    Preconditions.checkArgument(fromSnapshotId != toSnapshotId, "from and to snapshot ids cannot be the same");
    Preconditions.checkArgument(
        table.snapshot(fromSnapshotId) != null, "from snapshot %s does not exist", fromSnapshotId);
    Preconditions.checkArgument(
        table.snapshot(toSnapshotId) != null, "to snapshot %s does not exist", toSnapshotId);
    Preconditions.checkArgument(SnapshotUtil.ancestorOf(table, toSnapshotId, fromSnapshotId),
        "from snapshot %s is not an ancestor of to snapshot  %s", fromSnapshotId, toSnapshotId);
  }
}
