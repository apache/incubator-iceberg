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
import java.util.Set;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;

class IncrementalDataTableScan extends DataTableScan {

  IncrementalDataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context.useSnapshotId(null));
    SnapshotUtil.validateSnapshotIds(table, context.fromSnapshotId(), context.toSnapshotId());
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table as of time %s: configured for incremental data in snapshots (%s, %s]",
        timestampMillis, context().fromSnapshotId(), context().toSnapshotId()));
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table using scan snapshot id %s: configured for incremental data in snapshots (%s, %s]",
        scanSnapshotId, context().fromSnapshotId(), context().toSnapshotId()));
  }

  @Override
  public TableScan appendsBetween(long newFromSnapshotId, long newToSnapshotId) {
    SnapshotUtil.validateSnapshotIdsRefinement(newFromSnapshotId, newToSnapshotId, table(), context());
    return new IncrementalDataTableScan(tableOps(), table(), schema(),
        context().fromSnapshotId(newFromSnapshotId).toSnapshotId(newToSnapshotId));
  }

  @Override
  public TableScan appendsAfter(long newFromSnapshotId) {
    final Snapshot currentSnapshot = table().currentSnapshot();
    Preconditions.checkState(currentSnapshot != null,
        "Cannot scan appends after %s, there is no current snapshot", newFromSnapshotId);
    return appendsBetween(newFromSnapshotId, currentSnapshot.snapshotId());
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    // TODO publish an incremental appends scan event
    List<Snapshot> snapshots = SnapshotUtil.snapshotsWithin(table(),
        context().fromSnapshotId(), context().toSnapshotId());
    Set<Long> snapshotIds = Sets.newHashSet(Iterables.transform(snapshots, Snapshot::snapshotId));
    Set<ManifestFile> manifests = FluentIterable
        .from(snapshots)
        .transformAndConcat(Snapshot::dataManifests)
        .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
        .toSet();

    ManifestGroup manifestGroup = new ManifestGroup(tableOps().io(), manifests)
        .caseSensitive(isCaseSensitive())
        .select(colStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(filter())
        .filterManifestEntries(
            manifestEntry ->
                snapshotIds.contains(manifestEntry.snapshotId()) &&
                manifestEntry.status() == ManifestEntry.Status.ADDED)
        .specsById(tableOps().current().specsById())
        .ignoreDeleted();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && manifests.size() > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    return manifestGroup.planFiles();
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new IncrementalDataTableScan(ops, table, schema, context);
  }


}
