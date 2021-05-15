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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.ThreadPools;

public class DataTableScan extends BaseTableScan {
  static final ImmutableList<String> SCAN_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition", "key_metadata"
  );
  static final ImmutableList<String> SCAN_WITH_STATS_COLUMNS = ImmutableList.<String>builder()
      .addAll(SCAN_COLUMNS)
      .add("value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds", "column_sizes")
      .build();
  static final boolean PLAN_SCANS_WITH_WORKER_POOL =
      SystemProperties.getBoolean(SystemProperties.SCAN_THREAD_POOL_ENABLED, true);

  public DataTableScan(TableOperations ops, Table table) {
    super(ops, table, table.schema());
  }

  protected DataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    Long scanSnapshotId = snapshotId();
    Preconditions.checkState(scanSnapshotId == null,
        "Cannot enable incremental scan, scan-snapshot set to id=%s", scanSnapshotId);
    return new IncrementalDataTableScan(tableOps(), table(), schema(),
        context().fromSnapshotId(fromSnapshotId).toSnapshotId(toSnapshotId));
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    Snapshot currentSnapshot = table().currentSnapshot();
    Preconditions.checkState(currentSnapshot != null, "Cannot scan appends after %s, there is no current snapshot",
        fromSnapshotId);
    return appendsBetween(fromSnapshotId, currentSnapshot.snapshotId());
  }

  @Override
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new DataTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
                                                   Expression rowFilter, boolean ignoreResiduals,
                                                   boolean caseSensitive, boolean colStats) {
    ManifestGroup manifestGroup = new ManifestGroup(ops.io(), ops.encryption(),
        snapshot.dataManifests(), snapshot.deleteManifests())
        .caseSensitive(caseSensitive)
        .select(colStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(rowFilter)
        .specsById(ops.current().specsById())
        .ignoreDeleted();

    if (ignoreResiduals) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && snapshot.dataManifests().size() > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    return manifestGroup.planFiles();
  }

  @Override
  public long targetSplitSize() {
    return tableOps().current().propertyAsLong(
        TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
  }
}
