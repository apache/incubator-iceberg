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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * A {@link Table} implementation that exposes a table's history as rows.
 * <p>
 * History is based on the table's snapshot log, which logs each update to the table's current snapshot.
 */
public class HistoryTable extends BaseMetadataTable {
  private static final Schema HISTORY_SCHEMA = new Schema(
      Types.NestedField.required(1, "made_current_at", Types.TimestampType.withZone()),
      Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
      Types.NestedField.required(4, "is_current_ancestor", Types.BooleanType.get())
  );

  private final TableOperations ops;
  private final Table table;
  private final String name;

  HistoryTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".history");
  }

  HistoryTable(TableOperations ops, Table table, String name) {
    this.ops = ops;
    this.table = table;
    this.name = name;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public TableScan newScan() {
    return new HistoryScan();
  }

  @Override
  public Schema schema() {
    return HISTORY_SCHEMA;
  }

  private DataTask task(TableScan scan) {
    return StaticDataTask.of(
        ops.io().newInputFile(ops.current().metadataFileLocation()),
        ops.current().snapshotLog(),
        convertHistoryEntryFunc(table));
  }

  private class HistoryScan extends StaticTableScan {
    HistoryScan() {
      super(ops, table, HISTORY_SCHEMA, HistoryTable.this::task);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      // override planFiles to avoid the check for a current snapshot because this metadata table is for all snapshots
      return CloseableIterable.withNoopClose(HistoryTable.this.task(this));
    }
  }

  private static Function<HistoryEntry, StaticDataTask.Row> convertHistoryEntryFunc(Table table) {
    Map<Long, Snapshot> snapshots = Maps.newHashMap();
    for (Snapshot snap : table.snapshots()) {
      snapshots.put(snap.snapshotId(), snap);
    }

    Set<Long> ancestorIds = Sets.newHashSet(SnapshotUtil.currentAncestors(table));

    return historyEntry -> {
      long snapshotId = historyEntry.snapshotId();
      Snapshot snap = snapshots.get(snapshotId);
      return StaticDataTask.Row.of(
          historyEntry.timestampMillis() * 1000,
          historyEntry.snapshotId(),
          snap != null ? snap.parentId() : null,
          ancestorIds.contains(snapshotId)
      );
    };
  }
}
