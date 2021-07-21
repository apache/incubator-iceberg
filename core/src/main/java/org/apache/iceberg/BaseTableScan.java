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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link TableScan} implementations.
 */
abstract class BaseTableScan implements TableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTableScan.class);

  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private final TableOperations ops;
  private final Table table;
  private final Schema schema;
  private final TableScanContext context;

  protected BaseTableScan(TableOperations ops, Table table, Schema schema) {
    this(ops, table, schema, new TableScanContext());
  }

  protected BaseTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    this.ops = ops;
    this.table = table;
    this.schema = schema;
    this.context = context;
  }

  protected TableOperations tableOps() {
    return ops;
  }

  protected Long snapshotId() {
    return context.snapshotId();
  }

  protected boolean colStats() {
    return context.returnColumnStats();
  }

  protected boolean shouldIgnoreResiduals() {
    return context.ignoreResiduals();
  }

  protected Collection<String> selectedColumns() {
    return context.selectedColumns();
  }

  protected Map<String, String> options() {
    return context.options();
  }

  protected  TableScanContext context() {
    return context;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract TableScan newRefinedScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context);

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract CloseableIterable<FileScanTask> planFiles(
      TableOperations ops, Snapshot snapshot, Expression rowFilter,
      boolean ignoreResiduals, boolean caseSensitive, boolean colStats);

  @Override
  public Table table() {
    return table;
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(context.snapshotId() == null,
        "Cannot override snapshot, already set to id=%s", context.snapshotId());
    Preconditions.checkArgument(ops.current().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s", scanSnapshotId);
    return newRefinedScan(
        ops, table, schema, context.useSnapshotId(scanSnapshotId));
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    Preconditions.checkArgument(context.snapshotId() == null,
        "Cannot override snapshot, already set to id=%s", context.snapshotId());

    Long lastSnapshotId = null;
    for (HistoryEntry logEntry : ops.current().snapshotLog()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        lastSnapshotId = logEntry.snapshotId();
      }
    }

    // the snapshot ID could be null if no entries were older than the requested time. in that case,
    // there is no valid snapshot to read.
    Preconditions.checkArgument(lastSnapshotId != null,
        "Cannot find a snapshot older than %s", formatTimestampMillis(timestampMillis));

    return useSnapshot(lastSnapshotId);
  }

  @Override
  public TableScan option(String property, String value) {
    return newRefinedScan(
        ops, table, schema, context.withOption(property, value));
  }

  @Override
  public TableScan project(Schema projectedSchema) {
    return newRefinedScan(
        ops, table, schema, context.project(projectedSchema));
  }

  @Override
  public TableScan caseSensitive(boolean scanCaseSensitive) {
    return newRefinedScan(
        ops, table, schema, context.setCaseSensitive(scanCaseSensitive));
  }

  @Override
  public TableScan includeColumnStats() {
    return newRefinedScan(
        ops, table, schema, context.shouldReturnColumnStats(true));
  }

  @Override
  public TableScan select(Collection<String> columns) {
    return newRefinedScan(
        ops, table, schema, context.selectColumns(columns));
  }

  @Override
  public TableScan filter(Expression expr) {
    return newRefinedScan(ops, table, schema,
        context.filterRows(Expressions.and(context.rowFilter(), expr)));
  }

  @Override
  public Expression filter() {
    return context.rowFilter();
  }

  @Override
  public TableScan ignoreResiduals() {
    return newRefinedScan(
        ops, table, schema, context.ignoreResiduals(true));
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot snapshot = snapshot();
    if (snapshot != null) {
      LOG.info("Scanning table {} snapshot {} created at {} with filter {}", table,
          snapshot.snapshotId(), formatTimestampMillis(snapshot.timestampMillis()),
          context.rowFilter());

      Listeners.notifyAll(
          new ScanEvent(table.name(), snapshot.snapshotId(), context.rowFilter(), schema()));

      return planFiles(ops, snapshot,
          context.rowFilter(), context.ignoreResiduals(), context.caseSensitive(), context.returnColumnStats());

    } else {
      LOG.info("Scanning empty table {}", table);
      return CloseableIterable.empty();
    }
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    Map<String, String> options = context.options();
    long splitSize;
    if (options.containsKey(TableProperties.SPLIT_SIZE)) {
      splitSize = Long.parseLong(options.get(TableProperties.SPLIT_SIZE));
    } else {
      splitSize = targetSplitSize();
    }
    int lookback;
    if (options.containsKey(TableProperties.SPLIT_LOOKBACK)) {
      lookback = Integer.parseInt(options.get(TableProperties.SPLIT_LOOKBACK));
    } else {
      lookback = ops.current().propertyAsInt(
          TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
    }
    long openFileCost;
    if (options.containsKey(TableProperties.SPLIT_OPEN_FILE_COST)) {
      openFileCost = Long.parseLong(options.get(TableProperties.SPLIT_OPEN_FILE_COST));
    } else {
      openFileCost = ops.current().propertyAsLong(
          TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    }

    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, splitSize);
    return TableScanUtil.planTasks(splitFiles, splitSize, lookback, openFileCost);
  }

  @Override
  public Schema schema() {
    return lazyColumnProjection();
  }

  @Override
  public Snapshot snapshot() {
    return context.snapshotId() != null ?
        ops.current().snapshot(context.snapshotId()) :
        ops.current().currentSnapshot();
  }

  @Override
  public boolean isCaseSensitive() {
    return context.caseSensitive();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", table)
        .add("projection", schema().asStruct())
        .add("filter", context.rowFilter())
        .add("ignoreResiduals", context.ignoreResiduals())
        .add("caseSensitive", context.caseSensitive())
        .toString();
  }

  /**
   * To be able to make refinements {@link #select(Collection)} and {@link #caseSensitive(boolean)} in any order,
   * we resolve the schema to be projected lazily here.
   *
   * @return the Schema to project
   */
  private Schema lazyColumnProjection() {
    Collection<String> selectedColumns = context.selectedColumns();
    if (selectedColumns != null) {
      Set<Integer> requiredFieldIds = Sets.newHashSet();

      // all of the filter columns are required
      requiredFieldIds.addAll(
          Binder.boundReferences(schema.asStruct(),
              Collections.singletonList(context.rowFilter()), context.caseSensitive()));

      // all of the projection columns are required
      Set<Integer> selectedIds;
      if (context.caseSensitive()) {
        selectedIds = TypeUtil.getProjectedIds(schema.select(selectedColumns));
      } else {
        selectedIds = TypeUtil.getProjectedIds(schema.caseInsensitiveSelect(selectedColumns));
      }
      requiredFieldIds.addAll(selectedIds);

      return TypeUtil.select(schema, requiredFieldIds);

    } else if (context.projectedSchema() != null) {
      return context.projectedSchema();
    }

    return schema;
  }

  private static String formatTimestampMillis(long millis) {
    return DATE_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC));
  }
}
