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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Base {@link Table} implementation.
 * <p>
 * This can be extended by providing a {@link TableOperations} to the constructor.
 * <p>
 * Serializing and deserializing a BaseTable object returns a read only implementation of the BaseTable using a
 * {@link StaticTableOperations}. This way no Catalog related calls are needed when reading the table data after
 * deserialization.
 */
public class BaseTable implements Table, HasTableOperations, Serializable {
  private final TableOperations ops;
  private final String name;

  public BaseTable(TableOperations ops, String name) {
    this.ops = ops;
    this.name = name;
  }

  @Override
  public TableOperations operations() {
    return ops;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void refresh() {
    ops.refresh();
  }

  @Override
  public TableScan newScan() {
    return new DataTableScan(ops, this);
  }

  @Override
  public Schema schema() {
    return ops.current().schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return ops.current().schemasById();
  }

  /**
   * Return the {@link Schema schema} for this table at the time of the snapshot
   * specified by the snapshotId.
   *
   * @return the schema
   */
  public Schema schemaForSnapshot(long snapshotId) {
    TableMetadata current = ops.current();
    if (current.currentSnapshot() != null &&
        current.currentSnapshot().snapshotId() == snapshotId) {
      return current.schema();
    }
    Long snapshotTs = null;
    for (HistoryEntry logEntry : current.snapshotLog()) {
      if (logEntry.snapshotId() == snapshotId) {
        snapshotTs = logEntry.timestampMillis();
      }
    }
    Preconditions.checkArgument(snapshotTs != null,
        "Cannot find a snapshot with id %s", snapshotId);
    TableMetadata.MetadataLogEntry metadataLogEntry = null;
    for (TableMetadata.MetadataLogEntry logEntry : current.previousFiles()) {
      if (logEntry.timestampMillis() == snapshotTs) {
        metadataLogEntry = logEntry;
        break;
      }
    }
    Preconditions.checkArgument(metadataLogEntry != null,
        "Cannot find a metadata log entry corresponding to the snapshot %s", snapshotId);
    String metadataFile = metadataLogEntry.file();
    TableMetadata metadata = TableMetadataParser.read(io(), metadataFile);
    return metadata.schema();
  }

  /**
   * Return the {@link Schema schema} for this table at the time of the most recent
   * snapshot as of the specified timestampMillis.
   * Note: This is not necessarily the schema at the time of the specified timestampMillis.
   *
   * @return the schema
   */
  public Schema schemaForSnapshotAsOfTime(long timestampMillis) {
    Long snapshotId = null;
    for (HistoryEntry logEntry : ops.current().snapshotLog()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        snapshotId = logEntry.snapshotId();
      }
    }
    Preconditions.checkArgument(snapshotId != null,
        "Cannot find a snapshot older than %s", timestampMillis);
    return schemaForSnapshot(snapshotId);
  }

  @Override
  public PartitionSpec spec() {
    return ops.current().spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return ops.current().specsById();
  }

  @Override
  public SortOrder sortOrder() {
    return ops.current().sortOrder();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return ops.current().sortOrdersById();
  }

  @Override
  public Map<String, String> properties() {
    return ops.current().properties();
  }

  @Override
  public String location() {
    return ops.current().location();
  }

  @Override
  public Snapshot currentSnapshot() {
    return ops.current().currentSnapshot();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return ops.current().snapshot(snapshotId);
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return ops.current().snapshots();
  }

  @Override
  public List<HistoryEntry> history() {
    return ops.current().snapshotLog();
  }

  @Override
  public UpdateSchema updateSchema() {
    return new SchemaUpdate(ops);
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return new BaseUpdatePartitionSpec(ops);
  }

  @Override
  public UpdateProperties updateProperties() {
    return new PropertiesUpdate(ops);
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return new BaseReplaceSortOrder(ops);
  }

  @Override
  public UpdateLocation updateLocation() {
    return new SetLocation(ops);
  }

  @Override
  public AppendFiles newAppend() {
    return new MergeAppend(name, ops);
  }

  @Override
  public AppendFiles newFastAppend() {
    return new FastAppend(name, ops);
  }

  @Override
  public RewriteFiles newRewrite() {
    return new BaseRewriteFiles(name, ops);
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return new BaseRewriteManifests(ops);
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return new BaseOverwriteFiles(name, ops);
  }

  @Override
  public RowDelta newRowDelta() {
    return new BaseRowDelta(name, ops);
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return new BaseReplacePartitions(name, ops);
  }

  @Override
  public DeleteFiles newDelete() {
    return new StreamingDelete(name, ops);
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return new RemoveSnapshots(ops);
  }

  @Override
  public Rollback rollback() {
    return new RollbackToSnapshot(name, ops);
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return new SnapshotManager(name, ops);
  }

  @Override
  public Transaction newTransaction() {
    return Transactions.newTransaction(name, ops);
  }

  @Override
  public FileIO io() {
    return operations().io();
  }

  @Override
  public EncryptionManager encryption() {
    return operations().encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return operations().locationProvider();
  }

  @Override
  public String toString() {
    return name();
  }

  Object writeReplace() {
    return SerializableTable.copyOf(this);
  }
}
