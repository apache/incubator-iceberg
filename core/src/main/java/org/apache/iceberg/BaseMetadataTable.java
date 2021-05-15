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
import org.apache.iceberg.encryption.TableMetadataEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Base class for metadata tables.
 * <p>
 * Serializing and deserializing a metadata table object returns a read only implementation of the metadata table
 * using a {@link StaticTableOperations}. This way no Catalog related calls are needed when reading the table data after
 * deserialization.
 */
abstract class BaseMetadataTable implements Table, HasTableOperations, Serializable {
  private final PartitionSpec spec = PartitionSpec.unpartitioned();
  private final SortOrder sortOrder = SortOrder.unsorted();
  private final TableOperations ops;
  private final Table table;
  private final String name;

  protected BaseMetadataTable(TableOperations ops, Table table, String name) {
    this.ops = ops;
    this.table = table;
    this.name = name;
  }

  abstract MetadataTableType metadataTableType();

  protected Table table() {
    return table;
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
  public FileIO io() {
    return table().io();
  }

  @Override
  public String location() {
    return table().location();
  }

  @Override
  public EncryptionManager encryption() {
    return table().encryption();
  }

  @Override
  public TableMetadataEncryptionManager metadataEncryption() {
    return table().metadataEncryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return table().locationProvider();
  }

  @Override
  public void refresh() {
    table().refresh();
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return ImmutableMap.of(spec.specId(), spec);
  }

  @Override
  public SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return ImmutableMap.of(sortOrder.orderId(), sortOrder);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public Snapshot currentSnapshot() {
    return table().currentSnapshot();
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return table().snapshots();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return table().snapshot(snapshotId);
  }

  @Override
  public List<HistoryEntry> history() {
    return table().history();
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException("Cannot update the schema of a metadata table");
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    throw new UnsupportedOperationException("Cannot update the partition spec of a metadata table");
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException("Cannot update the properties of a metadata table");
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    throw new UnsupportedOperationException("Cannot update the sort order of a metadata table");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Cannot update the location of a metadata table");
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException("Cannot append to a metadata table");
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Cannot rewrite in a metadata table");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException("Cannot rewrite manifests in a metadata table");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException("Cannot overwrite in a metadata table");
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException("Cannot remove or replace rows in a metadata table");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException("Cannot replace partitions in a metadata table");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException("Cannot delete from a metadata table");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException("Cannot expire snapshots from a metadata table");
  }

  @Override
  public Rollback rollback() {
    throw new UnsupportedOperationException("Cannot roll back a metadata table");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException("Cannot manage snapshots in a metadata table");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException("Cannot create transactions for a metadata table");
  }

  @Override
  public String toString() {
    return name();
  }

  final Object writeReplace() {
    return SerializableTable.copyOf(this);
  }
}
