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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

/**
 * A {@link Table} implementation that exposes a table's partitions as rows.
 */
public class PartitionsTable extends BaseMetadataTable {

  private final TableOperations ops;
  private final Table table;
  private final Schema schema;
  private final String name;

  PartitionsTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".partitions");
  }

  PartitionsTable(TableOperations ops, Table table, String name) {
    this.ops = ops;
    this.table = table;
    this.schema = new Schema(
        Types.NestedField.required(1, "partition", table.spec().partitionType()),
        Types.NestedField.required(2, "record_count", Types.LongType.get()),
        Types.NestedField.required(3, "file_count", Types.IntegerType.get())
    );
    this.name = name;
  }

  private static StaticDataTask.Row convertPartition(Partition partition) {
    return StaticDataTask.Row.of(partition.key, partition.recordCount, partition.fileCount);
  }

  private static Iterable<Partition> partitions(Table table, Long snapshotId) {
    PartitionMap partitions = new PartitionMap(table.spec().partitionType());
    TableScan scan = table.newScan();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    for (FileScanTask task : scan.planFiles()) {
      partitions.get(task.file().partition()).update(task.file());
    }

    return partitions.all();
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
    return new PartitionsScan();
  }

  @Override
  public Schema schema() {
    if (table.spec().fields().size() < 1) {
      return schema.select("record_count", "file_count");
    }
    return schema;
  }

  @Override
  String metadataLocation() {
    return ops.current().metadataFileLocation();
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.PARTITIONS;
  }

  private DataTask task(TableScan scan) {
    Iterable<Partition> partitions = partitions(table, scan.snapshot().snapshotId());
    if (table.spec().fields().size() < 1) {
      // the table is unpartitioned, partitions contains only the root partition
      return StaticDataTask.of(io().newInputFile(ops.current().metadataFileLocation()), partitions,
          root -> StaticDataTask.Row.of(root.recordCount, root.fileCount));
    } else {
      return StaticDataTask.of(io().newInputFile(ops.current().metadataFileLocation()), partitions,
          PartitionsTable::convertPartition);
    }
  }

  static class PartitionMap {
    private final Map<StructLikeWrapper, Partition> partitions = Maps.newHashMap();
    private final Types.StructType type;
    private final StructLikeWrapper reused;

    PartitionMap(Types.StructType type) {
      this.type = type;
      this.reused = StructLikeWrapper.forType(type);
    }

    Partition get(StructLike key) {
      Partition partition = partitions.get(reused.set(key));
      if (partition == null) {
        partition = new Partition(key);
        partitions.put(StructLikeWrapper.forType(type).set(key), partition);
      }
      return partition;
    }

    Iterable<Partition> all() {
      return partitions.values();
    }

    List<PartitionStatsEntry> getAllPartitionStatsEntries() {
      List<PartitionStatsEntry> partitionStatsEntries = Lists.newArrayList();
      partitions.values().forEach(partition ->
          partitionStatsEntries.add(new GenericPartitionStatsEntry(
              (PartitionData) partition.key,
              partition.fileCount,
              partition.recordCount)));
      return partitionStatsEntries;
    }

    void addPartitionStatsEntries(List<PartitionStatsEntry> partitionStatsEntries) {
      if (Objects.isNull(partitionStatsEntries)) {
        return;
      }
      partitionStatsEntries.forEach(entry -> this.get(entry.getPartition()).add(entry.getFileCount(),
          entry.getRowCount()));
    }

    void subtractFromPartitionStatsEntries(List<PartitionStatsEntry> partitionStatsEntries) {
      partitionStatsEntries.forEach(entry -> {
            Partition partition = this.get(entry.getPartition());
            partition.setFileAndRecordCount(entry.getFileCount() - partition.getFileCount(),
                entry.getRowCount() - partition.getRecordCount());
          }
      );
    }

    void subtractPartitionMap(PartitionMap partitionMap) {
      partitionMap.all().forEach(subPartition -> {
        Partition partition = this.get(subPartition.key);
        partition.setFileAndRecordCount(partition.getFileCount() - subPartition.getFileCount(),
            partition.getRecordCount() - subPartition.getRecordCount());
      });
    }

  }

  static class Partition {
    private final StructLike key;
    private long recordCount;
    private int fileCount;

    Partition(StructLike key) {
      this.key = key;
      this.recordCount = 0;
      this.fileCount = 0;
    }

    synchronized void update(DataFile file) {
      this.recordCount = this.recordCount + file.recordCount();
      this.fileCount = this.fileCount + 1;
    }

    synchronized void add(int fileCnt, long rowCnt) {
      this.fileCount = this.fileCount + fileCnt;
      this.recordCount = this.recordCount + rowCnt;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public int getFileCount() {
      return fileCount;
    }

    public synchronized void setFileAndRecordCount(int fCount, long rCount) {
      this.fileCount = fCount;
      this.recordCount = rCount;
    }
  }

  private class PartitionsScan extends StaticTableScan {
    PartitionsScan() {
      super(ops, table, PartitionsTable.this.schema(), PartitionsTable.this::task);
    }
  }
}
