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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FindFiles {
  private FindFiles() {
  }

  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public static Builder in(Table table) {
    return new Builder(table);
  }

  public static class Builder {
    private final Table table;
    private final TableOperations ops;
    private boolean caseSensitive = true;
    private Long snapshotId = null;
    private Expression rowFilter = Expressions.alwaysTrue();
    private Expression fileFilter = Expressions.alwaysTrue();
    private Expression partitionFilter = Expressions.alwaysTrue();

    public Builder(Table table) {
      this.table = table;
      this.ops = ((HasTableOperations) table).operations();
    }

    public Builder caseInsensitive() {
      this.caseSensitive = false;
      return this;
    }

    public Builder caseSensitive(boolean findCaseSensitive) {
      this.caseSensitive = findCaseSensitive;
      return this;
    }

    /**
     * Base results on the given snapshot.
     *
     * @param findSnapshotId a snapshot ID
     * @return this for method chaining
     */
    public Builder inSnapshot(long findSnapshotId) {
      Preconditions.checkArgument(this.snapshotId == null,
          "Cannot set snapshot multiple times, already set to id=%s", findSnapshotId);
      Preconditions.checkArgument(table.snapshot(findSnapshotId) != null,
          "Cannot find snapshot for id=%s", findSnapshotId);
      this.snapshotId = findSnapshotId;
      return this;
    }

    /**
     * Base results on files in the snapshot that was current as of a timestamp.
     *
     * @param timestampMillis a timestamp in milliseconds
     * @return this for method chaining
     */
    public Builder asOfTime(long timestampMillis) {
      Preconditions.checkArgument(this.snapshotId == null,
          "Cannot set snapshot multiple times, already set to id=%s", snapshotId);

      Long lastSnapshotId = null;
      for (HistoryEntry logEntry : ops.current().snapshotLog()) {
        if (logEntry.timestampMillis() <= timestampMillis) {
          lastSnapshotId = logEntry.snapshotId();
        } else {
          // the last snapshot ID was the last one older than the timestamp
          break;
        }
      }

      // the snapshot ID could be null if no entries were older than the requested time. in that
      // case, there is no valid snapshot to read.
      Preconditions.checkArgument(lastSnapshotId != null,
          "Cannot find a snapshot older than %s",
          DATE_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneId.systemDefault())));
      return inSnapshot(lastSnapshotId);
    }

    /**
     * Filter results using a record filter. Files that may contain at least one matching record
     * will be returned by {@link #collect()}.
     *
     * @param expr a record filter
     * @return this for method chaining
     */
    public Builder withRecordsMatching(Expression expr) {
      this.rowFilter = Expressions.and(rowFilter, expr);
      return this;
    }

    /**
     * Filter results using a metadata filter for the data in a {@link DataFile}.
     *
     * @param expr a filter for {@link DataFile} metadata columns
     * @return this for method chaining
     */
    public Builder withMetadataMatching(Expression expr) {
      this.fileFilter = Expressions.and(fileFilter, expr);
      return this;
    }

    /**
     * Filter results to files in any one of the given partitions.
     *
     * @param spec a spec for the partitions
     * @param partition a StructLike that stores a partition tuple
     * @return this for method chaining
     */
    public Builder inPartition(PartitionSpec spec, StructLike partition) {
      return inPartitions(spec, partition);
    }

    /**
     * Filter results to files in any one of the given partitions.
     *
     * @param spec a spec for the partitions
     * @param partitions one or more StructLike that stores a partition tuple
     * @return this for method chaining
     */
    public Builder inPartitions(PartitionSpec spec, StructLike... partitions) {
      return inPartitions(spec, Arrays.asList(partitions));
    }

    /**
     * Filter results to files in any one of the given partitions.
     *
     * @param spec a spec for the partitions
     * @param partitions a list of StructLike that stores a partition tuple
     * @return this for method chaining
     */
    public Builder inPartitions(PartitionSpec spec, List<StructLike> partitions) {
      Preconditions.checkArgument(spec.equals(ops.current().spec(spec.specId())),
          "Partition spec does not belong to table: %s", table);

      Expression partitionSetFilter = Expressions.alwaysFalse();
      for (StructLike partitionData : partitions) {
        Expression partFilter = Expressions.alwaysTrue();
        for (int i = 0; i < spec.fields().size(); i += 1) {
          PartitionField field = spec.fields().get(i);
          partFilter = Expressions.and(
              partFilter,
              Expressions.equal(field.name(), partitionData.get(i, Object.class)));
        }
        partitionSetFilter = Expressions.or(partitionSetFilter, partFilter);
      }

      if (partitionFilter != Expressions.alwaysTrue()) {
        this.partitionFilter = Expressions.or(partitionFilter, partitionSetFilter);
      } else {
        this.partitionFilter = partitionSetFilter;
      }

      return this;
    }

    /**
     * Returns all files in the table that match all of the filters.
     */
    public CloseableIterable<DataFile> collect() {
      Snapshot snapshot = snapshotId != null ?
          ops.current().snapshot(snapshotId) : ops.current().currentSnapshot();

      // snapshot could be null when the table just gets created
      if (snapshot == null) {
        return CloseableIterable.empty();
      }

      // when snapshot is not null
      CloseableIterable<ManifestEntry<DataFile>> entries = new ManifestGroup(
          ops.io(), ops.encryption(), snapshot.dataManifests())
          .specsById(ops.current().specsById())
          .filterData(rowFilter)
          .filterFiles(fileFilter)
          .filterPartitions(partitionFilter)
          .ignoreDeleted()
          .caseSensitive(caseSensitive)
          .entries();

      return CloseableIterable.transform(entries, entry -> entry.file().copyWithoutStats());
    }
  }
}
