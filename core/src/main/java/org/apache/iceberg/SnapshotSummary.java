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
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Joiner.MapJoiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class SnapshotSummary {
  public static final String ADDED_FILES_PROP = "added-data-files";
  public static final String DELETED_FILES_PROP = "deleted-data-files";
  public static final String TOTAL_DATA_FILES_PROP = "total-data-files";
  public static final String ADDED_DELETE_FILES_PROP = "added-delete-files";
  public static final String REMOVED_DELETE_FILES_PROP = "removed-delete-files";
  public static final String TOTAL_DELETE_FILES_PROP = "total-delete-files";
  public static final String ADDED_RECORDS_PROP = "added-records";
  public static final String DELETED_RECORDS_PROP = "deleted-records";
  public static final String TOTAL_RECORDS_PROP = "total-records";
  public static final String ADDED_FILE_SIZE_PROP = "added-files-size";
  public static final String REMOVED_FILE_SIZE_PROP = "removed-files-size";
  public static final String ADDED_POS_DELETES_PROP = "added-position-deletes";
  public static final String REMOVED_POS_DELETES_PROP = "removed-position-deletes";
  public static final String TOTAL_POS_DELETES_PROP = "total-position-deletes";
  public static final String ADDED_EQ_DELETES_PROP = "added-equality-deletes";
  public static final String REMOVED_EQ_DELETES_PROP = "removed-equality-deletes";
  public static final String TOTAL_EQ_DELETES_PROP = "total-equality-deletes";
  public static final String DELETED_DUPLICATE_FILES = "deleted-duplicate-files";
  public static final String CHANGED_PARTITION_COUNT_PROP = "changed-partition-count";
  public static final String CHANGED_PARTITION_PREFIX = "partitions.";
  public static final String PARTITION_SUMMARY_PROP = "partition-summaries-included";
  public static final String STAGED_WAP_ID_PROP = "wap.id";
  public static final String PUBLISHED_WAP_ID_PROP = "published-wap-id";
  public static final String SOURCE_SNAPSHOT_ID_PROP = "source-snapshot-id";
  public static final String REPLACE_PARTITIONS_PROP = "replace-partitions";
  public static final String EXTRA_METADATA_PREFIX = "snapshot-property.";

  public static final MapJoiner MAP_JOINER = Joiner.on(",").withKeyValueSeparator("=");

  private SnapshotSummary() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    // commit summary tracking
    private final Map<String, String> properties = Maps.newHashMap();
    private final Map<String, UpdateMetrics> partitionMetrics = Maps.newHashMap();
    private final UpdateMetrics metrics = new UpdateMetrics();
    private int maxChangedPartitionsForSummaries = 0;
    private long deletedDuplicateFiles = 0L;
    private boolean trustPartitionMetrics = true;

    public void clear() {
      partitionMetrics.clear();
      metrics.clear();
      this.deletedDuplicateFiles = 0L;
      this.trustPartitionMetrics = true;
    }

    /**
     * Sets the maximum number of changed partitions before partition summaries will be excluded.
     * <p>
     * If the number of changed partitions is over this max, summaries will not be included. If the number of changed
     * partitions is <= this limit, then partition-level summaries will be included in the summary if they are
     * available, and "partition-summaries-included" will be set to "true".
     *
     * @param max maximum number of changed partitions
     */
    public void setPartitionSummaryLimit(int max) {
      this.maxChangedPartitionsForSummaries = max;
    }

    public void incrementDuplicateDeletes() {
      this.deletedDuplicateFiles += 1;
    }

    public void incrementDuplicateDeletes(int increment) {
      this.deletedDuplicateFiles += increment;
    }

    public void addedFile(PartitionSpec spec, DataFile file) {
      metrics.addedFile(file);
      updatePartitions(spec, file, true);
    }

    public void addedFile(PartitionSpec spec, DeleteFile file) {
      metrics.addedFile(file);
      updatePartitions(spec, file, true);
    }

    public void deletedFile(PartitionSpec spec, ContentFile<?> file) {
      if (file instanceof DataFile) {
        deletedFile(spec, (DataFile) file);
      } else if (file instanceof DeleteFile) {
        deletedFile(spec, (DeleteFile) file);
      } else {
        throw new IllegalArgumentException("Unsupported file type: " + file.getClass().getSimpleName());
      }
    }

    public void deletedFile(PartitionSpec spec, DataFile file) {
      metrics.removedFile(file);
      updatePartitions(spec, file, false);
    }

    public void deletedFile(PartitionSpec spec, DeleteFile file) {
      metrics.removedFile(file);
      updatePartitions(spec, file, false);
    }

    public void addedManifest(ManifestFile manifest) {
      this.trustPartitionMetrics = false;
      partitionMetrics.clear();
      metrics.addedManifest(manifest);
    }

    public void set(String property, String value) {
      properties.put(property, value);
    }

    private void updatePartitions(PartitionSpec spec, ContentFile<?> file, boolean isAddition) {
      if (trustPartitionMetrics) {
        UpdateMetrics partMetrics = partitionMetrics.computeIfAbsent(
            spec.partitionToPath(file.partition()),
            key -> new UpdateMetrics());

        if (isAddition) {
          partMetrics.addedFile(file);
        } else {
          partMetrics.removedFile(file);
        }
      }
    }

    public void merge(SnapshotSummary.Builder builder) {
      properties.putAll(builder.properties);
      metrics.merge(builder.metrics);

      this.trustPartitionMetrics = trustPartitionMetrics && builder.trustPartitionMetrics;
      if (trustPartitionMetrics) {
        for (Map.Entry<String, UpdateMetrics> entry : builder.partitionMetrics.entrySet()) {
          partitionMetrics.computeIfAbsent(entry.getKey(), key -> new UpdateMetrics()).merge(entry.getValue());
        }
      } else {
        partitionMetrics.clear();
      }

      this.deletedDuplicateFiles += builder.deletedDuplicateFiles;
    }

    public Map<String, String> build() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      // copy custom summary properties
      builder.putAll(properties);

      metrics.addTo(builder);
      setIf(deletedDuplicateFiles > 0, builder, DELETED_DUPLICATE_FILES, deletedDuplicateFiles);
      Set<String> changedPartitions = partitionMetrics.keySet();
      setIf(trustPartitionMetrics, builder, CHANGED_PARTITION_COUNT_PROP, changedPartitions.size());

      if (trustPartitionMetrics && changedPartitions.size() <= maxChangedPartitionsForSummaries) {
        setIf(changedPartitions.size() > 0, builder, PARTITION_SUMMARY_PROP, "true");
        for (String key : changedPartitions) {
          setIf(key != null, builder, CHANGED_PARTITION_PREFIX + key, partitionSummary(partitionMetrics.get(key)));
        }
      }

      return builder.build();
    }

    private static String partitionSummary(UpdateMetrics metrics) {
      ImmutableMap.Builder<String, String> partBuilder = ImmutableMap.builder();
      metrics.addTo(partBuilder);
      return MAP_JOINER.join(partBuilder.build());
    }
  }

  private static class UpdateMetrics {
    private long addedSize = 0L;
    private long removedSize = 0L;
    private int addedFiles = 0;
    private int removedFiles = 0;
    private int addedDeleteFiles = 0;
    private int removedDeleteFiles = 0;
    private long addedRecords = 0L;
    private long deletedRecords = 0L;
    private long addedPosDeletes = 0L;
    private long removedPosDeletes = 0L;
    private long addedEqDeletes = 0L;
    private long removedEqDeletes = 0L;
    private boolean trustSizeAndDeleteCounts = true;

    void clear() {
      this.addedSize = 0L;
      this.removedSize = 0L;
      this.addedFiles = 0;
      this.removedFiles = 0;
      this.addedDeleteFiles = 0;
      this.removedDeleteFiles = 0;
      this.addedRecords = 0L;
      this.deletedRecords = 0L;
      this.addedPosDeletes = 0L;
      this.removedPosDeletes = 0L;
      this.addedEqDeletes = 0L;
      this.removedEqDeletes = 0L;
      this.trustSizeAndDeleteCounts = true;
    }

    void addTo(ImmutableMap.Builder<String, String> builder) {
      setIf(addedFiles > 0, builder, ADDED_FILES_PROP, addedFiles);
      setIf(removedFiles > 0, builder, DELETED_FILES_PROP, removedFiles);
      setIf(addedDeleteFiles > 0, builder, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
      setIf(removedDeleteFiles > 0, builder, REMOVED_DELETE_FILES_PROP, removedDeleteFiles);
      setIf(addedRecords > 0, builder, ADDED_RECORDS_PROP, addedRecords);
      setIf(deletedRecords > 0, builder, DELETED_RECORDS_PROP, deletedRecords);

      if (trustSizeAndDeleteCounts) {
        setIf(addedSize > 0, builder, ADDED_FILE_SIZE_PROP, addedSize);
        setIf(removedSize > 0, builder, REMOVED_FILE_SIZE_PROP, removedSize);
        setIf(addedPosDeletes > 0, builder, ADDED_POS_DELETES_PROP, addedPosDeletes);
        setIf(removedPosDeletes > 0, builder, REMOVED_POS_DELETES_PROP, removedPosDeletes);
        setIf(addedEqDeletes > 0, builder, ADDED_EQ_DELETES_PROP, addedEqDeletes);
        setIf(removedEqDeletes > 0, builder, REMOVED_EQ_DELETES_PROP, removedEqDeletes);
      }
    }

    void addedFile(ContentFile<?> file) {
      this.addedSize += file.fileSizeInBytes();
      switch (file.content()) {
        case DATA:
          this.addedFiles += 1;
          this.addedRecords += file.recordCount();
          break;
        case POSITION_DELETES:
          this.addedDeleteFiles += 1;
          this.addedPosDeletes += file.recordCount();
          break;
        case EQUALITY_DELETES:
          this.addedDeleteFiles += 1;
          this.addedEqDeletes += file.recordCount();
          break;
        default:
          throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
      }
    }

    void removedFile(ContentFile<?> file) {
      this.removedSize += file.fileSizeInBytes();
      switch (file.content()) {
        case DATA:
          this.removedFiles += 1;
          this.deletedRecords += file.recordCount();
          break;
        case POSITION_DELETES:
          this.removedDeleteFiles += 1;
          this.removedPosDeletes += file.recordCount();
          break;
        case EQUALITY_DELETES:
          this.removedDeleteFiles += 1;
          this.removedEqDeletes += file.recordCount();
          break;
        default:
          throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
      }
    }

    void addedManifest(ManifestFile manifest) {
      switch (manifest.content()) {
        case DATA:
          this.addedFiles += manifest.addedFilesCount();
          this.addedRecords += manifest.addedRowsCount();
          this.removedFiles += manifest.deletedFilesCount();
          this.deletedRecords += manifest.deletedRowsCount();
          break;
        case DELETES:
          this.addedDeleteFiles += manifest.addedFilesCount();
          this.removedDeleteFiles += manifest.deletedFilesCount();
          this.trustSizeAndDeleteCounts = false;
          break;
      }
    }

    void merge(UpdateMetrics other) {
      this.addedFiles += other.addedFiles;
      this.removedFiles += other.removedFiles;
      this.addedDeleteFiles += other.addedDeleteFiles;
      this.removedDeleteFiles += other.removedDeleteFiles;
      this.addedSize += other.addedSize;
      this.removedSize += other.removedSize;
      this.addedRecords += other.addedRecords;
      this.deletedRecords += other.deletedRecords;
      this.addedPosDeletes += other.addedPosDeletes;
      this.removedPosDeletes += other.removedPosDeletes;
      this.addedEqDeletes += other.addedEqDeletes;
      this.removedEqDeletes += other.removedEqDeletes;
      this.trustSizeAndDeleteCounts = trustSizeAndDeleteCounts && other.trustSizeAndDeleteCounts;
    }
  }

  private static void setIf(boolean expression, ImmutableMap.Builder<String, String> builder,
                            String property, Object value) {
    if (expression) {
      builder.put(property, String.valueOf(value));
    }
  }
}
