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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

class V2Metadata {
  private V2Metadata() {
  }

  static final Schema MANIFEST_LIST_SCHEMA = new Schema(
      ManifestFile.PATH,
      ManifestFile.LENGTH,
      ManifestFile.SPEC_ID,
      ManifestFile.MANIFEST_CONTENT.asRequired(),
      ManifestFile.SEQUENCE_NUMBER.asRequired(),
      ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),
      ManifestFile.SNAPSHOT_ID.asRequired(),
      ManifestFile.ADDED_FILES_COUNT.asRequired(),
      ManifestFile.EXISTING_FILES_COUNT.asRequired(),
      ManifestFile.DELETED_FILES_COUNT.asRequired(),
      ManifestFile.ADDED_ROWS_COUNT.asRequired(),
      ManifestFile.EXISTING_ROWS_COUNT.asRequired(),
      ManifestFile.DELETED_ROWS_COUNT.asRequired(),
      ManifestFile.PARTITION_SUMMARIES
  );

  /**
   * A wrapper class to write any ManifestFile implementation to Avro using the v2 write schema.
   *
   * This is used to maintain compatibility with v2 by writing manifest list files with the old schema, instead of
   * writing a sequence number into metadata files in v2 tables.
   */
  static class IndexedManifestFile implements ManifestFile, IndexedRecord {
    private static final org.apache.avro.Schema AVRO_SCHEMA =
        AvroSchemaUtil.convert(MANIFEST_LIST_SCHEMA, "manifest_file");

    private final long commitSnapshotId;
    private final long sequenceNumber;
    private ManifestFile wrapped = null;

    IndexedManifestFile(long commitSnapshotId, long sequenceNumber) {
      this.commitSnapshotId = commitSnapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    public ManifestFile wrap(ManifestFile file) {
      this.wrapped = file;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return AVRO_SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot modify IndexedManifestFile wrapper via put");
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.path();
        case 1:
          return wrapped.length();
        case 2:
          return wrapped.partitionSpecId();
        case 3:
          return wrapped.content().id();
        case 4:
          if (wrapped.sequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // if the sequence number is being assigned here, then the manifest must be created by the current
            // operation. to validate this, check that the snapshot id matches the current commit
            Preconditions.checkState(commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s", wrapped.snapshotId());
            return sequenceNumber;
          } else {
            return wrapped.sequenceNumber();
          }
        case 5:
          if (wrapped.minSequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // same sanity check as above
            Preconditions.checkState(commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s", wrapped.snapshotId());
            // if the min sequence number is not determined, then there was no assigned sequence number for any file
            // written to the wrapped manifest. replace the unassigned sequence number with the one for this commit
            return sequenceNumber;
          } else {
            return wrapped.minSequenceNumber();
          }
        case 6:
          return wrapped.snapshotId();
        case 7:
          return wrapped.addedFilesCount();
        case 8:
          return wrapped.existingFilesCount();
        case 9:
          return wrapped.deletedFilesCount();
        case 10:
          return wrapped.addedRowsCount();
        case 11:
          return wrapped.existingRowsCount();
        case 12:
          return wrapped.deletedRowsCount();
        case 13:
          return wrapped.partitions();
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String path() {
      return wrapped.path();
    }

    @Override
    public long length() {
      return wrapped.length();
    }

    @Override
    public int partitionSpecId() {
      return wrapped.partitionSpecId();
    }

    @Override
    public ManifestContent content() {
      return wrapped.content();
    }

    @Override
    public long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return wrapped.minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public boolean hasAddedFiles() {
      return wrapped.hasAddedFiles();
    }

    @Override
    public Integer addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public boolean hasExistingFiles() {
      return wrapped.hasExistingFiles();
    }

    @Override
    public Integer existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public boolean hasDeletedFiles() {
      return wrapped.hasDeletedFiles();
    }

    @Override
    public Integer deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return wrapped.partitions();
    }

    @Override
    public ManifestFile copy() {
      return wrapped.copy();
    }
  }

  static Schema entrySchema(Types.StructType partitionType) {
    return wrapFileSchema(fileType(partitionType));
  }

  static Schema wrapFileSchema(Types.StructType fileSchema) {
    // this is used to build projection schemas
    return new Schema(
        ManifestEntry.STATUS, ManifestEntry.SNAPSHOT_ID, ManifestEntry.SEQUENCE_NUMBER,
        required(ManifestEntry.DATA_FILE_ID, "data_file", fileSchema));
  }

  static Types.StructType fileType(Types.StructType partitionType) {
    return Types.StructType.of(
        DataFile.CONTENT.asRequired(),
        DataFile.FILE_PATH,
        DataFile.FILE_FORMAT,
        required(DataFile.PARTITION_ID, DataFile.PARTITION_NAME, partitionType, DataFile.PARTITION_DOC),
        DataFile.RECORD_COUNT,
        DataFile.FILE_SIZE,
        DataFile.COLUMN_SIZES,
        DataFile.VALUE_COUNTS,
        DataFile.NULL_VALUE_COUNTS,
        DataFile.NAN_VALUE_COUNTS,
        DataFile.LOWER_BOUNDS,
        DataFile.UPPER_BOUNDS,
        DataFile.KEY_METADATA,
        DataFile.SPLIT_OFFSETS,
        DataFile.EQUALITY_IDS,
        DataFile.SORT_ORDER_ID
    );
  }

  static class IndexedManifestEntry<F extends ContentFile<F>> implements ManifestEntry<F>, IndexedRecord {
    private final org.apache.avro.Schema avroSchema;
    private final Long commitSnapshotId;
    private final IndexedDataFile<?> fileWrapper;
    private ManifestEntry<F> wrapped = null;

    IndexedManifestEntry(Long commitSnapshotId, Types.StructType partitionType) {
      this.avroSchema = AvroSchemaUtil.convert(entrySchema(partitionType), "manifest_entry");
      this.commitSnapshotId = commitSnapshotId;
      this.fileWrapper = new IndexedDataFile<>(partitionType);
    }

    public IndexedManifestEntry<F> wrap(ManifestEntry<F> entry) {
      this.wrapped = entry;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return avroSchema;
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot modify IndexedManifestEntry wrapper via put");
    }

    @Override
    public Object get(int i) {
      switch (i) {
        case 0:
          return wrapped.status().id();
        case 1:
          return wrapped.snapshotId();
        case 2:
          if (wrapped.sequenceNumber() == null) {
            // if the entry's sequence number is null, then it will inherit the sequence number of the current commit.
            // to validate that this is correct, check that the snapshot id is either null (will also be inherited) or
            // that it matches the id of the current commit.
            Preconditions.checkState(
                wrapped.snapshotId() == null || wrapped.snapshotId().equals(commitSnapshotId),
                "Found unassigned sequence number for an entry from snapshot: %s", wrapped.snapshotId());
            return null;
          }
          return wrapped.sequenceNumber();
        case 3:
          return fileWrapper.wrap(wrapped.file());
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + i);
      }
    }

    @Override
    public Status status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      wrapped.setSnapshotId(snapshotId);
    }

    @Override
    public Long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public void setSequenceNumber(long sequenceNumber) {
      wrapped.setSequenceNumber(sequenceNumber);
    }

    @Override
    public F file() {
      return wrapped.file();
    }

    @Override
    public ManifestEntry<F> copy() {
      return wrapped.copy();
    }

    @Override
    public ManifestEntry<F> copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }
  }

  /**
   * Wrapper used to write DataFile or DeleteFile to v2 metadata.
   */
  static class IndexedDataFile<F> implements ContentFile<F>, IndexedRecord {
    private final org.apache.avro.Schema avroSchema;
    private final IndexedStructLike partitionWrapper;
    private ContentFile<F> wrapped = null;

    IndexedDataFile(Types.StructType partitionType) {
      this.avroSchema = AvroSchemaUtil.convert(fileType(partitionType), "data_file");
      this.partitionWrapper = new IndexedStructLike(avroSchema.getField("partition").schema());
    }

    @SuppressWarnings("unchecked")
    IndexedDataFile<F> wrap(ContentFile<?> file) {
      this.wrapped = (ContentFile<F>) file;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return avroSchema;
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.content().id();
        case 1:
          return wrapped.path().toString();
        case 2:
          return wrapped.format() != null ? wrapped.format().toString() : null;
        case 3:
          return partitionWrapper.wrap(wrapped.partition());
        case 4:
          return wrapped.recordCount();
        case 5:
          return wrapped.fileSizeInBytes();
        case 6:
          return wrapped.columnSizes();
        case 7:
          return wrapped.valueCounts();
        case 8:
          return wrapped.nullValueCounts();
        case 9:
          return wrapped.nanValueCounts();
        case 10:
          return wrapped.lowerBounds();
        case 11:
          return wrapped.upperBounds();
        case 12:
          return wrapped.keyMetadata();
        case 13:
          return wrapped.splitOffsets();
        case 14:
          return wrapped.equalityFieldIds();
        case 15:
          return wrapped.sortOrderId();
      }
      throw new IllegalArgumentException("Unknown field ordinal: " + pos);
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot modify IndexedDataFile wrapper via put");
    }

    @Override
    public Long pos() {
      return null;
    }

    @Override
    public int specId() {
      return wrapped.specId();
    }

    @Override
    public FileContent content() {
      return wrapped.content();
    }

    @Override
    public CharSequence path() {
      return wrapped.path();
    }

    @Override
    public FileFormat format() {
      return wrapped.format();
    }

    @Override
    public StructLike partition() {
      return wrapped.partition();
    }

    @Override
    public long recordCount() {
      return wrapped.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return wrapped.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return wrapped.columnSizes();
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return wrapped.valueCounts();
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return wrapped.nullValueCounts();
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return wrapped.nanValueCounts();
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return wrapped.lowerBounds();
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return wrapped.upperBounds();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return wrapped.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return wrapped.splitOffsets();
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return wrapped.equalityFieldIds();
    }

    @Override
    public Integer sortOrderId() {
      return wrapped.sortOrderId();
    }

    @Override
    public F copy() {
      throw new UnsupportedOperationException("Cannot copy IndexedDataFile wrapper");
    }

    @Override
    public F copyWithoutStats() {
      throw new UnsupportedOperationException("Cannot copy IndexedDataFile wrapper");
    }
  }

  /**
   * Wrapper used to write PartitionStats entry to v2 metadata.
   */
  static class IndexedPartitionStatsEntry<F> implements PartitionStatsEntry, IndexedRecord {
    private final org.apache.avro.Schema avroSchema;
    private final IndexedStructLike partitionWrapper;
    private PartitionStatsEntry wrapped = null;

    IndexedPartitionStatsEntry(Types.StructType partitionType) {
      this.avroSchema = AvroSchemaUtil.convert(fileType(partitionType), "partitionStats_file");
      this.partitionWrapper = new IndexedStructLike(avroSchema.getField("partition").schema());
    }

    @SuppressWarnings("unchecked")
    PartitionStatsEntry wrap(PartitionStatsEntry partitionStatsEntry) {
      this.wrapped = partitionStatsEntry;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return avroSchema;
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return partitionWrapper.wrap(wrapped.getPartition());
        case 1:
          return wrapped.getFileCount();
        case 2:
          return wrapped.getRowCount();
      }
      throw new IllegalArgumentException("Unknown field ordinal: " + pos);
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot read into IndexedPartitionStatsFile");
    }

    @Override
    public StructLike getPartition() {
      return wrapped.getPartition();
    }

    @Override
    public int getFileCount() {
      return wrapped.getFileCount();
    }

    @Override
    public long getRowCount() {
      return wrapped.getRowCount();
    }
  }
}
