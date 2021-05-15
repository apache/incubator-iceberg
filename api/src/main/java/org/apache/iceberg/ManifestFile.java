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
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Represents a manifest file that can be scanned to find data files in a table.
 */
public interface ManifestFile {
  Types.NestedField PATH = required(500, "manifest_path", Types.StringType.get(), "Location URI with FS scheme");
  Types.NestedField LENGTH = required(501, "manifest_length", Types.LongType.get(), "Total file size in bytes");
  Types.NestedField SPEC_ID = required(502, "partition_spec_id", Types.IntegerType.get(), "Spec ID used to write");
  Types.NestedField MANIFEST_CONTENT = optional(517, "content", Types.IntegerType.get(),
      "Contents of the manifest: 0=data, 1=deletes");
  Types.NestedField SEQUENCE_NUMBER = optional(515, "sequence_number", Types.LongType.get(),
      "Sequence number when the manifest was added");
  Types.NestedField MIN_SEQUENCE_NUMBER = optional(516, "min_sequence_number", Types.LongType.get(),
      "Lowest sequence number in the manifest");
  Types.NestedField SNAPSHOT_ID = optional(503, "added_snapshot_id", Types.LongType.get(),
      "Snapshot ID that added the manifest");
  Types.NestedField ADDED_FILES_COUNT = optional(504, "added_data_files_count", Types.IntegerType.get(),
      "Added entry count");
  Types.NestedField EXISTING_FILES_COUNT = optional(505, "existing_data_files_count", Types.IntegerType.get(),
      "Existing entry count");
  Types.NestedField DELETED_FILES_COUNT = optional(506, "deleted_data_files_count", Types.IntegerType.get(),
      "Deleted entry count");
  Types.NestedField ADDED_ROWS_COUNT = optional(512, "added_rows_count", Types.LongType.get(),
      "Added rows count");
  Types.NestedField EXISTING_ROWS_COUNT = optional(513, "existing_rows_count", Types.LongType.get(),
      "Existing rows count");
  Types.NestedField DELETED_ROWS_COUNT = optional(514, "deleted_rows_count", Types.LongType.get(),
      "Deleted rows count");
  Types.StructType PARTITION_SUMMARY_TYPE = Types.StructType.of(
      required(509, "contains_null", Types.BooleanType.get(), "True if any file has a null partition value"),
      optional(518, "contains_nan", Types.BooleanType.get(), "True if any file has a nan partition value"),
      optional(510, "lower_bound", Types.BinaryType.get(), "Partition lower bound for all files"),
      optional(511, "upper_bound", Types.BinaryType.get(), "Partition upper bound for all files")
  );
  Types.NestedField PARTITION_SUMMARIES = optional(507, "partitions",
      Types.ListType.ofRequired(508, PARTITION_SUMMARY_TYPE),
      "Summary for each partition");
  Types.NestedField KEY_METADATA = optional(519, "key_metadata", Types.BinaryType.get(),
          "Encryption key metadata blob");
  // next ID to assign: 520

  Schema SCHEMA = new Schema(
      PATH, LENGTH, SPEC_ID, MANIFEST_CONTENT,
      SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER, SNAPSHOT_ID,
      ADDED_FILES_COUNT, EXISTING_FILES_COUNT, DELETED_FILES_COUNT,
      ADDED_ROWS_COUNT, EXISTING_ROWS_COUNT, DELETED_ROWS_COUNT,
      PARTITION_SUMMARIES, KEY_METADATA);

  static Schema schema() {
    return SCHEMA;
  }

  /**
   * Returns fully qualified path to the file, suitable for constructing a Hadoop Path.
   */
  String path();

  /**
   * Returns length of the manifest file.
   */
  long length();

  /**
   * Returns iD of the {@link PartitionSpec} used to write the manifest file.
   */
  int partitionSpecId();

  /**
   * Returns the content stored in the manifest; either DATA or DELETES.
   */
  ManifestContent content();

  /**
   * Returns the sequence number of the commit that added the manifest file.
   */
  long sequenceNumber();

  /**
   * Returns the lowest sequence number of any data file in the manifest.
   */
  long minSequenceNumber();

  /**
   * Returns iD of the snapshot that added the manifest file to table metadata.
   */
  Long snapshotId();

  /**
   * Returns true if the manifest contains ADDED entries or if the count is not known.
   *
   * @return whether this manifest contains entries with ADDED status
   */
  default boolean hasAddedFiles() {
    return addedFilesCount() == null || addedFilesCount() > 0;
  }

  /**
   * Returns the number of data files with status ADDED in the manifest file.
   */
  Integer addedFilesCount();

  /**
   * Returns the total number of rows in all data files with status ADDED in the manifest file.
   */
  Long addedRowsCount();

  /**
   * Returns true if the manifest contains EXISTING entries or if the count is not known.
   *
   * @return whether this manifest contains entries with EXISTING status
   */
  default boolean hasExistingFiles() {
    return existingFilesCount() == null || existingFilesCount() > 0;
  }

  /**
   * Returns the number of data files with status EXISTING in the manifest file.
   */
  Integer existingFilesCount();

  /**
   * Returns the total number of rows in all data files with status EXISTING in the manifest file.
   */
  Long existingRowsCount();

  /**
   * Returns true if the manifest contains DELETED entries or if the count is not known.
   *
   * @return whether this manifest contains entries with DELETED status
   */
  default boolean hasDeletedFiles() {
    return deletedFilesCount() == null || deletedFilesCount() > 0;
  }

  /**
   * Returns the number of data files with status DELETED in the manifest file.
   */
  Integer deletedFilesCount();

  /**
   * Returns the total number of rows in all data files with status DELETED in the manifest file.
   */
  Long deletedRowsCount();

  /**
   * Returns a list of {@link PartitionFieldSummary partition field summaries}.
   * <p>
   * Each summary corresponds to a field in the manifest file's partition spec, by ordinal. For
   * example, the partition spec [ ts_day=date(ts), type=identity(type) ] will have 2 summaries.
   * The first summary is for the ts_day partition field and the second is for the type partition
   * field.
   *
   * @return a list of partition field summaries, one for each field in the manifest's spec
   */
  List<PartitionFieldSummary> partitions();

  /**
   * Returns metadata about how this manifest file is encrypted, or null if the file is stored in plain text.
   */
  default ByteBuffer keyMetadata() {
    return null;
  }

  /**
   * Copies this {@link ManifestFile manifest file}. Readers can reuse manifest file instances; use
   * this method to make defensive copies.
   *
   * @return a copy of this manifest file
   */
  ManifestFile copy();

  /**
   * Summarizes the values of one partition field stored in a manifest file.
   */
  interface PartitionFieldSummary {
    static Types.StructType getType() {
      return PARTITION_SUMMARY_TYPE;
    }

    /**
     * Returns true if at least one data file in the manifest has a null value for the field.
     */
    boolean containsNull();

    /**
     * Returns true if at least one data file in the manifest has a NaN value for the field.
     * Null if this information doesn't exist.
     * <p>
     * Default to return null to ensure backward compatibility.
     */
    default Boolean containsNaN() {
      return null;
    }

    /**
     * Returns a ByteBuffer that contains a serialized bound lower than all values of the field.
     */
    ByteBuffer lowerBound();

    /**
     * Returns a ByteBuffer that contains a serialized bound higher than all values of the field.
     */
    ByteBuffer upperBound();

    /**
     * Copies this {@link PartitionFieldSummary summary}. Readers can reuse instances; use this
     * method to make defensive copies.
     *
     * @return a copy of this partition field summary
     */
    PartitionFieldSummary copy();
  }
}
