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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Interface for data files listed in a table manifest.
 */
public interface DataFile extends ContentFile<DataFile>, Serializable {
  // fields for adding delete data files
  Types.NestedField CONTENT = optional(134, "content", IntegerType.get(),
      "Contents of the file: 0=data, 1=position deletes, 2=equality deletes");
  Types.NestedField FILE_PATH = required(100, "file_path", StringType.get(), "Location URI with FS scheme");
  Types.NestedField FILE_FORMAT = required(101, "file_format", StringType.get(),
      "File format name: avro, orc, or parquet");
  Types.NestedField RECORD_COUNT = required(103, "record_count", LongType.get(), "Number of records in the file");
  Types.NestedField FILE_SIZE = required(104, "file_size_in_bytes", LongType.get(), "Total file size in bytes");
  Types.NestedField COLUMN_SIZES = optional(108, "column_sizes", MapType.ofRequired(117, 118,
      IntegerType.get(), LongType.get()), "Map of column id to total size on disk");
  Types.NestedField VALUE_COUNTS = optional(109, "value_counts", MapType.ofRequired(119, 120,
      IntegerType.get(), LongType.get()), "Map of column id to total count, including null and NaN");
  Types.NestedField NULL_VALUE_COUNTS = optional(110, "null_value_counts", MapType.ofRequired(121, 122,
      IntegerType.get(), LongType.get()), "Map of column id to null value count");
  Types.NestedField NAN_VALUE_COUNTS = optional(137, "nan_value_counts", MapType.ofRequired(138, 139,
      IntegerType.get(), LongType.get()), "Map of column id to number of NaN values in the column");
  Types.NestedField LOWER_BOUNDS = optional(125, "lower_bounds", MapType.ofRequired(126, 127,
      IntegerType.get(), BinaryType.get()), "Map of column id to lower bound");
  Types.NestedField UPPER_BOUNDS = optional(128, "upper_bounds", MapType.ofRequired(129, 130,
      IntegerType.get(), BinaryType.get()), "Map of column id to upper bound");
  Types.NestedField KEY_METADATA = optional(131, "key_metadata", BinaryType.get(), "Encryption key metadata blob");
  Types.NestedField SPLIT_OFFSETS = optional(132, "split_offsets", ListType.ofRequired(133, LongType.get()),
      "Splittable offsets");
  Types.NestedField EQUALITY_IDS = optional(135, "equality_ids", ListType.ofRequired(136, IntegerType.get()),
      "Equality comparison field IDs");

  int PARTITION_ID = 102;
  String PARTITION_NAME = "partition";
  String PARTITION_DOC = "Partition data tuple, schema based on the partition spec";
  // NEXT ID TO ASSIGN: 140

  static StructType getType(StructType partitionType) {
    // IDs start at 100 to leave room for changes to ManifestEntry
    return StructType.of(
        CONTENT,
        FILE_PATH,
        FILE_FORMAT,
        required(PARTITION_ID, PARTITION_NAME, partitionType, PARTITION_DOC),
        RECORD_COUNT,
        FILE_SIZE,
        COLUMN_SIZES,
        VALUE_COUNTS,
        NULL_VALUE_COUNTS,
        NAN_VALUE_COUNTS,
        LOWER_BOUNDS,
        UPPER_BOUNDS,
        KEY_METADATA,
        SPLIT_OFFSETS,
        EQUALITY_IDS
    );
  }

  /**
   * @return the content stored in the file; one of DATA, POSITION_DELETES, or EQUALITY_DELETES
   */
  @Override
  default FileContent content() {
    return FileContent.DATA;
  }

  @Override
  default List<Integer> equalityFieldIds() {
    return null;
  }
}
