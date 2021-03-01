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

import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

public interface PartitionStatsEntry {

  int PARTITION_ID = 600;
  String PARTITION_NAME = "partition";
  String PARTITION_DOC = "Partition data tuple, schema based on the partition spec";
  Types.NestedField FILE_COUNT = required(601, "file_count", Types.IntegerType.get(),
      "Number of records in this partition");
  Types.NestedField ROW_COUNT = required(602, "row_count", Types.LongType.get(),
      "Number of records in this partition");

  static Types.StructType getType(Types.StructType partitionType) {
    return Types.StructType.of(
        required(PARTITION_ID, PARTITION_NAME, partitionType, PARTITION_DOC),
        FILE_COUNT,
        ROW_COUNT
    );
  }

  static Schema getSchema(Types.StructType partitionType) {
    return new Schema(required(PARTITION_ID, PARTITION_NAME, partitionType, PARTITION_DOC), FILE_COUNT, ROW_COUNT);
  }

  /**
   * Returns partition for this file as a {@link StructLike}.
   */
  StructLike getPartition();

  /**
   * Returns the number of records in the partition.
   */
  int getFileCount();

  /**
   * Returns the number of records in the partition.
   */
  long getRowCount();
}
