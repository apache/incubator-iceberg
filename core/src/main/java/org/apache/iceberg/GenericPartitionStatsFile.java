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

import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

public class GenericPartitionStatsFile implements PartitionStatsFile {
  private int partitionSpecId;
  private PartitionData partitionData;
  private int fileCount;
  private Long rowCount;

  /**
   * @param partitionSpecId defines {@link PartitionSpec} used for this partition data
   * @param partitionData   {@link PartitionData} tuple. schema of this is define by partitionSpecId
   * @param fileCount       Total Number of data file count in this partition
   * @param rowCount        Total Number of rows in table in this partition.
   *                        <p>
   *                        PS : 1. In PartitionStats file ParitionSpec-PartitionData will be key. 2. When partition
   *                        evolution happens, new entries in Partition stats table will use new spec. This will help in
   *                        query planning to get more accurate stats.
   */

  public GenericPartitionStatsFile(int partitionSpecId, PartitionData partitionData, int fileCount, Long rowCount) {
    this.partitionSpecId = partitionSpecId;
    this.partitionData = partitionData;
    this.fileCount = fileCount;
    this.rowCount = rowCount;
  }

  public int getPartitionSpecId() {
    return partitionSpecId;
  }

  public PartitionData getPartitionData() {
    return partitionData;
  }

  public int getFileCount() {
    return fileCount;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public Schema getAvroSchema(Types.StructType partitionStruct) {
    return AvroSchemaUtil.convert(PartitionStatsFile.getType(partitionStruct),
        ImmutableMap.of(partitionStruct, PartitionData.class.getName()));
  }
}
