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
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

class GenericDataFile extends BaseFile<DataFile> implements DataFile {
  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  GenericDataFile(org.apache.avro.Schema avroSchema) {
    super(avroSchema);
  }

  GenericDataFile(int specId, String filePath, FileFormat format, PartitionData partition,
                  long fileSizeInBytes, Metrics metrics,
                  ByteBuffer keyMetadata, List<Long> splitOffsets, Integer sortOrderId) {
    super(specId, FileContent.DATA, filePath, format, partition, fileSizeInBytes, metrics.recordCount(),
        metrics.columnSizes(), metrics.valueCounts(), metrics.nullValueCounts(), metrics.nanValueCounts(),
        metrics.lowerBounds(), metrics.upperBounds(), splitOffsets, null, sortOrderId, keyMetadata);
  }

  /**
   * Used by Index File. recordCount == corresponding Data File recordCount
   */
  GenericDataFile(String filePath, FileFormat format, long fileSizeInBytes, long recordCount) {
    super(FileContent.INDEX_DATA, filePath, format, fileSizeInBytes, recordCount);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   * @param fullCopy whether to copy all fields or to drop column-level stats
   */
  private GenericDataFile(GenericDataFile toCopy, boolean fullCopy) {
    super(toCopy, fullCopy);
  }

  /**
   * Constructor for Java serialization.
   */
  GenericDataFile() {
  }

  @Override
  public DataFile copyWithoutStats() {
    return new GenericDataFile(this, false /* drop stats */);
  }

  @Override
  public DataFile copy() {
    return new GenericDataFile(this, true /* full copy */);
  }

  @Override
  protected Schema getAvroSchema(Types.StructType partitionStruct) {
    Types.StructType type = DataFile.getType(partitionStruct);
    return AvroSchemaUtil.convert(type, ImmutableMap.of(
        type, GenericDataFile.class.getName(),
        partitionStruct, PartitionData.class.getName()));
  }
}
