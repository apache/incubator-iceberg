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
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;

public class GenericPartitionStatsEntry implements PartitionStatsEntry, StructLike, IndexedRecord,
    SpecificData.SchemaConstructable, Serializable {

  enum PartitionStatsCol {
    PARTITION_DATA(0),
    FILE_COUNT(1),
    ROW_COUNT(2);

    private final int id;

    PartitionStatsCol(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

  private final org.apache.avro.Schema schema;
  private PartitionData partitionData = null;
  private int fileCount;
  private long rowCount;

  GenericPartitionStatsEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  public GenericPartitionStatsEntry(PartitionData partitionData, int fileCount, long rowCount) {
    this.schema = AvroSchemaUtil.convert(PartitionStatsEntry.getSchema(partitionData.getPartitionType()),
        "partitionStatsEntry");
    this.partitionData = partitionData;
    this.fileCount = fileCount;
    this.rowCount = rowCount;
  }

  @Override
  public void put(int i, Object v) {
    switch (PartitionStatsCol.values()[i]) {
      case PARTITION_DATA:
        this.partitionData = (PartitionData) v;
        return;
      case FILE_COUNT:
        this.fileCount = (Integer) v;
        return;
      case ROW_COUNT:
        this.rowCount = (Long) v;
        return;
      default:
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return partitionData;
      case 1:
        return rowCount;
      case 2:
        return fileCount;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public StructLike getPartition() {
    return partitionData;
  }

  @Override
  public int getFileCount() {
    return fileCount;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public int size() {
    return 3;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public String toString() {
    return "GenericPartitionStatsEntry{" +
        "partitionData=" + partitionData +
        ", fileCount=" + fileCount +
        ", rowCount=" + rowCount +
        '}';
  }


}
