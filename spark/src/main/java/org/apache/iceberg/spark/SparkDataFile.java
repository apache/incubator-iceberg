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

package org.apache.iceberg.spark;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class SparkDataFile implements DataFile, Serializable {

  private final int filePathPosition;
  private final int fileFormatPosition;
  private final int partitionPosition;
  private final int recordCountPosition;
  private final int fileSizeInBytesPosition;
  private final int columnSizesPosition;
  private final int valueCountsPosition;
  private final int nullValueCountsPosition;
  private final int lowerBoundsPosition;
  private final int upperBoundsPosition;
  private final int keyMetadataPosition;
  private final int splitOffsetsPosition;
  private final int specIdPosition;
  private final Type lowerBoundsType;
  private final Type upperBoundsType;
  private final Type keyMetadataType;

  private final SparkStructLike wrappedPartition;
  private final Types.StructType partitionStruct;
  private Row wrapped;

  private static final StructLike EMPTY_PARTITION_INFO = new StructLike() {
    @Override
    public int size() {
      return 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      throw new UnsupportedOperationException("Cannot get a value from an empty partition");
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot set a value in an empty partition");
    }
  };

  public SparkDataFile(Types.StructType type, StructType sparkType) {
    this.lowerBoundsType = type.fieldType("lower_bounds");
    this.upperBoundsType = type.fieldType("upper_bounds");
    this.keyMetadataType = type.fieldType("key_metadata");
    this.partitionStruct = type.fieldType("partition").asStructType();
    this.wrappedPartition = new SparkStructLike(partitionStruct);

    Map<String, Integer> positions = Maps.newHashMap();
    type.fields().forEach(field -> {
      String fieldName = field.name();
      positions.put(fieldName, fieldPosition(fieldName, sparkType));
    });

    filePathPosition = positions.get("file_path");
    fileFormatPosition = positions.get("file_format");
    partitionPosition = positions.get("partition");
    recordCountPosition = positions.get("record_count");
    fileSizeInBytesPosition = positions.get("file_size_in_bytes");
    columnSizesPosition = positions.get("column_sizes");
    valueCountsPosition = positions.get("value_counts");
    nullValueCountsPosition = positions.get("null_value_counts");
    lowerBoundsPosition = positions.get("lower_bounds");
    upperBoundsPosition = positions.get("upper_bounds");
    keyMetadataPosition = positions.get("key_metadata");
    splitOffsetsPosition = positions.get("split_offsets");
    specIdPosition = positions.get("partition_spec_id");
  }

  private SparkDataFile(SparkDataFile other) {
    this.lowerBoundsType = other.lowerBoundsType;
    this.upperBoundsType = other.upperBoundsType;
    this.keyMetadataType = other.keyMetadataType;
    this.wrappedPartition = new SparkStructLike(other.partitionStruct);
    this.filePathPosition = other.filePathPosition;
    this.fileFormatPosition = other.fileFormatPosition;
    this.partitionPosition = other.partitionPosition;
    this.recordCountPosition = other.recordCountPosition;
    this.fileSizeInBytesPosition = other.fileSizeInBytesPosition;
    this.columnSizesPosition = other.columnSizesPosition;
    this.valueCountsPosition = other.valueCountsPosition;
    this.nullValueCountsPosition = other.nullValueCountsPosition;
    this.lowerBoundsPosition = other.lowerBoundsPosition;
    this.upperBoundsPosition = other.upperBoundsPosition;
    this.keyMetadataPosition = other.keyMetadataPosition;
    this.splitOffsetsPosition = other.splitOffsetsPosition;
    this.specIdPosition = other.specIdPosition;
    this.partitionStruct = other.partitionStruct;
    this.wrap(other.wrapped.copy());
  }

  public SparkDataFile wrap(Row row) {
    this.wrapped = row;
    if (wrappedPartition.size() > 0) {
      Row partition = row.getAs(partitionPosition);
      this.wrappedPartition.wrap(partition);
    }
    return this;
  }

  @Override
  public int specId() {
    if (wrappedPartition.size() > 0) {
      return wrapped.getAs(specIdPosition);
    } else {
      return 0;
    }
  }

  @Override
  public CharSequence path() {
    return wrapped.getAs(filePathPosition);
  }

  @Override
  public FileFormat format() {
    String formatAsString = wrapped.getString(fileFormatPosition).toUpperCase(Locale.ROOT);
    return FileFormat.valueOf(formatAsString);
  }

  @Override
  public StructLike partition() {
    if (wrappedPartition.size() > 0) {
      return wrappedPartition;
    } else {
      return EMPTY_PARTITION_INFO;
    }
  }

  @Override
  public long recordCount() {
    return wrapped.getAs(recordCountPosition);
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.getAs(fileSizeInBytesPosition);
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.isNullAt(columnSizesPosition) ? null : wrapped.getJavaMap(columnSizesPosition);
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.isNullAt(valueCountsPosition) ? null : wrapped.getJavaMap(valueCountsPosition);
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return wrapped.isNullAt(nullValueCountsPosition) ? null : wrapped.getJavaMap(nullValueCountsPosition);
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    Map<?, ?> lowerBounds = wrapped.isNullAt(lowerBoundsPosition) ? null : wrapped.getJavaMap(lowerBoundsPosition);
    return convert(lowerBoundsType, lowerBounds);
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    Map<?, ?> upperBounds = wrapped.isNullAt(upperBoundsPosition) ? null : wrapped.getJavaMap(upperBoundsPosition);
    return convert(upperBoundsType, upperBounds);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return convert(keyMetadataType, wrapped.get(keyMetadataPosition));
  }

  @Override
  public DataFile copy() {
    return new SparkDataFile(this);
  }

  @Override
  public DataFile copyWithoutStats() {
    throw new UnsupportedOperationException("Not implemented: copyWithoutStats");
  }

  @Override
  public List<Long> splitOffsets() {
    return wrapped.isNullAt(splitOffsetsPosition) ? null : wrapped.getList(splitOffsetsPosition);
  }

  private int fieldPosition(String name, StructType sparkType) {
    try {
      return sparkType.fieldIndex(name);
    } catch (IllegalArgumentException e) {
      // the partition field is absent for unpartitioned tables
      if (name.equals("partition") && wrappedPartition.size() == 0) {
        return -1;
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T convert(Type valueType, Object value) {
    return (T) SparkValueConverter.convert(valueType, value);
  }
}
