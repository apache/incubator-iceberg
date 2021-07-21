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

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

/**
 * Configuration for Parquet readers.
 *
 * @param <T> type of value to read
 */
class ReadConf<T> {
  private final ParquetFileReader reader;
  private final InputFile file;
  private final ParquetReadOptions options;
  private final MessageType projection;
  private final ParquetValueReader<T> model;
  private final VectorizedReader<T> vectorizedModel;
  private final List<BlockMetaData> rowGroups;
  private final boolean[] shouldSkip;
  private final long totalValues;
  private final boolean reuseContainers;
  private final Integer batchSize;
  private final long[] startRowPositions;

  // List of column chunk metadata for each row group
  private final List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetaDataForRowGroups;

  @SuppressWarnings("unchecked")
  ReadConf(InputFile file, ParquetReadOptions options, Schema expectedSchema, Expression filter,
           Function<MessageType, ParquetValueReader<?>> readerFunc, Function<MessageType,
           VectorizedReader<?>> batchedReaderFunc, NameMapping nameMapping, boolean reuseContainers,
           boolean caseSensitive, Integer bSize) {
    this.file = file;
    this.options = options;
    this.reader = newReader(file, options);
    MessageType fileSchema = reader.getFileMetaData().getSchema();

    MessageType typeWithIds;
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      typeWithIds = fileSchema;
      this.projection = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
    } else if (nameMapping != null) {
      typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
      this.projection = ParquetSchemaUtil.pruneColumns(typeWithIds, expectedSchema);
    } else {
      typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
      this.projection = ParquetSchemaUtil.pruneColumnsFallback(fileSchema, expectedSchema);
    }

    this.rowGroups = reader.getRowGroups();
    this.shouldSkip = new boolean[rowGroups.size()];
    this.startRowPositions = new long[rowGroups.size()];

    // Fetch all row groups starting positions to compute the row offsets of the filtered row groups
    Map<Long, Long> offsetToStartPos = generateOffsetToStartPos(expectedSchema);

    ParquetMetricsRowGroupFilter statsFilter = null;
    ParquetDictionaryRowGroupFilter dictFilter = null;
    if (filter != null) {
      statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
      dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
    }

    long computedTotalValues = 0L;
    for (int i = 0; i < shouldSkip.length; i += 1) {
      BlockMetaData rowGroup = rowGroups.get(i);
      startRowPositions[i] = offsetToStartPos == null ? 0 : offsetToStartPos.get(rowGroup.getStartingPos());
      boolean shouldRead = filter == null || statsFilter.shouldRead(typeWithIds, rowGroup) &&
          dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup));
      this.shouldSkip[i] = !shouldRead;
      if (shouldRead) {
        computedTotalValues += rowGroup.getRowCount();
      }
    }

    this.totalValues = computedTotalValues;
    if (readerFunc != null) {
      this.model = (ParquetValueReader<T>) readerFunc.apply(typeWithIds);
      this.vectorizedModel = null;
      this.columnChunkMetaDataForRowGroups = null;
    } else {
      this.model = null;
      this.vectorizedModel = (VectorizedReader<T>) batchedReaderFunc.apply(typeWithIds);
      this.columnChunkMetaDataForRowGroups = getColumnChunkMetadataForRowGroups();
    }

    this.reuseContainers = reuseContainers;
    this.batchSize = bSize;
  }

  private ReadConf(ReadConf<T> toCopy) {
    this.reader = null;
    this.file = toCopy.file;
    this.options = toCopy.options;
    this.projection = toCopy.projection;
    this.model = toCopy.model;
    this.rowGroups = toCopy.rowGroups;
    this.shouldSkip = toCopy.shouldSkip;
    this.totalValues = toCopy.totalValues;
    this.reuseContainers = toCopy.reuseContainers;
    this.batchSize = toCopy.batchSize;
    this.vectorizedModel = toCopy.vectorizedModel;
    this.columnChunkMetaDataForRowGroups = toCopy.columnChunkMetaDataForRowGroups;
    this.startRowPositions = toCopy.startRowPositions;
  }

  ParquetFileReader reader() {
    if (reader != null) {
      reader.setRequestedSchema(projection);
      return reader;
    }

    ParquetFileReader newReader = newReader(file, options);
    newReader.setRequestedSchema(projection);
    return newReader;
  }

  ParquetValueReader<T> model() {
    return model;
  }

  VectorizedReader<T> vectorizedModel() {
    return vectorizedModel;
  }

  boolean[] shouldSkip() {
    return shouldSkip;
  }

  private Map<Long, Long> generateOffsetToStartPos(Schema schema) {
    if (schema.findField(MetadataColumns.ROW_POSITION.fieldId()) == null) {
      return null;
    }

    try (ParquetFileReader fileReader = newReader(file, ParquetReadOptions.builder().build())) {
      Map<Long, Long> offsetToStartPos = new HashMap<>();

      long curRowCount = 0;
      for (int i = 0; i < fileReader.getRowGroups().size(); i += 1) {
        BlockMetaData meta = fileReader.getRowGroups().get(i);
        offsetToStartPos.put(meta.getStartingPos(), curRowCount);
        curRowCount += meta.getRowCount();
      }

      return offsetToStartPos;

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create/close reader for file: " + file, e);
    }
  }

  long[] startRowPositions() {
    return startRowPositions;
  }

  long totalValues() {
    return totalValues;
  }

  boolean reuseContainers() {
    return reuseContainers;
  }

  Integer batchSize() {
    return batchSize;
  }

  List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetadataForRowGroups() {
    return columnChunkMetaDataForRowGroups;
  }

  ReadConf<T> copy() {
    return new ReadConf<>(this);
  }

  private static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
    try {
      return ParquetFileReader.open(ParquetIO.file(file), options);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to open Parquet file: %s", file.location());
    }
  }

  private List<Map<ColumnPath, ColumnChunkMetaData>> getColumnChunkMetadataForRowGroups() {
    Set<ColumnPath> projectedColumns = projection.getColumns().stream()
        .map(columnDescriptor -> ColumnPath.get(columnDescriptor.getPath())).collect(Collectors.toSet());
    ImmutableList.Builder<Map<ColumnPath, ColumnChunkMetaData>> listBuilder = ImmutableList.builder();
    for (int i = 0; i < rowGroups.size(); i++) {
      if (!shouldSkip[i]) {
        BlockMetaData blockMetaData = rowGroups.get(i);
        ImmutableMap.Builder<ColumnPath, ColumnChunkMetaData> mapBuilder = ImmutableMap.builder();
        blockMetaData.getColumns().stream()
            .filter(columnChunkMetaData -> projectedColumns.contains(columnChunkMetaData.getPath()))
            .forEach(columnChunkMetaData -> mapBuilder.put(columnChunkMetaData.getPath(), columnChunkMetaData));
        listBuilder.add(mapBuilder.build());
      } else {
        listBuilder.add(ImmutableMap.of());
      }
    }
    return listBuilder.build();
  }
}
