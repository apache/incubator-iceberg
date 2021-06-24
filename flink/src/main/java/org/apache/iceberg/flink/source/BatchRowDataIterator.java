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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.data.vectorized.VectorizedFlinkOrcReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.jetbrains.annotations.NotNull;

class BatchRowDataIterator extends DataIterator<RowData> {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final DataType[] dataTypes;

  BatchRowDataIterator(CombinedScanTask task, FileIO io, EncryptionManager encryption, Schema tableSchema,
                       Schema projectedSchema, String nameMapping, boolean caseSensitive, DataType[] dataTypes) {
    super(task, io, encryption);
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.dataTypes = dataTypes;
    if (!useOrcVectorizedRead(task)) {
      throw new UnsupportedOperationException("Unsupport vectorized read");
    }
  }

  @Override
  CloseableIterator<RowData> openTaskIterator(FileScanTask task) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
        PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    CloseableIterable<RowData> iter;
    switch (task.file().format()) {
      case ORC:
        iter = newOrcIterable(task, tableSchema, idToConstant);
        break;

      case PARQUET:
      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + task.file().format());
    }

    return iter.iterator();
  }

  private CloseableIterable<RowData> newOrcIterable(FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(schema,
        Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder = ORC
        .read(getInputFile(task))
        .split(task.start(), task.length())
        .project(readSchemaWithoutConstantAndMetadataFields)
        .createBatchedReaderFunc(readOrcSchema ->
            VectorizedFlinkOrcReaders.buildReader(schema, readOrcSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    CloseableIterable<VectorizedColumnBatch> iterable = builder.build();

    return new BatchRowIterator(iterable);
  }


  private static class BatchRowIterator implements CloseableIterable<RowData> {
    private final CloseableIterator<VectorizedColumnBatch> iterator;
    private int rowNums = 0;
    private int rowId = 0;
    private ColumnarRowData row;

    BatchRowIterator(CloseableIterable<VectorizedColumnBatch> iterable) {
      this.iterator = iterable.iterator();
    }

    @Override
    public void close() throws IOException {
      iterator.close();
    }

    @NotNull
    @Override
    public CloseableIterator<RowData> iterator() {
      return new CloseableIterator<RowData>() {

        @Override
        public boolean hasNext() {
          if (iterator.hasNext() && rowId >= rowNums) {
            if (row == null) {
              VectorizedColumnBatch vectorizedColumnBatch = iterator.next();
              row = new ColumnarRowData(vectorizedColumnBatch);
              rowNums = vectorizedColumnBatch.getNumRows();
            } else if (rowId > rowNums) {
              row = null;
              rowNums = 0;
              rowId = 0;
            }
          }

          return iterator.hasNext() || rowId < rowNums;
        }

        @Override
        public RowData next() {
          row.setRowId(rowId++);
          return row;
        }

        @Override
        public void close() throws IOException {
          iterator.close();
        }
      };
    }
  }

  private boolean useOrcVectorizedRead(CombinedScanTask task) {
    Collection<FileScanTask> fileScanTasks = task.files();
    for (FileScanTask fileScanTask : fileScanTasks) {
      DataFile dataFile = fileScanTask.file();
      if (!FileContent.DATA.equals(dataFile.content())) {
        return false;
      }

      if (!FileFormat.ORC.equals(dataFile.format())) {
        return false;
      }
    }

    for (DataType dataType : dataTypes) {
      if (!isVectorizationSupported(dataType.getLogicalType())) {
        return false;
      }
    }

    return true;
  }

  private static boolean isVectorizationSupported(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
      case BOOLEAN:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case ROW:
      case ARRAY:
        return true;
      case TIMESTAMP_WITH_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
      case MULTISET:
      case MAP:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case NULL:
      case RAW:
      case SYMBOL:
      default:
        return false;
    }
  }
}
