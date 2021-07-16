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

import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

class RowDataIterator extends DataIterator<RowData> {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;

  RowDataIterator(CombinedScanTask task, FileIO io, EncryptionManager encryption, Schema tableSchema,
                  Schema projectedSchema, String nameMapping, boolean caseSensitive) {
    super(task, io, encryption);
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
  }

  @Override
  protected CloseableIterator<RowData> openTaskIterator(FileScanTask task) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
        PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes = new FlinkDeleteFilter(task, tableSchema, projectedSchema);
    CloseableIterable<RowData> iterable = deletes.filter(newIterable(task, deletes.requiredSchema(), idToConstant));

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection = RowDataProjection.create(deletes.requiredSchema(), projectedSchema);
      iterable = CloseableIterable.transform(iterable, rowDataProjection::project);
    }

    return iterable.iterator();
  }

  private CloseableIterable<RowData> newIterable(FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    CloseableIterable<RowData> iter;
    if (task.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    } else {
      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(task, schema, idToConstant);
          break;

        case AVRO:
          iter = newAvroIterable(task, schema, idToConstant);
          break;

        case ORC:
          iter = newOrcIterable(task, schema, idToConstant);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    return iter;
  }

  private CloseableIterable<RowData> newAvroIterable(FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    Avro.ReadBuilder builder = Avro.read(getInputFile(task))
        .reuseContainers()
        .project(schema)
        .split(task.start(), task.length())
        .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newParquetIterable(FileScanTask task, Schema schema,
                                                        Map<Integer, ?> idToConstant) {
    Parquet.ReadBuilder builder = Parquet.read(getInputFile(task))
        .reuseContainers()
        .split(task.start(), task.length())
        .project(schema)
        .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive)
        .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newOrcIterable(FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(schema,
        Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder = ORC.read(getInputFile(task))
        .project(readSchemaWithoutConstantAndMetadataFields)
        .split(task.start(), task.length())
        .createReaderFunc(readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private class FlinkDeleteFilter extends DeleteFilter<RowData> {
    private final RowDataWrapper asStructLike;

    FlinkDeleteFilter(FileScanTask task, Schema tableSchema, Schema requestedSchema) {
      super(task, tableSchema, requestedSchema);
      this.asStructLike = new RowDataWrapper(FlinkSchemaUtil.convert(requiredSchema()), requiredSchema().asStruct());
    }

    @Override
    protected StructLike asStructLike(RowData row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return RowDataIterator.this.getInputFile(location);
    }
  }
}
