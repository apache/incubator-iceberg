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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

class RowDataReader extends BaseDataReader<InternalRow> {
  // for some reason, the apply method can't be called from Java without reflection
  private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
      .impl(UnsafeProjection.class, InternalRow.class)
      .build();

  private final Schema tableSchema;
  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;

  RowDataReader(CombinedScanTask task, Table table, Schema expectedSchema, boolean caseSensitive) {
    super(task, table.io(), table.encryption());
    this.tableSchema = table.schema();
    this.expectedSchema = expectedSchema;
    this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.caseSensitive = caseSensitive;
  }

  @Override
  CloseableIterator<InternalRow> open(FileScanTask task) {
    SparkDeleteFilter deletes = new SparkDeleteFilter(task, tableSchema, expectedSchema);

    // schema or rows returned by readers
    Schema requiredSchema = deletes.requiredSchema();
    Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, RowDataReader::convertConstant);
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    return deletes.filter(open(task, requiredSchema, idToConstant)).iterator();
  }

  protected Schema tableSchema() {
    return tableSchema;
  }

  protected CloseableIterable<InternalRow> open(FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant) {
    CloseableIterable<InternalRow> iter;
    if (task.isDataTask()) {
      iter = newDataIterable(task.asDataTask(), readSchema);
    } else {
      InputFile location = getInputFile(task);
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(location, task, readSchema, idToConstant);
          break;

        case AVRO:
          iter = newAvroIterable(location, task, readSchema, idToConstant);
          break;

        case ORC:
          iter = newOrcIterable(location, task, readSchema, idToConstant);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    return iter;
  }

  private CloseableIterable<InternalRow> newAvroIterable(
      InputFile location,
      FileScanTask task,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    Avro.ReadBuilder builder = Avro.read(location)
        .reuseContainers()
        .project(projection)
        .split(task.start(), task.length())
        .createReaderFunc(readSchema -> new SparkAvroReader(projection, readSchema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<InternalRow> newParquetIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    Parquet.ReadBuilder builder = Parquet.read(location)
        .reuseContainers()
        .split(task.start(), task.length())
        .project(readSchema)
        .createReaderFunc(fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<InternalRow> newOrcIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(readSchema,
        Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder = ORC.read(location)
        .project(readSchemaWithoutConstantAndMetadataFields)
        .split(task.start(), task.length())
        .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<InternalRow> newDataIterable(DataTask task, Schema readSchema) {
    StructInternalRow row = new StructInternalRow(tableSchema.asStruct());
    CloseableIterable<InternalRow> asSparkRows = CloseableIterable.transform(
        task.asDataTask().rows(), row::setStruct);
    return CloseableIterable.transform(
        asSparkRows, APPLY_PROJECTION.bind(projection(readSchema, tableSchema))::invoke);
  }

  private static UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
    StructType struct = SparkSchemaUtil.convert(readSchema);

    List<AttributeReference> refs = JavaConverters.seqAsJavaListConverter(struct.toAttributes()).asJava();
    List<Attribute> attrs = Lists.newArrayListWithExpectedSize(struct.fields().length);
    List<org.apache.spark.sql.catalyst.expressions.Expression> exprs =
        Lists.newArrayListWithExpectedSize(struct.fields().length);

    for (AttributeReference ref : refs) {
      attrs.add(ref.toAttribute());
    }

    for (Types.NestedField field : finalSchema.columns()) {
      int indexInReadSchema = struct.fieldIndex(field.name());
      exprs.add(refs.get(indexInReadSchema));
    }

    return UnsafeProjection.create(
        JavaConverters.asScalaBufferConverter(exprs).asScala().toSeq(),
        JavaConverters.asScalaBufferConverter(attrs).asScala().toSeq());
  }

  protected class SparkDeleteFilter extends DeleteFilter<InternalRow> {
    private final InternalRowWrapper asStructLike;

    SparkDeleteFilter(FileScanTask task, Schema tableSchema, Schema requestedSchema) {
      super(task, tableSchema, requestedSchema);
      this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    }

    @Override
    protected Consumer<InternalRow> deleteMarker() {
      return record -> record.setBoolean(deleteMarkerIndex(), true);
    }

    @Override
    protected boolean isDeletedRow(InternalRow row) {
      return row.getBoolean(deleteMarkerIndex());
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return RowDataReader.this.getInputFile(location);
    }
  }
}
