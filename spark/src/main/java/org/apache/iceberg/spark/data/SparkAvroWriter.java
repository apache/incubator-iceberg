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

package org.apache.iceberg.spark.data;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroWriterBuilderFieldIdUtil;
import org.apache.iceberg.avro.MetricsAwareDatumWriter;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructType;

public class SparkAvroWriter implements MetricsAwareDatumWriter<InternalRow> {
  private final StructType dsSchema;
  private ValueWriter<InternalRow> writer = null;

  public SparkAvroWriter(StructType dsSchema) {
    this.dsSchema = dsSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<InternalRow>) AvroWithSparkSchemaVisitor
        .visit(dsSchema, schema, new WriteBuilder());
  }

  @Override
  public void write(InternalRow datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  @Override
  public Stream<FieldMetrics> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends AvroWithSparkSchemaVisitor<ValueWriter<?>> {
    private final Deque<Integer> fieldIds = Lists.newLinkedList();

    @Override
    public ValueWriter<?> record(DataType struct, Schema record, List<String> names, List<ValueWriter<?>> fields) {
      return SparkValueWriters.struct(fields, IntStream.range(0, names.size())
          .mapToObj(i -> fieldNameAndType(struct, i).second()).collect(Collectors.toList()));
    }

    @Override
    public ValueWriter<?> union(DataType type, Schema union, List<ValueWriter<?>> options) {
      Preconditions.checkArgument(options.contains(ValueWriters.nulls()),
          "Cannot create writer for non-option union: %s", union);
      Preconditions.checkArgument(options.size() == 2,
          "Cannot create writer for non-option union: %s", union);
      if (union.getTypes().get(0).getType() == Schema.Type.NULL) {
        return ValueWriters.option(0, options.get(1), union.getTypes().get(1).getType());
      } else {
        return ValueWriters.option(1, options.get(0), union.getTypes().get(0).getType());
      }
    }

    @Override
    public ValueWriter<?> array(DataType sArray, Schema array, ValueWriter<?> elementWriter) {
      return SparkValueWriters.array(elementWriter, arrayElementType(sArray));
    }

    @Override
    public ValueWriter<?> map(DataType sMap, Schema map, ValueWriter<?> valueReader) {
      return SparkValueWriters.map(
          SparkValueWriters.strings(AvroSchemaUtil.getKeyId(map)), mapKeyType(sMap),
          valueReader, mapValueType(sMap));
    }

    @Override
    public ValueWriter<?> map(DataType sMap, Schema map, ValueWriter<?> keyWriter, ValueWriter<?> valueWriter) {
      return SparkValueWriters.arrayMap(keyWriter, mapKeyType(sMap), valueWriter, mapValueType(sMap));
    }

    @Override
    public ValueWriter<?> primitive(DataType type, Schema primitive) {
      Preconditions.checkState(!fieldIds.isEmpty() && fieldIds.peek() != null,
          "[BUG] cannot get field id for logical type %s and schema %s", type, primitive);
      int fieldId = fieldIds.peek();

      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            // Spark uses the same representation
            return ValueWriters.ints(fieldId);

          case "timestamp-micros":
            // Spark uses the same representation
            return ValueWriters.longs(fieldId);

          case "decimal":
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return SparkValueWriters.decimal(fieldId, decimal.getPrecision(), decimal.getScale());

          case "uuid":
            return ValueWriters.uuids(fieldId);

          default:
            throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        }
      }

      switch (primitive.getType()) {
        case NULL:
          return ValueWriters.nulls();
        case BOOLEAN:
          return ValueWriters.booleans(fieldId);
        case INT:
          if (type instanceof ByteType) {
            return ValueWriters.tinyints(fieldId);
          } else if (type instanceof ShortType) {
            return ValueWriters.shorts(fieldId);
          }
          return ValueWriters.ints(fieldId);
        case LONG:
          return ValueWriters.longs(fieldId);
        case FLOAT:
          return ValueWriters.floats(fieldId);
        case DOUBLE:
          return ValueWriters.doubles(fieldId);
        case STRING:
          return SparkValueWriters.strings(fieldId);
        case FIXED:
          return ValueWriters.fixed(fieldId, primitive.getFixedSize());
        case BYTES:
          return ValueWriters.bytes(fieldId);
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }

    public void beforeField(String name, Schema type, Schema parentSchema) {
      AvroWriterBuilderFieldIdUtil.beforeField(fieldIds, name, parentSchema);
    }

    public void afterField(String name, Schema type, Schema parentSchema) {
      AvroWriterBuilderFieldIdUtil.afterField(fieldIds);
    }

    public void beforeListElement(String name, Schema type, Schema parentSchema) {
      AvroWriterBuilderFieldIdUtil.beforeListElement(fieldIds, parentSchema);
    }

    public void beforeMapKey(String name, Schema type, Schema parentSchema) {
      AvroWriterBuilderFieldIdUtil.beforeMapKey(fieldIds, name, parentSchema);
    }

    public void beforeMapValue(String name, Schema type, Schema parentSchema) {
      AvroWriterBuilderFieldIdUtil.beforeMapValue(fieldIds, name, parentSchema);
    }
  }
}
