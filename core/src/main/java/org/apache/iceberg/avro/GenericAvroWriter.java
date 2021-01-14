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

package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class GenericAvroWriter<T> implements MetricsAwareDatumWriter<T> {
  private ValueWriter<T> writer = null;

  GenericAvroWriter(Schema schema) {
    setSchema(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<T>) AvroSchemaVisitor.visit(schema, new WriteBuilder());
  }

  @Override
  public void write(T datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  @Override
  public Stream<FieldMetrics> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends AvroSchemaVisitor<ValueWriter<?>> {
    private WriteBuilder() {
    }

    @Override
    public ValueWriter<?> record(Schema record, List<String> names, List<ValueWriter<?>> fields) {
      return ValueWriters.record(fields);
    }

    @Override
    public ValueWriter<?> union(Schema union, List<ValueWriter<?>> options) {
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
    public ValueWriter<?> array(Schema array, ValueWriter<?> elementWriter) {
      if (array.getLogicalType() instanceof LogicalMap) {
        ValueWriters.StructWriter<?> keyValueWriter = (ValueWriters.StructWriter<?>) elementWriter;
        return ValueWriters.arrayMap(keyValueWriter.writer(0), keyValueWriter.writer(1));
      }

      return ValueWriters.array(elementWriter);
    }

    @Override
    public ValueWriter<?> map(Schema map, ValueWriter<?> valueWriter) {
      int keyId = AvroSchemaUtil.getKeyId(map);
      return ValueWriters.map(ValueWriters.strings(keyId), valueWriter);
    }

    @Override
    public ValueWriter<?> primitive(Schema primitive) {
      int fieldId = AvroSchemaUtil.fieldId(primitive, parentSchema(), this::lastFieldName);

      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            return ValueWriters.ints(fieldId);

          case "time-micros":
            return ValueWriters.longs(fieldId);

          case "timestamp-micros":
            return ValueWriters.longs(fieldId);

          case "decimal":
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return ValueWriters.decimal(fieldId, decimal.getPrecision(), decimal.getScale());

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
          return ValueWriters.ints(fieldId);
        case LONG:
          return ValueWriters.longs(fieldId);
        case FLOAT:
          return ValueWriters.floats(fieldId);
        case DOUBLE:
          return ValueWriters.doubles(fieldId);
        case STRING:
          return ValueWriters.strings(fieldId);
        case FIXED:
          return ValueWriters.genericFixed(fieldId, primitive.getFixedSize());
        case BYTES:
          return ValueWriters.byteBuffers(fieldId);
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
