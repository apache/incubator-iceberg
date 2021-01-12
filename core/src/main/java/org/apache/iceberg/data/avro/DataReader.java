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

package org.apache.iceberg.data.avro;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroSchemaWithTypeVisitor;
import org.apache.iceberg.avro.SupportsRowPosition;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class DataReader<T> implements DatumReader<T>, SupportsRowPosition {

  public static <D> DataReader<D> create(org.apache.iceberg.Schema expectedSchema, Schema readSchema) {
    return create(expectedSchema, readSchema, ImmutableMap.of());
  }

  public static <D> DataReader<D> create(org.apache.iceberg.Schema expectedSchema, Schema readSchema,
                                         Map<Integer, ?> idToConstant) {
    return new DataReader<>(expectedSchema, readSchema, idToConstant);
  }

  private final Schema readSchema;
  private final ValueReader<T> reader;
  private Schema fileSchema = null;

  @SuppressWarnings("unchecked")
  protected DataReader(org.apache.iceberg.Schema expectedSchema, Schema readSchema, Map<Integer, ?> idToConstant) {
    this.readSchema = readSchema;
    this.reader = (ValueReader<T>) AvroSchemaWithTypeVisitor
        .visit(expectedSchema, readSchema, new ReadBuilder(idToConstant));
  }

  @Override
  public void setSchema(Schema newFileSchema) {
    this.fileSchema = Schema.applyAliases(newFileSchema, readSchema);
  }

  @Override
  public T read(T reuse, Decoder decoder) throws IOException {
    return DecoderResolver.resolveAndRead(decoder, readSchema, fileSchema, reader, reuse);
  }

  @Override
  public void setRowPositionSupplier(Supplier<Long> posSupplier) {
    if (reader instanceof SupportsRowPosition) {
      ((SupportsRowPosition) reader).setRowPositionSupplier(posSupplier);
    }
  }

  protected ValueReader<?> createStructReader(Types.StructType struct,
                                              List<ValueReader<?>> fields, Map<Integer, ?> idToConstant) {
    return GenericReaders.struct(struct, fields, idToConstant);
  }

  protected <R extends Record> ValueReader<R> createStructReader(Schema schema, Class<R> recordClass,
                                                                 List<ValueReader<?>> fields,
                                                                 Map<Integer, ?> idToConstant) {
    return GenericReaders.struct(schema, recordClass, fields, idToConstant);
  }


  private class ReadBuilder extends AvroSchemaWithTypeVisitor<ValueReader<?>> {
    private final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValueReader<?> record(Types.StructType struct, Schema record,
                                 List<String> names, List<ValueReader<?>> fields) {
      try {
        Class<?> recordClass = DynClasses.builder()
            .loader(loader)
            .impl(record.getFullName())
            .buildChecked();
        if (Record.class.isAssignableFrom(recordClass)) {
          return createStructReader(record, (Class<? extends Record>) recordClass, fields, idToConstant);
        }
        return createStructReader(struct, fields, idToConstant);
      } catch (ClassNotFoundException e) {
        return createStructReader(struct, fields, idToConstant);
      }
    }

    @Override
    public ValueReader<?> union(Type ignored, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Types.ListType ignored, Schema array, ValueReader<?> elementReader) {
      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> map(Types.MapType iMap, Schema map, ValueReader<?> keyReader, ValueReader<?> valueReader) {
      return ValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> map(Types.MapType ignored, Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Type.PrimitiveType ignored, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            return GenericReaders.dates();

          case "time-micros":
            return GenericReaders.times();

          case "timestamp-micros":
            if (AvroSchemaUtil.isTimestamptz(primitive)) {
              return GenericReaders.timestamptz();
            }
            return GenericReaders.timestamps();

          case "decimal":
            return ValueReaders.decimal(
                ValueReaders.decimalBytesReader(primitive),
                ((LogicalTypes.Decimal) logicalType).getScale());

          case "uuid":
            return ValueReaders.uuids();

          default:
            throw new IllegalArgumentException("Unknown logical type: " + logicalType);
        }
      }

      switch (primitive.getType()) {
        case NULL:
          return ValueReaders.nulls();
        case BOOLEAN:
          return ValueReaders.booleans();
        case INT:
          return ValueReaders.ints();
        case LONG:
          return ValueReaders.longs();
        case FLOAT:
          return ValueReaders.floats();
        case DOUBLE:
          return ValueReaders.doubles();
        case STRING:
          // might want to use a binary-backed container like Utf8
          return ValueReaders.strings();
        case FIXED:
          return ValueReaders.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueReaders.byteBuffers();
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
