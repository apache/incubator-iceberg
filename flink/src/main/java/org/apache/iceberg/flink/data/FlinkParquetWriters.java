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

package org.apache.iceberg.flink.data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkParquetWriters {
  private FlinkParquetWriters() {
  }

  @SuppressWarnings("unchecked")
  public static <T> ParquetValueWriter<T> buildWriter(LogicalType schema, MessageType type) {
    return (ParquetValueWriter<T>) ParquetWithFlinkSchemaVisitor.visit(schema, type, new WriteBuilder(type));
  }

  private static class WriteBuilder extends ParquetWithFlinkSchemaVisitor<ParquetValueWriter<?>> {
    private final MessageType type;

    WriteBuilder(MessageType type) {
      this.type = type;
    }

    @Override
    public ParquetValueWriter<?> message(LogicalType sStruct, MessageType message, List<ParquetValueWriter<?>> fields) {
      return struct(sStruct, message.asGroupType(), fields);
    }

    @Override
    public ParquetValueWriter<?> struct(LogicalType fStruct, GroupType struct,
                                        List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<LogicalType> flinkFields = fStruct.getChildren();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      List<LogicalType> flinkTypes = Lists.newArrayList();
      for (int i = 0; i < fields.size(); i += 1) {
        writers.add(newOption(struct.getType(i), fieldWriters.get(i)));
        flinkTypes.add(flinkFields.get(i));
      }

      return new RowDataWriter(writers, flinkTypes);
    }

    @Override
    public ParquetValueWriter<?> list(LogicalType fArray, GroupType array, ParquetValueWriter<?> elementWriter) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      return new ArrayDataWriter<>(repeatedD, repeatedR,
          newOption(repeated.getType(0), elementWriter),
          arrayElementType(fArray));
    }

    @Override
    public ParquetValueWriter<?> map(LogicalType fMap, GroupType map,
                                     ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      return new MapDataWriter<>(repeatedD, repeatedR,
          newOption(repeatedKeyValue.getType(0), keyWriter),
          newOption(repeatedKeyValue.getType(1), valueWriter),
          mapKeyType(fMap), mapValueType(fMap));
    }

    private ParquetValueWriter<?> newOption(org.apache.parquet.schema.Type fieldType, ParquetValueWriter<?> writer) {
      int maxD = type.getMaxDefinitionLevel(path(fieldType.getName()));
      return ParquetValueWriters.option(fieldType, maxD, writer);
    }

    @Override
    public ParquetValueWriter<?> primitive(LogicalType fType, PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return strings(desc);
          case DATE:
          case INT_8:
          case INT_16:
          case INT_32:
            return ints(fType, desc);
          case INT_64:
            return ParquetValueWriters.longs(desc);
          case TIME_MICROS:
            return timeMicros(desc);
          case TIMESTAMP_MICROS:
            return timestamps(desc);
          case DECIMAL:
            DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation();
            switch (primitive.getPrimitiveTypeName()) {
              case INT32:
                return decimalAsInteger(desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return decimalAsLong(desc, decimal.getPrecision(), decimal.getScale());
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return decimalAsFixed(desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return byteArrays(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return byteArrays(desc);
        case BOOLEAN:
          return ParquetValueWriters.booleans(desc);
        case INT32:
          return ints(fType, desc);
        case INT64:
          return ParquetValueWriters.longs(desc);
        case FLOAT:
          return ParquetValueWriters.floats(desc);
        case DOUBLE:
          return ParquetValueWriters.doubles(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private static ParquetValueWriters.PrimitiveWriter<?> ints(LogicalType type, ColumnDescriptor desc) {
    if (type instanceof TinyIntType) {
      return ParquetValueWriters.tinyints(desc);
    } else if (type instanceof SmallIntType) {
      return ParquetValueWriters.shorts(desc);
    }
    return ParquetValueWriters.ints(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<StringData> strings(ColumnDescriptor desc) {
    return new StringDataWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<Integer> timeMicros(ColumnDescriptor desc) {
    return new TimeMicrosWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<DecimalData> decimalAsInteger(ColumnDescriptor desc,
                                                                                   int precision, int scale) {
    Preconditions.checkArgument(precision <= 9, "Cannot write decimal value as integer with precision larger than 9," +
        " wrong precision %s", precision);
    return new IntegerDecimalWriter(desc, precision, scale);
  }

  private static ParquetValueWriters.PrimitiveWriter<DecimalData> decimalAsLong(ColumnDescriptor desc,
                                                                                int precision, int scale) {
    Preconditions.checkArgument(precision <= 18, "Cannot write decimal value as long with precision larger than 18, " +
        " wrong precision %s", precision);
    return new LongDecimalWriter(desc, precision, scale);
  }

  private static ParquetValueWriters.PrimitiveWriter<DecimalData> decimalAsFixed(ColumnDescriptor desc,
                                                                                 int precision, int scale) {
    return new FixedDecimalWriter(desc, precision, scale);
  }

  private static ParquetValueWriters.PrimitiveWriter<TimestampData> timestamps(ColumnDescriptor desc) {
    return new TimestampDataWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<byte[]> byteArrays(ColumnDescriptor desc) {
    return new ByteArrayWriter(desc);
  }

  private static class StringDataWriter extends ParquetValueWriters.PrimitiveWriter<StringData> {
    private StringDataWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, StringData value) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(value.toBytes()));
    }
  }

  private static class TimeMicrosWriter extends ParquetValueWriters.PrimitiveWriter<Integer> {
    private TimeMicrosWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Integer value) {
      long micros = value.longValue() * 1000;
      column.writeLong(repetitionLevel, micros);
    }
  }

  private static class IntegerDecimalWriter extends ParquetValueWriters.PrimitiveWriter<DecimalData> {
    private final int precision;
    private final int scale;

    private IntegerDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, DecimalData decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeInteger(repetitionLevel, (int) decimal.toUnscaledLong());
    }
  }

  private static class LongDecimalWriter extends ParquetValueWriters.PrimitiveWriter<DecimalData> {
    private final int precision;
    private final int scale;

    private LongDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, DecimalData decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeLong(repetitionLevel, decimal.toUnscaledLong());
    }
  }

  private static class FixedDecimalWriter extends ParquetValueWriters.PrimitiveWriter<DecimalData> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private FixedDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
      this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    public void write(int repetitionLevel, DecimalData decimal) {
      byte[] binary = DecimalUtil.toReusedFixLengthBytes(precision, scale, decimal.toBigDecimal(), bytes.get());
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(binary));
    }
  }

  private static class TimestampDataWriter extends ParquetValueWriters.PrimitiveWriter<TimestampData> {
    private TimestampDataWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, TimestampData value) {
      column.writeLong(repetitionLevel, value.getMillisecond() * 1000 + value.getNanoOfMillisecond() / 1000);
    }
  }

  private static class ByteArrayWriter extends ParquetValueWriters.PrimitiveWriter<byte[]> {
    private ByteArrayWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, byte[] bytes) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(bytes));
    }
  }

  private static class ArrayDataWriter<E> extends ParquetValueWriters.RepeatedWriter<ArrayData, E> {
    private final LogicalType elementType;

    private ArrayDataWriter(int definitionLevel, int repetitionLevel,
                            ParquetValueWriter<E> writer, LogicalType elementType) {
      super(definitionLevel, repetitionLevel, writer);
      this.elementType = elementType;
    }

    @Override
    protected Iterator<E> elements(ArrayData list) {
      return new ElementIterator<>(list);
    }

    private class ElementIterator<E> implements Iterator<E> {
      private final int size;
      private final ArrayData list;
      private final ArrayData.ElementGetter getter;
      private int index;

      private ElementIterator(ArrayData list) {
        this.list = list;
        size = list.size();
        getter = ArrayData.createElementGetter(elementType);
        index = 0;
      }

      @Override
      public boolean hasNext() {
        return index != size;
      }

      @Override
      @SuppressWarnings("unchecked")
      public E next() {
        if (index >= size) {
          throw new NoSuchElementException();
        }

        E element = (E) getter.getElementOrNull(list, index);
        index += 1;

        return element;
      }
    }
  }

  private static class MapDataWriter<K, V> extends ParquetValueWriters.RepeatedKeyValueWriter<MapData, K, V> {
    private final LogicalType keyType;
    private final LogicalType valueType;

    private MapDataWriter(int definitionLevel, int repetitionLevel,
                          ParquetValueWriter<K> keyWriter, ParquetValueWriter<V> valueWriter,
                          LogicalType keyType, LogicalType valueType) {
      super(definitionLevel, repetitionLevel, keyWriter, valueWriter);
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    protected Iterator<Map.Entry<K, V>> pairs(MapData map) {
      return new EntryIterator<>(map);
    }

    private class EntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
      private final int size;
      private final ArrayData keys;
      private final ArrayData values;
      private final ParquetValueReaders.ReusableEntry<K, V> entry;
      private final ArrayData.ElementGetter keyGetter;
      private final ArrayData.ElementGetter valueGetter;
      private int index;

      private EntryIterator(MapData map) {
        size = map.size();
        keys = map.keyArray();
        values = map.valueArray();
        entry = new ParquetValueReaders.ReusableEntry<>();
        keyGetter = ArrayData.createElementGetter(keyType);
        valueGetter = ArrayData.createElementGetter(valueType);
        index = 0;
      }

      @Override
      public boolean hasNext() {
        return index != size;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Map.Entry<K, V> next() {
        if (index >= size) {
          throw new NoSuchElementException();
        }

        entry.set((K) keyGetter.getElementOrNull(keys, index), (V) valueGetter.getElementOrNull(values, index));
        index += 1;

        return entry;
      }
    }
  }

  private static class RowDataWriter extends ParquetValueWriters.StructWriter<RowData> {
    private final RowData.FieldGetter[] fieldGetter;

    RowDataWriter(List<ParquetValueWriter<?>> writers, List<LogicalType> types) {
      super(writers);
      fieldGetter = new RowData.FieldGetter[types.size()];
      for (int i = 0; i < types.size(); i += 1) {
        fieldGetter[i] = RowData.createFieldGetter(types.get(i), i);
      }
    }

    @Override
    protected Object get(RowData struct, int index) {
      return fieldGetter[index].getFieldOrNull(struct);
    }
  }
}
