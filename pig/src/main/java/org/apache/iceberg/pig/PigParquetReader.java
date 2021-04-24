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

package org.apache.iceberg.pig;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueReaders.BinaryAsDecimalReader;
import org.apache.iceberg.parquet.ParquetValueReaders.FloatAsDoubleReader;
import org.apache.iceberg.parquet.ParquetValueReaders.IntAsLongReader;
import org.apache.iceberg.parquet.ParquetValueReaders.IntegerAsDecimalReader;
import org.apache.iceberg.parquet.ParquetValueReaders.LongAsDecimalReader;
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import org.apache.iceberg.parquet.ParquetValueReaders.RepeatedKeyValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders.RepeatedReader;
import org.apache.iceberg.parquet.ParquetValueReaders.ReusableEntry;
import org.apache.iceberg.parquet.ParquetValueReaders.StringReader;
import org.apache.iceberg.parquet.ParquetValueReaders.StructReader;
import org.apache.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PigParquetReader {
  private PigParquetReader() {
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Tuple> buildReader(
      MessageType fileSchema, Schema expectedSchema, Map<Integer, Object> partitionValues) {

    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      return (ParquetValueReader<Tuple>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReadBuilder(fileSchema, partitionValues));
    } else {
      return (ParquetValueReader<Tuple>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new FallbackReadBuilder(fileSchema, partitionValues));
    }
  }

  private static class FallbackReadBuilder extends ReadBuilder {
    FallbackReadBuilder(MessageType type, Map<Integer, Object> partitionValues) {
      super(type, partitionValues);
    }

    @Override
    public ParquetValueReader<?> message(
        org.apache.iceberg.types.Type expected, MessageType message, List<ParquetValueReader<?>> fieldReaders) {
      // the top level matches by ID, but the remaining IDs are missing
      return super.struct(expected, message, fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(
        org.apache.iceberg.types.Type ignored, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      // the expected struct is ignored because nested fields are never found when the
      List<ParquetValueReader<?>> newFields = Lists.newArrayListWithExpectedSize(
          fieldReaders.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(fieldReaders.size());
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = getMessageType().getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        newFields.add(ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
        types.add(fieldType);
      }

      return new TupleReader(types, newFields);
    }
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final Map<Integer, Object> partitionValues;

    ReadBuilder(MessageType type, Map<Integer, Object> partitionValues) {
      this.type = type;
      this.partitionValues = partitionValues;
    }

    MessageType getMessageType() {
      return this.type;
    }

    @Override
    public ParquetValueReader<?> message(
        org.apache.iceberg.types.Type expected, MessageType message, List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(
        org.apache.iceberg.types.Type expected, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        int id = fieldType.getId().intValue();
        readersById.put(id, ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
        typesById.put(id, fieldType);
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.asStructType().fields() : ImmutableList.of();
      List<ParquetValueReader<?>> reorderedFields = Lists.newArrayListWithExpectedSize(
          expectedFields.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        if (partitionValues.containsKey(id)) {
          // the value may be null so containsKey is used to check for a partition value
          reorderedFields.add(ParquetValueReaders.constant(partitionValues.get(id)));
          types.add(null);
        } else {
          ParquetValueReader<?> reader = readersById.get(id);
          if (reader != null) {
            reorderedFields.add(reader);
            types.add(typesById.get(id));
          } else {
            reorderedFields.add(ParquetValueReaders.nulls());
            types.add(null);
          }
        }
      }

      return new TupleReader(types, reorderedFields);
    }

    @Override
    public ParquetValueReader<?> list(
        org.apache.iceberg.types.Type expectedList, GroupType array, ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

      return new ArrayReader<>(repeatedD, repeatedR, ParquetValueReaders.option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(
        org.apache.iceberg.types.Type expectedMap, GroupType map, ParquetValueReader<?> keyReader,
        ParquetValueReader<?> valueReader) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

      return new MapReader<>(repeatedD, repeatedR,
          ParquetValueReaders.option(keyType, keyD, keyReader),
          ParquetValueReaders.option(valueType, valueD, valueReader));
    }

    @Override
    public ParquetValueReader<?> primitive(
        org.apache.iceberg.types.Type expected, PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringReader(desc);
          case DATE:
            return new DateReader(desc);
          case INT_8:
          case INT_16:
          case INT_32:
            if (expected != null && expected.typeId() == Types.LongType.get().typeId()) {
              return new IntAsLongReader(desc);
            } else {
              return new UnboxedReader(desc);
            }
          case INT_64: return new UnboxedReader<>(desc);
          case TIMESTAMP_MILLIS: return new TimestampMillisReader(desc);
          case TIMESTAMP_MICROS: return new TimestampMicrosReader(desc);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new BinaryAsDecimalReader(desc, decimal.getScale());
              case INT32:
                return new IntegerAsDecimalReader(desc, decimal.getScale());
              case INT64:
                return new LongAsDecimalReader(desc, decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          default:
            throw new UnsupportedOperationException("Unsupported type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new BytesReader(desc);
        case INT32:
          if (expected != null && expected.typeId() == TypeID.LONG) {
            return new IntAsLongReader(desc);
          } else {
            return new UnboxedReader<>(desc);
          }
        case FLOAT:
          if (expected != null && expected.typeId() == TypeID.DOUBLE) {
            return new FloatAsDoubleReader(desc);
          } else {
            return new UnboxedReader<>(desc);
          }
        case BOOLEAN:
        case INT64:
        case DOUBLE:
          return new UnboxedReader<>(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private static class DateReader extends PrimitiveReader<String> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    DateReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String reuse) {
      OffsetDateTime day = EPOCH.plusDays(column.nextInteger());
      return String.format("%04d-%02d-%02d", day.getYear(), day.getMonth().getValue(), day.getDayOfMonth());
    }
  }

  private static class BytesReader extends PrimitiveReader<DataByteArray> {
    BytesReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public DataByteArray read(DataByteArray reuse) {
      byte[] bytes = column.nextBinary().getBytes();
      return new DataByteArray(bytes);
    }
  }

  private static class TimestampMicrosReader extends UnboxedReader<String> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    TimestampMicrosReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String ignored) {
      return ChronoUnit.MICROS.addTo(EPOCH, column.nextLong()).toString();
    }
  }

  private static class TimestampMillisReader extends UnboxedReader<String> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    TimestampMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String ignored) {
      return ChronoUnit.MILLIS.addTo(EPOCH, column.nextLong()).toString();
    }
  }

  private static class MapReader<K, V> extends RepeatedKeyValueReader<Map<K, V>, Map<K, V>, K, V> {
    private final ReusableEntry<K, V> nullEntry = new ReusableEntry<>();

    MapReader(int definitionLevel, int repetitionLevel,
              ParquetValueReader<K> keyReader, ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }

    @Override
    protected Map<K, V> newMapData(Map<K, V> reuse) {
      return new LinkedHashMap<>();
    }

    @Override
    protected Map.Entry<K, V> getPair(Map<K, V> reuse) {
      return nullEntry;
    }

    @Override
    protected void addPair(Map<K, V> map, K key, V value) {
      map.put(key, value);
    }

    @Override
    protected Map<K, V> buildMap(Map<K, V> map) {
      return map;
    }
  }

  private static class ArrayReader<T> extends RepeatedReader<DataBag, DataBag, T> {
    private final BagFactory bagFactory = BagFactory.getInstance();
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    ArrayReader(int definitionLevel, int repetitionLevel, ParquetValueReader<T> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected DataBag newListData(DataBag reuse) {
      return bagFactory.newDefaultBag();
    }

    @Override
    protected T getElement(DataBag list) {
      return null;
    }

    @Override
    protected void addElement(DataBag bag, T element) {
      bag.add(tupleFactory.newTuple(element));
    }

    @Override
    protected DataBag buildList(DataBag bag) {
      return bag;
    }
  }

  private static class TupleReader extends StructReader<Tuple, Tuple> {
    private static final TupleFactory TF = TupleFactory.getInstance();
    private final int numColumns;

    TupleReader(List<Type> types, List<ParquetValueReader<?>> readers) {
      super(types, readers);
      this.numColumns = readers.size();
    }

    @Override
    protected Tuple newStructData(Tuple reuse) {
      return TF.newTuple(numColumns);
    }

    @Override
    protected Object getField(Tuple tuple, int pos) {
      return null;
    }

    @Override
    protected Tuple buildStruct(Tuple tuple) {
      return tuple;
    }

    @Override
    protected void set(Tuple tuple, int pos, Object value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException(String.format("Error setting tuple value for pos: %d, value: %s", pos, value), e);
      }
    }
  }
}
