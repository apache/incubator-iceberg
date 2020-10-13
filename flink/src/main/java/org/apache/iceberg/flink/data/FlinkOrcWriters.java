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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

class FlinkOrcWriters {

  private FlinkOrcWriters() {
  }

  static OrcValueWriter<StringData> strings() {
    return StringWriter.INSTANCE;
  }

  static OrcValueWriter<Integer> dates() {
    return DateWriter.INSTANCE;
  }

  static OrcValueWriter<Integer> times() {
    return TimeWriter.INSTANCE;
  }

  static OrcValueWriter<TimestampData> timestamps() {
    return TimestampWriter.INSTANCE;
  }

  static OrcValueWriter<TimestampData> timestampTzs() {
    return TimestampTzWriter.INSTANCE;
  }

  static OrcValueWriter<DecimalData> decimals(int precision, int scale) {
    if (precision <= 18) {
      return new Decimal18Writer(precision, scale);
    } else if (precision <= 38) {
      return new Decimal38Writer(precision, scale);
    } else {
      throw new IllegalArgumentException("Invalid precision: " + precision);
    }
  }

  static <T> OrcValueWriter<ArrayData> list(OrcValueWriter<T> elementWriter, LogicalType elementType) {
    return new ListWriter<>(elementWriter, elementType);
  }

  static <K, V> OrcValueWriter<MapData> map(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter,
                                            LogicalType keyType, LogicalType valueType) {
    return new MapWriter<>(keyWriter, valueWriter, keyType, valueType);
  }

  static OrcValueWriter<RowData> struct(List<OrcValueWriter<?>> writers, List<LogicalType> types) {
    return new StructWriter(writers, types);
  }

  private static class StringWriter implements OrcValueWriter<StringData> {
    private static final StringWriter INSTANCE = new StringWriter();

    @Override
    public Class<?> getJavaClass() {
      return StringData.class;
    }

    @Override
    public void nonNullWrite(int rowId, StringData data, ColumnVector output) {
      byte[] value = data.toBytes();
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class DateWriter implements OrcValueWriter<Integer> {
    private static final DateWriter INSTANCE = new DateWriter();

    @Override
    public Class<?> getJavaClass() {
      return Integer.class;
    }

    @Override
    public void nonNullWrite(int rowId, Integer data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class TimeWriter implements OrcValueWriter<Integer> {
    private static final TimeWriter INSTANCE = new TimeWriter();

    @Override
    public Class<?> getJavaClass() {
      return Integer.class;
    }

    @Override
    public void nonNullWrite(int rowId, Integer millis, ColumnVector output) {
      // The time in flink is in millisecond, while the standard time in iceberg is microsecond.
      // So we need to transform it to microsecond.
      ((LongColumnVector) output).vector[rowId] = millis * 1000L;
    }
  }

  private static class TimestampWriter implements OrcValueWriter<TimestampData> {
    private static final TimestampWriter INSTANCE = new TimestampWriter();

    @Override
    public Class<?> getJavaClass() {
      return TimestampData.class;
    }

    @Override
    public void nonNullWrite(int rowId, TimestampData data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      cv.setIsUTC(true);
      // millis
      OffsetDateTime offsetDateTime = data.toInstant().atOffset(ZoneOffset.UTC);
      cv.time[rowId] = offsetDateTime.toEpochSecond() * 1_000 + offsetDateTime.getNano() / 1_000_000;
      // truncate nanos to only keep microsecond precision.
      cv.nanos[rowId] = (offsetDateTime.getNano() / 1_000) * 1_000;
    }
  }

  private static class TimestampTzWriter implements OrcValueWriter<TimestampData> {
    private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

    @Override
    public Class<TimestampData> getJavaClass() {
      return TimestampData.class;
    }

    @Override
    public void nonNullWrite(int rowId, TimestampData data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      // millis
      Instant instant = data.toInstant();
      cv.time[rowId] = instant.toEpochMilli();
      // truncate nanos to only keep microsecond precision.
      cv.nanos[rowId] = (instant.getNano() / 1_000) * 1_000;
    }
  }

  private static class Decimal18Writer implements OrcValueWriter<DecimalData> {
    private final int precision;
    private final int scale;

    Decimal18Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Class<?> getJavaClass() {
      return DecimalData.class;
    }

    @Override
    public void nonNullWrite(int rowId, DecimalData data, ColumnVector output) {
      Preconditions.checkArgument(scale == data.scale(),
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data);
      Preconditions.checkArgument(data.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, data);

      ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(data.toUnscaledLong(), data.scale());
    }
  }

  private static class Decimal38Writer implements OrcValueWriter<DecimalData> {
    private final int precision;
    private final int scale;

    Decimal38Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Class<?> getJavaClass() {
      return DecimalData.class;
    }

    @Override
    public void nonNullWrite(int rowId, DecimalData data, ColumnVector output) {
      Preconditions.checkArgument(scale == data.scale(),
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data);
      Preconditions.checkArgument(data.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, data);

      ((DecimalColumnVector) output).vector[rowId].set(HiveDecimal.create(data.toBigDecimal(), false));
    }
  }

  static class ListWriter<T> implements OrcValueWriter<ArrayData> {
    private final OrcValueWriter<T> elementWriter;
    private final ArrayData.ElementGetter elementGetter;

    ListWriter(OrcValueWriter<T> elementWriter, LogicalType elementType) {
      this.elementWriter = elementWriter;
      this.elementGetter = ArrayData.createElementGetter(elementType);
    }

    @Override
    public Class<?> getJavaClass() {
      return ArrayData.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, ArrayData data, ColumnVector output) {
      ListColumnVector cv = (ListColumnVector) output;
      cv.lengths[rowId] = data.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough.
      cv.child.ensureSize(cv.childCount, true);

      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        Object value = elementGetter.getElementOrNull(data, e);
        elementWriter.write((int) (e + cv.offsets[rowId]), (T) value, cv.child);
      }
    }
  }

  static class MapWriter<K, V> implements OrcValueWriter<MapData> {
    private final OrcValueWriter<K> keyWriter;
    private final OrcValueWriter<V> valueWriter;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    MapWriter(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter,
              LogicalType keyType, LogicalType valueType) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
      this.keyGetter = ArrayData.createElementGetter(keyType);
      this.valueGetter = ArrayData.createElementGetter(valueType);
    }

    @Override
    public Class<?> getJavaClass() {
      return MapData.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, MapData data, ColumnVector output) {
      MapColumnVector cv = (MapColumnVector) output;
      ArrayData keyArray = data.keyArray();
      ArrayData valArray = data.valueArray();

      // record the length and start of the list elements
      cv.lengths[rowId] = data.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough
      cv.keys.ensureSize(cv.childCount, true);
      cv.values.ensureSize(cv.childCount, true);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        int pos = (int) (e + cv.offsets[rowId]);
        keyWriter.write(pos, (K) keyGetter.getElementOrNull(keyArray, e), cv.keys);
        valueWriter.write(pos, (V) valueGetter.getElementOrNull(valArray, e), cv.values);
      }
    }
  }

  static class StructWriter implements OrcValueWriter<RowData> {
    private final List<OrcValueWriter<?>> writers;
    private final List<RowData.FieldGetter> fieldGetters;

    StructWriter(List<OrcValueWriter<?>> writers, List<LogicalType> types) {
      this.writers = writers;

      this.fieldGetters = Lists.newArrayListWithExpectedSize(types.size());
      for (int i = 0; i < types.size(); i++) {
        fieldGetters.add(RowData.createFieldGetter(types.get(i), i));
      }
    }

    List<OrcValueWriter<?>> writers() {
      return writers;
    }

    @Override
    public Class<?> getJavaClass() {
      return RowData.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, RowData data, ColumnVector output) {
      StructColumnVector cv = (StructColumnVector) output;
      for (int c = 0; c < writers.size(); ++c) {
        OrcValueWriter writer = writers.get(c);
        writer.write(rowId, fieldGetters.get(c).getFieldOrNull(data), cv.fields[c]);
      }
    }
  }
}
