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

package org.apache.iceberg;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public class MetricsAppender<D> implements FileAppender<D> {

  private final FileAppender<D> appender;
  private final Schema schema;
  private Metrics metrics = null;
  private MetricsConfig metricsConfig = MetricsConfig.getDefault();
  private MetricsCollector metricsCollector;

  private MetricsAppender(FileAppender<D> appender, Schema schema, MetricsAppenderConfiguration conf) {
    this.appender = appender;
    this.schema = schema;
    this.metricsCollector = TypeUtil.visit(schema, new BuildMetricsCollector(conf));
  }

  @Override
  public void add(D datum) {
    appender.add(datum);
    metricsCollector.add(datum);
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(metrics != null, "Cannot produce metrics until closed");
    return metrics;
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    appender.close();

    Map<Integer, Long> values = new HashMap<Integer, Long>();
    Map<Integer, Long> nulls = new HashMap<Integer, Long>();
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<Integer, ByteBuffer>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<Integer, ByteBuffer>();

    ((Stream<FieldMetrics>) metricsCollector.getMetrics()).forEach(fieldMetrics -> {
      values.put(fieldMetrics.getId(), fieldMetrics.getValueCount());
      nulls.put(fieldMetrics.getId(), fieldMetrics.getNullValueCount());
      lowerBounds.put(fieldMetrics.getId(), fieldMetrics.getLowerBound());
      upperBounds.put(fieldMetrics.getId(), fieldMetrics.getUpperBound());
    });

    this.metrics = new Metrics(metricsCollector.count(),
        null,
        values,
        nulls,
        lowerBounds,
        upperBounds);
  }

  private enum RecordRepresentation {
    RECORD,
    GENERIC_RECORD
  }

  private enum FixedTypeRepresentation {
    BYTE_ARRAY,
    BYTE_BUFFER,
    GENERIC_FIXED
  }

  private enum DateTypeRepresentation {
    INT,
    LOCAL_DATE
  }

  private enum TimeTypeRepresentation {
    LONG,
    LOCAL_TIME
  }

  private enum TimestampTypeRepresentation {
    LONG,
    OFFSET_DATETIME,
    LOCAL_DATETIME
  }

  private static class MetricsAppenderConfiguration {
    private final FixedTypeRepresentation fixedTypeRepresentation;
    private final DateTypeRepresentation dateTypeRepresentation;
    private final TimeTypeRepresentation timeTypeRepresentation;
    private final TimestampTypeRepresentation timestampTypeRepresentation;
    private final RecordRepresentation recordRepresentation;

    MetricsAppenderConfiguration(RecordRepresentation recordRepresentation,
                                 FixedTypeRepresentation fixedTypeRepresentation,
                                 DateTypeRepresentation dateTypeRepresentation,
                                 TimeTypeRepresentation timeTypeRepresentation,
                                 TimestampTypeRepresentation timestampTypeRepresentation) {
      this.recordRepresentation = recordRepresentation;
      this.fixedTypeRepresentation = fixedTypeRepresentation;
      this.dateTypeRepresentation = dateTypeRepresentation;
      this.timeTypeRepresentation = timeTypeRepresentation;
      this.timestampTypeRepresentation = timestampTypeRepresentation;
    }

    public FixedTypeRepresentation getFixedTypeRepresentation() {
      return fixedTypeRepresentation;
    }

    public DateTypeRepresentation getDateTypeRepresentation() {
      return dateTypeRepresentation;
    }

    public TimestampTypeRepresentation getTimestampTypeRepresentation() {
      return timestampTypeRepresentation;
    }
  }

  public static class Builder<D> {

    private final FileAppender<D> appender;
    private final Schema schema;
    private FixedTypeRepresentation fixedTypeRepresentation;
    private DateTypeRepresentation dateTypeRepresentation;
    private TimeTypeRepresentation timeTypeRepresentation;
    private TimestampTypeRepresentation timestampTypeRepresentation;
    private RecordRepresentation recordRepresentation;

    public Builder(FileAppender<D> appender, Schema schema) {
      this.appender = appender;
      this.schema = schema;
      // TODO default null values or force setting every time?
      fixedTypeRepresentation = FixedTypeRepresentation.BYTE_ARRAY;
      dateTypeRepresentation = DateTypeRepresentation.LOCAL_DATE;
      timeTypeRepresentation = TimeTypeRepresentation.LOCAL_TIME;
      timestampTypeRepresentation = TimestampTypeRepresentation.OFFSET_DATETIME;
      recordRepresentation = RecordRepresentation.RECORD;
    }

    // TODO or setRecordRepresentation(RecordRepresentation) for every option?
    public Builder useRecord() {
      recordRepresentation = RecordRepresentation.RECORD;
      return this;
    }

    public Builder useGenericRecord() {
      recordRepresentation = RecordRepresentation.GENERIC_RECORD;
      return this;
    }

    public Builder useByteArrayForFixedType() {
      fixedTypeRepresentation = FixedTypeRepresentation.BYTE_ARRAY;
      return this;
    }

    public Builder useByteBufferForFixedType() {
      fixedTypeRepresentation = FixedTypeRepresentation.BYTE_BUFFER;
      return this;
    }

    public Builder useGenericFixedForFixedType() {
      fixedTypeRepresentation = FixedTypeRepresentation.GENERIC_FIXED;
      return this;
    }

    public Builder useIntForDateType() {
      dateTypeRepresentation = DateTypeRepresentation.INT;
      return this;
    }

    public Builder useLocalDateForDateType() {
      dateTypeRepresentation = DateTypeRepresentation.LOCAL_DATE;
      return this;
    }

    public Builder useLongForTimeType() {
      timeTypeRepresentation = TimeTypeRepresentation.LONG;
      return this;
    }

    public Builder useLocalTimeForTimeType() {
      timeTypeRepresentation = TimeTypeRepresentation.LOCAL_TIME;
      return this;
    }

    public Builder useLongForTimestampType() {
      timestampTypeRepresentation = TimestampTypeRepresentation.LONG;
      return this;
    }

    public Builder useDateTimeForTimestampType() {
      timestampTypeRepresentation = TimestampTypeRepresentation.OFFSET_DATETIME;
      return this;
    }

    public Builder useLocalDateTimeForTimestampType() {
      timestampTypeRepresentation = TimestampTypeRepresentation.LOCAL_DATETIME;
      return this;
    }


    public MetricsAppender<D> build() {
      // TODO check iif not defaults??
      return new MetricsAppender<>(appender, schema,
          new MetricsAppenderConfiguration(recordRepresentation,
              fixedTypeRepresentation,
              dateTypeRepresentation,
              timeTypeRepresentation,
              timestampTypeRepresentation));
    }
  }

  private static class MetricsCollectorMap<K, V> extends MetricsCollectorBase<Map<K, V>> {
    private final MetricsCollector collectorKey;
    private final MetricsCollector collectorValue;

    MetricsCollectorMap(MetricsCollector<K> collectorKey, MetricsCollector<V> collectorValue) {
      this.collectorKey = collectorKey;
      this.collectorValue = collectorValue;
    }

    @Override
    public void add(Map<K, V> map) {
      if (map != null) {
        for (K k : map.keySet()) {
          collectorKey.add(k);
          collectorValue.add(map.get(k));
        }
      } else {
        collectorKey.add(null);
        collectorValue.add(null);
      }
    }

    @Override
    public Stream<FieldMetrics> getMetrics() {
      return Stream.of(collectorKey, collectorValue).flatMap(MetricsCollector::getMetrics);
    }
  }

  private static class MetricsCollectorList<L> extends MetricsCollectorBase<List<L>> {

    private final MetricsCollector collector;

    MetricsCollectorList(MetricsCollector<L> collector) {
      this.collector = collector;
    }

    @Override
    public void add(List<L> list) {
      if (list != null) {
        for (L e : list) {
          collector.add(e);
        }
      } else {
        collector.add(null);
      }
    }

    @Override
    public Stream<FieldMetrics> getMetrics() {
      return collector.getMetrics();
    }
  }

  private static class MetricsCollectorGenericRecord extends MetricsCollectorBase<GenericData.Record> {
    private final Integer id;
    private final List<MetricsCollector> collectors;
    private Long count;
    private Long nulls;

    MetricsCollectorGenericRecord(Integer id, List<MetricsCollector> collectors) {
      this.collectors = collectors;
      this.id = id;
      count = 0L;
      nulls = 0L;
    }

    @Override
    public void add(GenericData.Record record) {
      count++;
      if (record == null) {
        nulls++;
      }
      for (int i = 0; i < collectors.size(); i++) {
        if (collectors.get(i) != null) { // TODO: necessary
          collectors.get(i).add(record == null ? null : record.get(i));
        }
      }
    }

    @Override
    public Stream<FieldMetrics> getMetrics() {
      return Stream.concat(collectors.stream().flatMap(MetricsCollector::getMetrics),
          (id != null) ? Stream.of(new FieldMetrics(id, count, nulls, null, null)) : Stream.empty());
    }

    @Override
    public Long count() {
      return count;
    }
  }

  private static class MetricsCollectorRecord extends MetricsCollectorBase<Record> {
    private final List<MetricsCollector> collectors;
    private final Integer id;
    private Long count;
    private Long nulls;

    MetricsCollectorRecord(Integer id, List<MetricsCollector> collectors) {
      this.id = id;
      this.collectors = collectors;
      count = 0L;
      nulls = 0L;
    }

    @Override
    public void add(Record record) {
      count++;
      if (record == null) {
        nulls++;
      }
      for (int i = 0; i < collectors.size(); i++) {
        if (collectors.get(i) != null) { // TODO: necessary
          collectors.get(i).add(record == null ? null : record.get(i));
        }
      }
    }

    @Override
    public Stream<FieldMetrics> getMetrics() {
      return Stream.concat(collectors.stream().flatMap(MetricsCollector::getMetrics),
          (id != null) ? Stream.of(new FieldMetrics(id, count, nulls, null, null)) : Stream.empty());
    }

    @Override
    public Long count() {
      return count;
    }
  }

  abstract static class MetricsCollectorPrimitive<D> extends MetricsCollectorBase<D> {
    private final Comparator<D> comparator;
    private D max;
    private D min;
    private Long values = 0L;
    private Long nulls = 0L;
    private final Integer id;

    MetricsCollectorPrimitive(Integer id, Comparator<D> comparator) {
      this.id = id;
      this.comparator = comparator;
    }

    @Override
    public void add(D datum) {
      values++;
      if (datum == null) {
        nulls++;
      } else {
        if (max == null || comparator.compare(datum, max) > 0) {
          max = datum;
        }
        if (min == null || comparator.compare(datum, min) < 0) {
          min = datum;
        }
      }
    }

    abstract ByteBuffer encode(D datum);

    @Override
    public Stream<FieldMetrics> getMetrics() {
      return Stream.of(new FieldMetrics(id, values, nulls, encode(min), encode(max)));
    }
  }

  private static class MetricsCollectorInt extends MetricsCollectorPrimitive<Integer> {
    MetricsCollectorInt(Integer id, Comparator<Integer> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Integer datum) {
      return Conversions.toByteBuffer(Types.IntegerType.get(), datum);
    }
  }

  private static class MetricsCollectorLong extends MetricsCollectorPrimitive<Long> {
    MetricsCollectorLong(Integer id, Comparator<Long> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Long datum) {
      return Conversions.toByteBuffer(Types.LongType.get(), datum);
    }
  }

  private static class MetricsCollectorBoolean extends MetricsCollectorPrimitive<Boolean> {
    MetricsCollectorBoolean(Integer id, Comparator<Boolean> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Boolean datum) {
      return Conversions.toByteBuffer(Types.BooleanType.get(), datum);
    }
  }

  private static class MetricsCollectorString extends MetricsCollectorPrimitive<String> {
    MetricsCollectorString(Integer id, Comparator<String> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(String datum) {
      return Conversions.toByteBuffer(Types.StringType.get(), datum);
    }
  }

  private static class MetricsCollectorFloat extends MetricsCollectorPrimitive<Float> {
    MetricsCollectorFloat(Integer id, Comparator<Float> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Float datum) {
      return Conversions.toByteBuffer(Types.FloatType.get(), datum);
    }
  }

  private static class MetricsCollectorDouble extends MetricsCollectorPrimitive<Double> {
    MetricsCollectorDouble(Integer id, Comparator<Double> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Double datum) {
      return Conversions.toByteBuffer(Types.DoubleType.get(), datum);
    }
  }

  private static class MetricsCollectorUUID extends MetricsCollectorPrimitive<UUID> {
    MetricsCollectorUUID(Integer id, Comparator<UUID> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(UUID datum) {
      return Conversions.toByteBuffer(Types.UUIDType.get(), datum);
    }
  }

  private static class MetricsCollectorBytes extends MetricsCollectorPrimitive<ByteBuffer> {
    MetricsCollectorBytes(Integer id, Comparator<ByteBuffer> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(ByteBuffer datum) {
      return Conversions.toByteBuffer(Types.BinaryType.get(), datum);
    }
  }

  private static class MetricsCollectorDate extends MetricsCollectorPrimitive<LocalDate> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    MetricsCollectorDate(Integer id, Comparator<LocalDate> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(LocalDate datum) {
      return Conversions.toByteBuffer(Types.DateType.get(), (int) ChronoUnit.DAYS.between(EPOCH_DAY, datum));
    }
  }

  private static class MetricsCollectorTime extends MetricsCollectorPrimitive<LocalTime> {
    MetricsCollectorTime(Integer id, Comparator<LocalTime> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(LocalTime datum) {
      return Conversions.toByteBuffer(Types.TimeType.get(), datum.toNanoOfDay() / 1000);
    }
  }

  private static class MetricsCollectorLocalDateTime extends MetricsCollectorPrimitive<LocalDateTime> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private final boolean withoutTimeZone;

    MetricsCollectorLocalDateTime(Integer id, Comparator<LocalDateTime> comparator, boolean withoutTimeZone) {
      super(id, comparator);
      this.withoutTimeZone = withoutTimeZone;
    }

    @Override
    ByteBuffer encode(LocalDateTime datum) {
      // TODO check
      return Conversions.toByteBuffer(
          withoutTimeZone ? Types.TimestampType.withoutZone() : Types.TimestampType.withZone(),
          ChronoUnit.MICROS.between(EPOCH, datum.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class MetricsCollectorOffsetDateTime extends MetricsCollectorPrimitive<OffsetDateTime> {
    private final boolean withoutTimeZone;

    MetricsCollectorOffsetDateTime(Integer id, Comparator<OffsetDateTime> comparator, boolean withoutTimeZone) {
      super(id, comparator);
      this.withoutTimeZone = withoutTimeZone;
    }

    @Override
    ByteBuffer encode(OffsetDateTime datum) {
      return Conversions.toByteBuffer(
          withoutTimeZone ? Types.TimestampType.withoutZone() : Types.TimestampType.withZone(),
          DateTimeUtil.microsFromTimestamptz(datum));
    }
  }

  private static class MetricsCollectorGenericFixed extends MetricsCollectorPrimitive<GenericFixed> {
    private final int length;

    MetricsCollectorGenericFixed(Integer id, Comparator<GenericFixed> comparator, int length) {
      super(id, comparator);
      this.length = length;
    }

    @Override
    ByteBuffer encode(GenericFixed datum) {
      return Conversions.toByteBuffer(Types.FixedType.ofLength(length), ByteBuffer.wrap(datum.bytes()));
    }
  }

  private static class MetricsCollectorFixedByteBuffer extends MetricsCollectorPrimitive<ByteBuffer> {
    private final int length;

    MetricsCollectorFixedByteBuffer(Integer id, Comparator<ByteBuffer> comparator, int length) {
      super(id, comparator);
      this.length = length;
    }

    @Override
    ByteBuffer encode(ByteBuffer datum) {
      return datum;
    }
  }

  private static class MetricsCollectorFixed extends MetricsCollectorPrimitive<byte[]> {
    private final int length;

    MetricsCollectorFixed(Integer id, Comparator<byte[]> comparator, int length) {
      super(id, comparator);
      this.length = length;
    }

    @Override
    ByteBuffer encode(byte[] datum) {
      return Conversions.toByteBuffer(Types.FixedType.ofLength(length), ByteBuffer.wrap(datum));
    }
  }

  private static class MetricsCollectorDecimal extends MetricsCollectorPrimitive<BigDecimal> {
    private final int precision;
    private final int scale;

    MetricsCollectorDecimal(Integer id, Comparator<BigDecimal> comparator, int precision, int scale) {
      super(id, comparator);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    ByteBuffer encode(BigDecimal datum) {
      return Conversions.toByteBuffer(Types.DecimalType.of(precision, scale), datum);
    }
  }

  // TODO shall we move it to Comparators?
  private static class UnsignedByteArrayComparator implements Comparator<byte[]> {
    private static final UnsignedByteArrayComparator INSTANCE = new UnsignedByteArrayComparator();

    private UnsignedByteArrayComparator() {
    }

    @Override
    public int compare(byte[] buf1, byte[] buf2) {
      int len = Math.min(buf1.length, buf2.length);

      // find the first difference and return
      for (int i = 0; i < len; i += 1) {
        // Conversion to int is what Byte.toUnsignedInt would do
        int cmp = Integer.compare(
            ((int) buf1[i]) & 0xff,
            ((int) buf2[i]) & 0xff);
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is first
      return Integer.compare(buf1.length, buf2.length);
    }
  }

  private static class UnsignedGenericFixedComparator implements Comparator<GenericFixed> {
    private static final UnsignedGenericFixedComparator INSTANCE = new UnsignedGenericFixedComparator();
    private static final UnsignedByteArrayComparator byteArrayComparator = UnsignedByteArrayComparator.INSTANCE;

    private UnsignedGenericFixedComparator() {}

    @Override
    public int compare(GenericFixed buf1, GenericFixed buf2) {
      return byteArrayComparator.compare(buf1.bytes(), buf2.bytes());
    }
  }

  private static class BuildMetricsCollector
      extends TypeUtil.SchemaVisitor<MetricsCollector> {

    private final MetricsAppenderConfiguration conf;
    //private Types.NestedField currentField = null;
    private Deque<Types.NestedField> stack = Lists.newLinkedList();

    private BuildMetricsCollector(MetricsAppenderConfiguration conf) {
      this.conf = conf;
    }

    @Override
    public void beforeField(Types.NestedField field) {
      stack.push(field);
    }

    @Override
    public void afterField(Types.NestedField field) {
      stack.pop();
    }

    @Override
    public MetricsCollector schema(
        Schema schema, MetricsCollector metricsCollector) {
      return metricsCollector;
    }

    @Override
    public MetricsCollector struct(Types.StructType struct, List<MetricsCollector> fieldResults) {
      MetricsCollector res = null;
      Integer fieldId = stack.peek() != null ? stack.peek().fieldId() : null;

      switch (conf.recordRepresentation) {
        case RECORD:
          res = new MetricsCollectorRecord(fieldId, fieldResults);
          break;
        case GENERIC_RECORD:
          res = new MetricsCollectorGenericRecord(fieldId, fieldResults);
          break;
        default:
          throw new UnsupportedOperationException("Not a supported record representation: " +
              conf.recordRepresentation);
      }

      return res;
    }

    @Override
    public MetricsCollector field(Types.NestedField field, MetricsCollector fieldResult) {
      return fieldResult;
    }

    @Override
    public MetricsCollector list(Types.ListType list, MetricsCollector elementResult) {
      return new MetricsCollectorList(elementResult);
    }

    @Override
    public MetricsCollector map(Types.MapType map, MetricsCollector keyResult, MetricsCollector valueResult) {
      return new MetricsCollectorMap(keyResult, valueResult);
    }

    @Override
    public MetricsCollector primitive(Type.PrimitiveType primitive) {
      int fieldId = stack.peek().fieldId();

      switch (primitive.typeId()) {
        case BOOLEAN:
          return new MetricsCollectorBoolean(fieldId, Comparators.forType(primitive));
        case INTEGER:
          return new MetricsCollectorInt(fieldId, Comparators.forType(primitive));
        case LONG:
          return new MetricsCollectorLong(fieldId, Comparators.forType(primitive));
        case FLOAT:
          return new MetricsCollectorFloat(fieldId, Comparators.forType(primitive));
        case DOUBLE:
          return new MetricsCollectorDouble(fieldId, Comparators.forType(primitive));
        case DATE:
          MetricsCollector metricsCollectorDate = null;

          switch (conf.dateTypeRepresentation) {
            case INT:
              metricsCollectorDate = new MetricsCollectorInt(fieldId, Comparators.forType(Types.IntegerType.get()));
              break;
            case LOCAL_DATE:
              metricsCollectorDate = new MetricsCollectorDate(fieldId, Comparators.forType(primitive));
              break;
            default:
              throw new UnsupportedOperationException("Not a supported date type representation: " +
                  conf.dateTypeRepresentation);
          }

          return metricsCollectorDate;
        case TIME:
          MetricsCollector metricsCollectorTime = null;

          switch (conf.timeTypeRepresentation) {
            case LOCAL_TIME:
              metricsCollectorTime = new MetricsCollectorTime(fieldId, Comparators.forType(primitive));
              break;
            case LONG:
              metricsCollectorTime = new MetricsCollectorLong(fieldId, Comparators.forType(Types.LongType.get()));
              break;
            default:
              throw new UnsupportedOperationException("Not a supported time type representation: " +
                  conf.timeTypeRepresentation);
          }

          return metricsCollectorTime;
        case TIMESTAMP:
          Types.TimestampType timestamp = (Types.TimestampType) primitive;
          MetricsCollector metricsCollector = null;

          switch (conf.timestampTypeRepresentation) {
            case LOCAL_DATETIME:
              metricsCollector = new MetricsCollectorLocalDateTime(fieldId,
                  Comparators.forType(primitive), timestamp.shouldAdjustToUTC());
              break;
            case OFFSET_DATETIME:
              metricsCollector = new MetricsCollectorOffsetDateTime(fieldId,
                  Comparator.naturalOrder(), timestamp.shouldAdjustToUTC());
              break;
            case LONG:
              metricsCollector = new MetricsCollectorLong(fieldId,
                  Comparators.forType(Types.LongType.get()));
              break;
            default:
              throw new UnsupportedOperationException("Not a supported timestamp type representation: " +
                  conf.timestampTypeRepresentation);
          }

          return metricsCollector;
        case STRING:
          return new MetricsCollectorString(fieldId, Comparators.forType(primitive));
        case UUID:
          return new MetricsCollectorUUID(fieldId, Comparators.forType(primitive));
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) primitive;
          MetricsCollector metricsCollectorFixed = null;

          switch (conf.fixedTypeRepresentation) {
            case BYTE_ARRAY:
              metricsCollectorFixed = new MetricsCollectorFixed(fieldId,
                  UnsignedByteArrayComparator.INSTANCE, fixed.length());
              break;
            case BYTE_BUFFER:
              metricsCollectorFixed = new MetricsCollectorFixedByteBuffer(fieldId,
                  Comparators.forType(primitive), fixed.length());
              break;
            case GENERIC_FIXED:
              metricsCollectorFixed = new MetricsCollectorGenericFixed(fieldId,
                  UnsignedGenericFixedComparator.INSTANCE, fixed.length());
              break;
            default:
              throw new UnsupportedOperationException("Not a supported fixed type representation: " +
                  conf.fixedTypeRepresentation);
          }

          return metricsCollectorFixed;
        case BINARY:
          return new MetricsCollectorBytes(fieldId, Comparators.forType(primitive));
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) primitive;
          return new MetricsCollectorDecimal(fieldId,
              Comparators.forType(primitive), decimal.precision(), decimal.scale());
        default:
          throw new UnsupportedOperationException("Not a supported type: " + primitive);
      }
    }
  }
}
