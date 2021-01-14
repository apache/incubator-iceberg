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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestAvroMetricsBounds {
  // all supported types, except for UUID which is on deprecation path: see https://github.com/apache/iceberg/pull/1611
  protected static final Types.NestedField INT_FIELD = optional(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField STRING_FIELD = optional(2, "string", Types.StringType.get());
  protected static final Types.NestedField FLOAT_FIELD = optional(3, "float", Types.FloatType.get());
  protected static final Types.NestedField DOUBLE_FIELD = optional(4, "double", Types.DoubleType.get());
  protected static final Types.NestedField DECIMAL_FIELD = optional(5, "decimal", Types.DecimalType.of(5, 3));
  protected static final Types.NestedField FIXED_FIELD = optional(6, "fixed", Types.FixedType.ofLength(4));
  protected static final Types.NestedField BINARY_FIELD = optional(7, "binary", Types.BinaryType.get());
  protected static final Types.NestedField LONG_FIELD = optional(8, "long", Types.LongType.get());
  protected static final Types.NestedField BOOLEAN_FIELD = optional(9, "boolean", Types.BooleanType.get());
  protected static final Types.NestedField DATE_FIELD = optional(10, "date", Types.DateType.get());
  protected static final Types.NestedField TIME_FIELD = optional(11, "time", Types.TimeType.get());
  protected static final Types.NestedField TIMESTAMPZ_FIELD = optional(12, "timestampz",
      Types.TimestampType.withZone());
  protected static final Types.NestedField TIMESTAMP_FIELD = optional(13, "timestamp",
      Types.TimestampType.withoutZone());

  protected static final Map<Types.NestedField, Schema> SINGLE_FIELD_SCHEMA_MAP = populateSingleFieldSchemaMap();

  private static Map<Types.NestedField, Schema> populateSingleFieldSchemaMap() {
    Set<Types.NestedField> allTypes = new HashSet<>();
    allTypes.add(INT_FIELD);
    allTypes.add(STRING_FIELD);
    allTypes.add(FLOAT_FIELD);
    allTypes.add(DOUBLE_FIELD);
    allTypes.add(DECIMAL_FIELD);
    allTypes.add(FIXED_FIELD);
    allTypes.add(BINARY_FIELD);
    allTypes.add(LONG_FIELD);
    allTypes.add(BOOLEAN_FIELD);
    allTypes.add(DATE_FIELD);
    allTypes.add(TIME_FIELD);
    allTypes.add(TIMESTAMPZ_FIELD);
    allTypes.add(TIMESTAMP_FIELD);
    return allTypes.stream().collect(Collectors.toMap(Function.identity(), Schema::new));
  }

  protected Metrics writeAndGetMetrics(Types.NestedField field, Record... records) throws IOException {
    return writeAndGetMetrics(SINGLE_FIELD_SCHEMA_MAP.get(field), records);
  }

  protected abstract Metrics writeAndGetMetrics(Schema schema, Record... records) throws IOException;

  protected boolean supportTimeWithoutZoneField() {
    return true;
  }

  protected boolean supportTimeField() {
    return true;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testIntFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(INT_FIELD,
        createRecord(INT_FIELD, 3),
        createRecord(INT_FIELD, 8),
        createRecord(INT_FIELD, -1),
        createRecord(INT_FIELD, 0));
    assertBounds(INT_FIELD, -1, 8, metrics);
  }

  @Test
  public void testStringFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(STRING_FIELD,
        createRecord(STRING_FIELD, "aaa"),
        createRecord(STRING_FIELD, "aab"),
        createRecord(STRING_FIELD, "az"),
        createRecord(STRING_FIELD, "a"));
    assertBounds(STRING_FIELD, CharBuffer.wrap("a"), CharBuffer.wrap("az"), metrics);
  }

  @Test
  public void testFloatFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(FLOAT_FIELD,
        createRecord(FLOAT_FIELD, 3.2F),
        createRecord(FLOAT_FIELD, 8.9F),
        createRecord(FLOAT_FIELD, -1.1F),
        createRecord(FLOAT_FIELD, Float.NaN),
        createRecord(FLOAT_FIELD, 0F));
    assertBounds(FLOAT_FIELD, -1.1F, 8.9F, metrics);
  }

  @Test
  public void testDoubleFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(DOUBLE_FIELD,
        createRecord(DOUBLE_FIELD, 3.2D),
        createRecord(DOUBLE_FIELD, 8.9D),
        createRecord(DOUBLE_FIELD, -1.1D),
        createRecord(DOUBLE_FIELD, Double.NaN),
        createRecord(DOUBLE_FIELD, 0D));
    assertBounds(DOUBLE_FIELD, -1.1D, 8.9D, metrics);
  }

  @Test
  public void testDecimalFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(DECIMAL_FIELD,
        createRecord(DECIMAL_FIELD, new BigDecimal("3.599")),
        createRecord(DECIMAL_FIELD, new BigDecimal("3.600")),
        createRecord(DECIMAL_FIELD, new BigDecimal("-25.613")),
        createRecord(DECIMAL_FIELD, new BigDecimal("-25.614")));
    assertBounds(DECIMAL_FIELD, new BigDecimal("-25.614"), new BigDecimal("3.600"), metrics);
  }

  @Test
  public void testFixedFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(FIXED_FIELD,
        createRecord(FIXED_FIELD, bytes("abcd")),
        createRecord(FIXED_FIELD, bytes("abcz")),
        createRecord(FIXED_FIELD, bytes("abce")),
        createRecord(FIXED_FIELD, bytes("aacd")));
    assertBounds(FIXED_FIELD, byteBuffer("aacd"), byteBuffer("abcz"), metrics);
  }

  @Test
  public void testBinaryFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(BINARY_FIELD,
        createRecord(BINARY_FIELD, ByteBuffer.wrap(new byte[] { 0x0A, 0x01})),
        createRecord(BINARY_FIELD, ByteBuffer.wrap(new byte[] { 0x01, 0x0A, 0x0B })),
        createRecord(BINARY_FIELD, ByteBuffer.wrap(new byte[] { 0x10 })),
        createRecord(BINARY_FIELD, ByteBuffer.wrap(new byte[] { 0x10, 0x01 })));
    assertBounds(BINARY_FIELD,
        ByteBuffer.wrap(new byte[] { 0x01, 0x0A, 0x0B }),
        ByteBuffer.wrap(new byte[] { 0x10, 0x01 }),
        metrics);
  }

  @Test
  public void testLongFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(LONG_FIELD,
        createRecord(LONG_FIELD, 52L),
        createRecord(LONG_FIELD, 8L),
        createRecord(LONG_FIELD, -1L),
        createRecord(LONG_FIELD, 0L));
    assertBounds(LONG_FIELD, -1L, 52L, metrics);
  }

  @Test
  public void testBooleanFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(BOOLEAN_FIELD,
        createRecord(BOOLEAN_FIELD, false),
        createRecord(BOOLEAN_FIELD, false),
        createRecord(BOOLEAN_FIELD, false),
        createRecord(BOOLEAN_FIELD, true));
    assertBounds(BOOLEAN_FIELD, false, true, metrics);
  }

  @Test
  public void testDateFieldBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(DATE_FIELD,
        createRecord(DATE_FIELD, DateTimeUtil.dateFromDays(-1)),
        createRecord(DATE_FIELD, DateTimeUtil.dateFromDays(32)),
        createRecord(DATE_FIELD, DateTimeUtil.dateFromDays(0)),
        createRecord(DATE_FIELD, DateTimeUtil.dateFromDays(1276)));
    assertBounds(DATE_FIELD, -1, 1276, metrics);
  }

  @Test
  public void testDateTimeBound() throws IOException {
    Assume.assumeTrue("Skip test for engine that do not support time field", supportTimeField());

    Metrics metrics = writeAndGetMetrics(TIME_FIELD,
        createRecord(TIME_FIELD, DateTimeUtil.timeFromMicros(3000L)),
        createRecord(TIME_FIELD, DateTimeUtil.timeFromMicros(80000000000L)),
        createRecord(TIME_FIELD, DateTimeUtil.timeFromMicros(0L)),
        createRecord(TIME_FIELD, DateTimeUtil.timeFromMicros(27000L)));
    assertBounds(TIME_FIELD, 0L, 80000000000L, metrics);
  }

  @Test
  public void testDateTimestampBound() throws IOException {
    Assume.assumeTrue("Skip test for engine that do not support timestamp without time zone field",
        supportTimeWithoutZoneField());
    Metrics metrics = writeAndGetMetrics(TIMESTAMP_FIELD,
        createRecord(TIMESTAMP_FIELD, DateTimeUtil.timestampFromMicros(3000L)),
        createRecord(TIMESTAMP_FIELD, DateTimeUtil.timestampFromMicros(80000000000L)),
        createRecord(TIMESTAMP_FIELD, DateTimeUtil.timestampFromMicros(-1L)),
        createRecord(TIMESTAMP_FIELD, DateTimeUtil.timestampFromMicros(3000L)));
    assertBounds(TIMESTAMP_FIELD, -1L, 80000000000L, metrics);
  }

  @Test
  public void testDateTimestampWithZoneBound() throws IOException {
    Metrics metrics = writeAndGetMetrics(TIMESTAMPZ_FIELD,
        createRecord(TIMESTAMPZ_FIELD, DateTimeUtil.timestamptzFromMicros(3000L)),
        createRecord(TIMESTAMPZ_FIELD, DateTimeUtil.timestamptzFromMicros(80000000000L)),
        createRecord(TIMESTAMPZ_FIELD, DateTimeUtil.timestamptzFromMicros(-1L)),
        createRecord(TIMESTAMPZ_FIELD, DateTimeUtil.timestamptzFromMicros(3000L)));
    assertBounds(TIMESTAMPZ_FIELD, -1L, 80000000000L, metrics);
  }

  @Test
  public void testArrayBound() throws IOException {
    Types.NestedField listType = optional(1, "list",
        Types.ListType.ofRequired(2, Types.IntegerType.get()));
    Schema schema = new Schema(listType);
    Metrics metrics = writeAndGetMetrics(schema,
        createRecord(schema, "list", ImmutableList.of(3, 2, 1)),
        createRecord(schema, "list", ImmutableList.of(6, 4, 5)),
        createRecord(schema, "list", ImmutableList.of(8, 9, 7)));

    assertNullBounds(1, metrics);
    assertBounds(2, Types.IntegerType.get(), 1, 9, metrics);
  }

  @Test
  public void testMapBound() throws IOException {
    Types.NestedField mapField = optional(1, "map",
        Types.MapType.ofOptional(2, 3, Types.StringType.get(),
            Types.ListType.ofRequired(4, Types.IntegerType.get())));
    Schema schema = new Schema(mapField);
    Metrics metrics = writeAndGetMetrics(schema,
        createRecord(schema, "map", ImmutableMap.of("az", ImmutableList.of(3, 2, 1))),
        createRecord(schema, "map", ImmutableMap.of("aaaaa", ImmutableList.of(6, 4, 5))),
        createRecord(schema, "map", ImmutableMap.of("ab", ImmutableList.of(8, 9, 7))));

    assertNullBounds(1, metrics);
    assertBounds(2, Types.StringType.get(), CharBuffer.wrap("aaaaa"), CharBuffer.wrap("az"), metrics);
    assertNullBounds(3, metrics);
    assertBounds(4, Types.IntegerType.get(), 1, 9, metrics);
  }

  @Test
  public void testStructBound() throws IOException {
    Types.StructType leafField = Types.StructType.of(
        optional(2, "string", Types.StringType.get()),
        optional(3, "list", Types.ListType.ofRequired(4, Types.IntegerType.get())));
    Types.NestedField structField = optional(1, "struct", leafField);

    Schema schema = new Schema(structField);

    Record leafStruct1 = GenericRecord.create(leafField);
    leafStruct1.setField("string", "az");
    leafStruct1.setField("list", ImmutableList.of(3, 2, 1));

    Record leafStruct2 = GenericRecord.create(leafField);
    leafStruct2.setField("string", "aaaaa");
    leafStruct2.setField("list", ImmutableList.of(6, 5, 4));

    Record leafStruct3 = GenericRecord.create(leafField);
    leafStruct3.setField("string", "ab");
    leafStruct3.setField("list", ImmutableList.of(8, 9, 7));

    Metrics metrics = writeAndGetMetrics(schema,
        createRecord(schema, "struct", leafStruct1),
        createRecord(schema, "struct", leafStruct2),
        createRecord(schema, "struct", leafStruct3));

    assertNullBounds(1, metrics);
    assertBounds(2, Types.StringType.get(), CharBuffer.wrap("aaaaa"), CharBuffer.wrap("az"), metrics);
    assertNullBounds(3, metrics);
    assertBounds(4, Types.IntegerType.get(), 1, 9, metrics);
  }

  private ByteBuffer byteBuffer(String str) {
    return ByteBuffer.wrap(bytes(str));
  }

  private byte[] bytes(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  private Record createRecord(Types.NestedField field, Object value) {
    return createRecord(SINGLE_FIELD_SCHEMA_MAP.get(field), field.name(), value);
  }

  private Record createRecord(Schema schema, String name, Object value) {
    Record record = GenericRecord.create(schema);
    record.setField(name, value);
    return record;

  }

  private <T> void assertBounds(Types.NestedField field, T lowerBound, T upperBound, Metrics metrics) {
    assertBounds(field.fieldId(), field.type(), lowerBound, upperBound, metrics);
  }

  private <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();

    Assert.assertEquals(
        lowerBound,
        lowerBounds.containsKey(fieldId) ? Conversions.fromByteBuffer(type, lowerBounds.get(fieldId)) : null);
    Assert.assertEquals(
        upperBound,
        upperBounds.containsKey(fieldId) ? Conversions.fromByteBuffer(type, upperBounds.get(fieldId)) : null);
  }

  private void assertNullBounds(int fieldId, Metrics metrics) {
    Assert.assertNull(metrics.lowerBounds().get(fieldId));
    Assert.assertNull(metrics.upperBounds().get(fieldId));
  }
}
