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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public abstract class TestMergingMetrics<T> {

  // all supported fields, except for UUID which is on deprecation path: see https://github.com/apache/iceberg/pull/1611
  // as well as Types.TimeType and Types.TimestampType.withoutZone as both are not supported by Spark
  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());
  protected static final Types.NestedField FLOAT_FIELD = required(3, "float", Types.FloatType.get());
  protected static final Types.NestedField DOUBLE_FIELD = optional(4, "double", Types.DoubleType.get());
  protected static final Types.NestedField DECIMAL_FIELD = optional(5, "decimal", Types.DecimalType.of(5, 3));
  protected static final Types.NestedField FIXED_FIELD = optional(7, "fixed", Types.FixedType.ofLength(4));
  protected static final Types.NestedField BINARY_FIELD = optional(8, "binary", Types.BinaryType.get());
  protected static final Types.NestedField FLOAT_LIST = optional(9, "floatlist",
      Types.ListType.ofRequired(10, Types.FloatType.get()));
  protected static final Types.NestedField LONG_FIELD = optional(11, "long", Types.LongType.get());

  protected static final Types.NestedField MAP_FIELD_1 = optional(17, "map1",
      Types.MapType.ofOptional(18, 19, Types.FloatType.get(), Types.StringType.get())
  );
  protected static final Types.NestedField MAP_FIELD_2 = optional(20, "map2",
      Types.MapType.ofOptional(21, 22, Types.IntegerType.get(), Types.DoubleType.get())
  );

  protected static final Types.StructType NESTED_FIELDS = Types.StructType.of(
      required(24, "booleanField", Types.BooleanType.get()),
      optional(25, "date", Types.DateType.get()),
      optional(27, "timestamp", Types.TimestampType.withZone()));

  protected static final Types.NestedField STRUCT_FIELD = optional(23, "structField", NESTED_FIELDS);


  private static final Map<Types.NestedField, Integer> FIELDS_WITH_NAN_COUNT_TO_ID = ImmutableMap.of(
      FLOAT_FIELD, 3, DOUBLE_FIELD, 4, FLOAT_LIST, 10, MAP_FIELD_1, 18, MAP_FIELD_2, 22
  );

  // create a schema with all supported fields
  protected static final Schema SCHEMA = new Schema(
      ID_FIELD,
      DATA_FIELD,
      FLOAT_FIELD,
      DOUBLE_FIELD,
      DECIMAL_FIELD,
      FIXED_FIELD,
      BINARY_FIELD,
      FLOAT_LIST,
      LONG_FIELD,
      MAP_FIELD_1,
      MAP_FIELD_2,
      STRUCT_FIELD
  );

  protected final FileFormat fileFormat;

  @Parameterized.Parameters(name = "fileFormat = {0}")
  public static Object[] parameters() {
    return new Object[] { FileFormat.PARQUET, FileFormat.ORC };
  }

  public TestMergingMetrics(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  protected FileAppender<T> writeAndGetAppender(List<Record> records) throws Exception {
    return writeAndGetAppender(records, TestMetricUtil.createTestTable(null, null, SCHEMA));
  }

  protected abstract FileAppender<T> writeAndGetAppender(List<Record> records,
                                                         Table table) throws Exception;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void verifySingleRecordMetric() throws Exception {
    Record record = GenericRecord.create(SCHEMA);
    record.setField(ID_FIELD.name(), 3);
    record.setField(FLOAT_FIELD.name(), Float.NaN); // FLOAT_FIELD - 1
    record.setField(DOUBLE_FIELD.name(), Double.NaN); // DOUBLE_FIELD - 1
    record.setField(FLOAT_LIST.name(), ImmutableList.of(3.3F, 2.8F, Float.NaN, -25.1F, Float.NaN)); // FLOAT_LIST - 2
    record.setField(MAP_FIELD_1.name(), ImmutableMap.of(Float.NaN, "a", 0F, "b")); // MAP_FIELD_1 - 1
    record.setField(MAP_FIELD_2.name(), ImmutableMap.of(
        0, 0D, 1, Double.NaN, 2, 2D, 3, Double.NaN, 4, Double.NaN)); // MAP_FIELD_2 - 3

    FileAppender<T> appender = writeAndGetAppender(ImmutableList.of(record));
    Metrics metrics = appender.metrics();
    Map<Integer, Long> nanValueCount = metrics.nanValueCounts();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();

    assertNaNCountMatch(1L, nanValueCount, FLOAT_FIELD);
    assertNaNCountMatch(1L, nanValueCount, DOUBLE_FIELD);
    assertNaNCountMatch(2L, nanValueCount, FLOAT_LIST);
    assertNaNCountMatch(1L, nanValueCount, MAP_FIELD_1);
    assertNaNCountMatch(3L, nanValueCount, MAP_FIELD_2);

    assertBoundValueMatch(null, upperBounds, FLOAT_FIELD);
    assertBoundValueMatch(null, upperBounds, DOUBLE_FIELD);
    assertBoundValueMatch(3.3F, upperBounds, FLOAT_LIST);
    assertBoundValueMatch(0F, upperBounds, MAP_FIELD_1);
    assertBoundValueMatch(2D, upperBounds, MAP_FIELD_2);

    assertBoundValueMatch(null, lowerBounds, FLOAT_FIELD);
    assertBoundValueMatch(null, lowerBounds, DOUBLE_FIELD);
    assertBoundValueMatch(-25.1F, lowerBounds, FLOAT_LIST);
    assertBoundValueMatch(0F, lowerBounds, MAP_FIELD_1);
    assertBoundValueMatch(0D, lowerBounds, MAP_FIELD_2);
  }

  @Test
  public void verifyRandomlyGeneratedRecordsMetric() throws Exception {
    // too big of the record count will more likely to make all upper/lower bounds +/-infinity,
    // which makes the tests easier to pass
    List<Record> recordList = RandomGenericData.generate(SCHEMA, 5, 250L);
    FileAppender<T> appender = writeAndGetAppender(recordList);

    Map<Types.NestedField, AtomicReference<Number>> expectedUpperBounds = new HashMap<>();
    Map<Types.NestedField, AtomicReference<Number>> expectedLowerBounds = new HashMap<>();
    Map<Types.NestedField, AtomicLong> expectedNaNCount = new HashMap<>();

    populateExpectedValues(recordList, expectedUpperBounds, expectedLowerBounds, expectedNaNCount);

    Metrics metrics = appender.metrics();
    expectedUpperBounds.forEach((key, value) -> assertBoundValueMatch(value.get(), metrics.upperBounds(), key));
    expectedLowerBounds.forEach((key, value) -> assertBoundValueMatch(value.get(), metrics.lowerBounds(), key));
    expectedNaNCount.forEach((key, value) -> assertNaNCountMatch(value.get(), metrics.nanValueCounts(), key));

    SCHEMA.columns().stream()
        .filter(column -> !FIELDS_WITH_NAN_COUNT_TO_ID.containsKey(column))
        .map(Types.NestedField::fieldId)
        .forEach(id -> Assert.assertNull("NaN count for field %s should be null",
            metrics.nanValueCounts().get(id)));
  }

  private void assertNaNCountMatch(Long expected, Map<Integer, Long> nanValueCount, Types.NestedField field) {
    Assert.assertEquals(
        String.format("NaN count for field %s does not match expected", field.name()),
        expected, nanValueCount.get(FIELDS_WITH_NAN_COUNT_TO_ID.get(field)));
  }

  private void assertBoundValueMatch(Number expected, Map<Integer, ByteBuffer> boundMap, Types.NestedField field) {
    if (field.type().isNestedType() && fileFormat == FileFormat.ORC) {
      // we don't update floating column bounds values within ORC nested columns
      return;
    }

    int actualFieldId = FIELDS_WITH_NAN_COUNT_TO_ID.get(field);
    ByteBuffer byteBuffer = boundMap.get(actualFieldId);
    Type type = SCHEMA.findType(actualFieldId);
    Assert.assertEquals(String.format("Bound value for field %s must match", field.name()),
        expected, byteBuffer == null ? null : Conversions.fromByteBuffer(type, byteBuffer));
  }

  private void populateExpectedValues(List<Record> records,
                                      Map<Types.NestedField, AtomicReference<Number>> upperBounds,
                                      Map<Types.NestedField, AtomicReference<Number>> lowerBounds,
                                      Map<Types.NestedField, AtomicLong> expectedNaNCount) {
    for (Types.NestedField field : FIELDS_WITH_NAN_COUNT_TO_ID.keySet()) {
      expectedNaNCount.put(field, new AtomicLong(0));
    }

    for (Record record : records) {
      updateExpectedValuePerRecord(upperBounds, lowerBounds, expectedNaNCount,
          FLOAT_FIELD, (Float) record.getField(FLOAT_FIELD.name()));
      updateExpectedValuePerRecord(upperBounds, lowerBounds, expectedNaNCount,
          DOUBLE_FIELD, (Double) record.getField(DOUBLE_FIELD.name()));

      List<Float> floatList = (List<Float>) record.getField(FLOAT_LIST.name());
      if (floatList != null) {
        updateExpectedValueFromRecords(upperBounds, lowerBounds, expectedNaNCount, FLOAT_LIST, floatList);
      }

      Map<Float, ?> map1 = (Map<Float, ?>) record.getField(MAP_FIELD_1.name());
      if (map1 != null) {
        updateExpectedValueFromRecords(upperBounds, lowerBounds, expectedNaNCount, MAP_FIELD_1, map1.keySet());
      }

      Map<?, Double> map2 = (Map<?, Double>) record.getField(MAP_FIELD_2.name());
      if (map2 != null) {
        updateExpectedValueFromRecords(upperBounds, lowerBounds, expectedNaNCount, MAP_FIELD_2, map2.values());
      }
    }
  }

  private <T1 extends Number> void updateExpectedValueFromRecords(
      Map<Types.NestedField, AtomicReference<Number>> upperBounds,
      Map<Types.NestedField, AtomicReference<Number>> lowerBounds,
      Map<Types.NestedField, AtomicLong> expectedNaNCount,
      Types.NestedField key, Collection<T1> vals) {
    List<Number> nonNullNumbers = vals.stream().filter(v -> !NaNUtil.isNaN(v)).collect(Collectors.toList());
    Optional<Number> maxOptional = nonNullNumbers.stream().filter(Objects::nonNull)
        .reduce((v1, v2) -> getMinOrMax(v1, v2, true));
    Optional<Number> minOptional = nonNullNumbers.stream().filter(Objects::nonNull)
        .reduce((v1, v2) -> getMinOrMax(v1, v2, false));

    expectedNaNCount.get(key).addAndGet(vals.size() - nonNullNumbers.size());
    maxOptional.ifPresent(max -> updateBound(key, max, upperBounds, true));
    minOptional.ifPresent(min -> updateBound(key, min, lowerBounds, false));
  }

  private void updateExpectedValuePerRecord(Map<Types.NestedField, AtomicReference<Number>> upperBounds,
                                            Map<Types.NestedField, AtomicReference<Number>> lowerBounds,
                                            Map<Types.NestedField, AtomicLong> expectedNaNCount,
                                            Types.NestedField key, Number val) {
    if (NaNUtil.isNaN(val)) {
      expectedNaNCount.get(key).incrementAndGet();
    } else if (val != null) {
      updateBound(key, val, upperBounds, true);
      updateBound(key, val, lowerBounds, false);
    }
  }

  private void updateBound(
      Types.NestedField key, Number val, Map<Types.NestedField, AtomicReference<Number>> bounds, boolean isMax) {
    bounds.computeIfAbsent(key, k -> new AtomicReference<>(val)).updateAndGet(old -> getMinOrMax(old, val, isMax));
  }

  private Number getMinOrMax(Number val1, Number val2, boolean isMax) {
    if (val1 instanceof Double) {
      return isMax ? Double.max((Double) val1, (Double) val2) : Double.min((Double) val1, (Double) val2);
    } else {
      return isMax ? Float.max((Float) val1, (Float) val2) : Float.min((Float) val1, (Float) val2);
    }
  }

  @Test
  public void verifySortedColMetric() throws Exception {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", 3);
    record.setField("float", Float.NaN); // FLOAT_FIELD - 1
    record.setField("double", Double.NaN); // DOUBLE_FIELD - 1
    record.setField("floatlist", ImmutableList.of(3.3F, 2.8F, Float.NaN, -25.1F, Float.NaN)); // FLOAT_LIST - 2
    record.setField("map1", ImmutableMap.of(Float.NaN, "a", 0F, "b")); // MAP_FIELD_1 - 1
    record.setField("map2", ImmutableMap.of(
        0, 0D, 1, Double.NaN, 2, 2D, 3, Double.NaN, 4, Double.NaN)); // MAP_FIELD_2 - 3

    Record nestedStruct = GenericRecord.create(NESTED_FIELDS);
    nestedStruct.setField("booleanField", true);
    nestedStruct.setField("date", DateTimeUtil.dateFromDays(1500));
    nestedStruct.setField("timestamp", null);
    record.setField("structField", nestedStruct);

    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").asc("structField.date").build();
    FileAppender<T> appender = writeAndGetAppender(ImmutableList.of(record),
        TestMetricUtil.createTestTable(properties, sortOrder, SCHEMA));

    // Only two sorted fields (id, structField.date) will have metrics
    Map<Integer, ByteBuffer> lowerBounds = appender.metrics().lowerBounds();
    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertFalse(lowerBounds.containsKey(2));
    Assert.assertFalse(lowerBounds.containsKey(3));
    Assert.assertFalse(lowerBounds.containsKey(4));
    Assert.assertFalse(lowerBounds.containsKey(5));
    Assert.assertFalse(lowerBounds.containsKey(6));
    Assert.assertFalse(lowerBounds.containsKey(7));
    Assert.assertFalse(lowerBounds.containsKey(8));
    Assert.assertFalse(lowerBounds.containsKey(9));
    Assert.assertFalse(lowerBounds.containsKey(11));
    Assert.assertFalse(lowerBounds.containsKey(24));
    Assert.assertEquals(1500, (int) Conversions.fromByteBuffer(Types.DateType.get(), lowerBounds.get(25)));
    Assert.assertFalse(lowerBounds.containsKey(27));

    Map<Integer, ByteBuffer> upperBounds = appender.metrics().upperBounds();
    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertFalse(upperBounds.containsKey(3));
    Assert.assertFalse(upperBounds.containsKey(4));
    Assert.assertFalse(upperBounds.containsKey(5));
    Assert.assertFalse(upperBounds.containsKey(6));
    Assert.assertFalse(upperBounds.containsKey(7));
    Assert.assertFalse(upperBounds.containsKey(8));
    Assert.assertFalse(upperBounds.containsKey(9));
    Assert.assertFalse(upperBounds.containsKey(11));
    Assert.assertFalse(upperBounds.containsKey(24));
    Assert.assertEquals(1500, (int) Conversions.fromByteBuffer(Types.DateType.get(), upperBounds.get(25)));
    Assert.assertFalse(upperBounds.containsKey(27));
  }
}
