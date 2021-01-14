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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
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
  protected static final Types.NestedField STRUCT_FIELD = optional(23, "structField", Types.StructType.of(
      required(24, "booleanField", Types.BooleanType.get()),
      optional(25, "date", Types.DateType.get()),
      optional(27, "timestamp", Types.TimestampType.withZone())
  ));

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
    return new Object[] {FileFormat.PARQUET, FileFormat.AVRO};
  }

  public TestMergingMetrics(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  protected abstract FileAppender<T> writeAndGetAppender(List<Record> records) throws Exception;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void verifySingleRecordMetric() throws Exception {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", 3);
    record.setField("float", Float.NaN); // FLOAT_FIELD - 1
    record.setField("double", Double.NaN); // DOUBLE_FIELD - 1
    record.setField("floatlist", ImmutableList.of(3.3F, 2.8F, Float.NaN, -25.1F, Float.NaN)); // FLOAT_LIST - 2
    record.setField("map1", ImmutableMap.of(Float.NaN, "a", 0F, "b")); // MAP_FIELD_1 - 1
    record.setField("map2", ImmutableMap.of(
        0, 0D, 1, Double.NaN, 2, 2D, 3, Double.NaN, 4, Double.NaN)); // MAP_FIELD_2 - 3

    FileAppender<T> appender = writeAndGetAppender(ImmutableList.of(record));
    Map<Integer, Long> nanValueCount = appender.metrics().nanValueCounts();

    assertNaNCountMatch(1L, nanValueCount, FLOAT_FIELD);
    assertNaNCountMatch(1L, nanValueCount, DOUBLE_FIELD);
    assertNaNCountMatch(2L, nanValueCount, FLOAT_LIST);
    assertNaNCountMatch(1L, nanValueCount, MAP_FIELD_1);
    assertNaNCountMatch(3L, nanValueCount, MAP_FIELD_2);
  }

  private void assertNaNCountMatch(Long expected, Map<Integer, Long> nanValueCount, Types.NestedField field) {
    Assert.assertEquals(
        String.format("NaN count for field %s does not match expected", field.name()),
        expected, nanValueCount.get(FIELDS_WITH_NAN_COUNT_TO_ID.get(field)));
  }

  @Test
  public void verifyRandomlyGeneratedRecordsMetric() throws Exception {
    List<Record> recordList = RandomGenericData.generate(SCHEMA, 50, 250L);

    FileAppender<T> appender = writeAndGetAppender(recordList);
    Map<Integer, Long> nanValueCount = appender.metrics().nanValueCounts();

    FIELDS_WITH_NAN_COUNT_TO_ID.forEach((key, value) -> Assert.assertEquals(
        String.format("NaN count for field %s does not match expected", key.name()),
        getExpectedNaNCount(recordList, key),
        nanValueCount.get(value)));

    SCHEMA.columns().stream()
        .filter(column -> !FIELDS_WITH_NAN_COUNT_TO_ID.containsKey(column))
        .map(Types.NestedField::fieldId)
        .forEach(id -> Assert.assertNull("NaN count for field %s should be null", nanValueCount.get(id)));
  }

  private Long getExpectedNaNCount(List<Record> expectedRecords, Types.NestedField field) {
    return expectedRecords.stream()
        .mapToLong(e -> {
          Object value = e.getField(field.name());
          if (value == null) {
            return 0;
          }
          if (FLOAT_FIELD.equals(field)) {
            return Float.isNaN((Float) value) ? 1 : 0;
          } else if  (DOUBLE_FIELD.equals(field)) {
            return Double.isNaN((Double) value) ? 1 : 0;
          } else if  (FLOAT_LIST.equals(field)) {
            return ((List<Float>) value).stream()
                .filter(val -> val != null && Float.isNaN(val))
                .count();
          } else if  (MAP_FIELD_1.equals(field)) {
            return ((Map<Float, ?>) value).keySet().stream()
                .filter(key -> Float.isNaN(key))
                .count();
          } else if  (MAP_FIELD_2.equals(field)) {
            return ((Map<?, Double>) value).values().stream()
                .filter(val -> val != null && Double.isNaN(val))
                .count();
          } else {
            throw new RuntimeException("unknown field name for getting expected NaN count: " + field.name());
          }
        }).sum();
  }
}
