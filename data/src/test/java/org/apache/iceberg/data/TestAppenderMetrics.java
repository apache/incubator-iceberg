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

package org.apache.iceberg.data;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsAppender;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Conversions.fromByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * TODO: build TestAvroMetrics similar to TestParquetMetrics.
 */
public class TestAppenderMetrics {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));


  private static final Types.StructType NESTED_SIMPLE_SCHEMA = Types.StructType.of(
      optional(7, "nested_int", Types.IntegerType.get())
  );

  private static final Types.StructType NESTED_LIST_TYPE =  Types.StructType.of(
      optional(70, "nested_int70", Types.IntegerType.get())
  );

  private static final Types.ListType LIST_TYPE = Types.ListType.ofRequired(8, NESTED_LIST_TYPE);

  private static final Types.MapType MAP_TYPE = Types.MapType.ofRequired(9, 10,
      Types.LongType.get(),
      Types.LongType.get());

  private static final Types.StructType LEAF_STRUCT_TYPE = Types.StructType.of(
      optional(5, "leafLongCol", Types.LongType.get()),
      optional(6, "leafBinaryCol", Types.BinaryType.get())
  );

  private static final Types.StructType NESTED_STRUCT_TYPE = Types.StructType.of(
      required(3, "longCol", Types.LongType.get()),
      optional(4, "leafStructCol", LEAF_STRUCT_TYPE)
  );

  private static final Schema NESTED_SCHEMA = new Schema(
      required(100, "id", Types.IntegerType.get()),
      required(200, "nestedStructCol", NESTED_STRUCT_TYPE),
      optional(700, "list", LIST_TYPE),
      optional(1100, "map", MAP_TYPE)
  );

  private static final Schema SIMPLE_SCHEMA = new Schema(
      optional(100, "id", Types.IntegerType.get()),
      optional(400, "long", Types.LongType.get()),
      optional(200, "nested", NESTED_SIMPLE_SCHEMA),
      optional(500, "list", LIST_TYPE),
      optional(600, "map", MAP_TYPE)
  );


  private static final Schema SO_SIMPLE_SCHEMA = new Schema(
      optional(1, "booleanCol", Types.BooleanType.get()),
      required(2, "intCol", Types.IntegerType.get()),
      optional(3, "longCol", Types.LongType.get()),
      required(4, "floatCol", Types.FloatType.get()),
      optional(5, "doubleCol", Types.DoubleType.get()),
      optional(6, "decimalCol", Types.DecimalType.of(10, 2)),
      required(7, "stringCol", Types.StringType.get()),
      optional(8, "dateCol", Types.DateType.get()),
      required(9, "timeCol", Types.TimeType.get()),
      //required(10, "timestampCol", Types.TimestampType.withoutZone()),
      optional(11, "uuidCol", Types.UUIDType.get())
      //required(12, "fixedCol", Types.FixedType.ofLength(4)),
      //required(13, "binaryCol", Types.BinaryType.get())
  );

  private List<Record> file1Records = null;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testA() {

  }

  @Test
  public void testMetricsSimple() throws IOException {
    Record record = GenericRecord.create(SIMPLE_SCHEMA);
    Record nested = GenericRecord.create(NESTED_SIMPLE_SCHEMA);

    this.file1Records = new ArrayList<>();

    file1Records.add(record.copy(ImmutableMap.of("id", 1)));
    file1Records.add(record.copy(ImmutableMap.of("id", 1)));
    file1Records.add(record.copy(ImmutableMap.of("id", 1, "long", 4L)));
    file1Records.add(record.copy(ImmutableMap.of("id", 1, "nested", nested.copy(ImmutableMap.of()))));

    Metrics metrics = writeFile(SIMPLE_SCHEMA, file1Records);

    System.out.println("record count: " + metrics.recordCount());
    System.out.println("value counts: " + metrics.valueCounts());
    System.out.println("null values count:" + metrics.nullValueCounts());
  }

  @Test
  public void testMetrics() throws IOException {
    Record record = GenericRecord.create(NESTED_SCHEMA);
    Record record2 = GenericRecord.create(NESTED_STRUCT_TYPE);
    Record record3 = GenericRecord.create(LEAF_STRUCT_TYPE);
    Record record4 = GenericRecord.create(NESTED_LIST_TYPE);

    this.file1Records = new ArrayList<>();

    file1Records.add(record.copy(ImmutableMap.of("id", 1,
        "nestedStructCol",
        record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol",
            record3.copy(ImmutableMap.of("leafLongCol", 2L,
                "leafBinaryCol", ByteBuffer.wrap(new byte[] {0, 1, 2}))))))));
    file1Records.add(record.copy(ImmutableMap.of("id", 2,
        "nestedStructCol",
        record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol",
            record3.copy(ImmutableMap.of("leafLongCol", 2L,
                "leafBinaryCol", ByteBuffer.wrap(new byte[] {3, 1, 2}))))))));
    file1Records.add(record.copy(ImmutableMap.of("id", 3,
        "nestedStructCol",
        record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol", record3.copy())))));
    file1Records.add(record.copy(ImmutableMap.of("id", 4,
        "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L)))));
    file1Records.add(record.copy(ImmutableMap.of("id", 4,
        "list", ImmutableList.of(record4.copy(ImmutableMap.of("nested_int70", 56)),
            record4.copy(ImmutableMap.of("nested_int70", 57)),
            record4.copy(ImmutableMap.of("nested_int70", 58))),
        "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L)))));

    Metrics metrics = writeFile(NESTED_SCHEMA, file1Records);

    Long recordCount = metrics.recordCount();
    System.out.println("Record count:" + recordCount);

    //Map<Integer, Long> integerLongMap = m.columnSizes(); DISCARDED
    //Map<Integer, ByteBuffer> integerByteBufferMap = m.lowerBounds();
    //Map<Integer, ByteBuffer> integerByteBufferMap1 = m.upperBounds();

    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    System.out.println("Lower bounds");
    for (Integer i : lowerBounds.keySet()) {
      System.out.println(i);
    }

    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();
    System.out.println("Upper bounds");
    for (Integer i : upperBounds.keySet()) {
      System.out.println(i);
    }

    Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
    Map<Integer, Long> valueCounts = metrics.valueCounts();

    // solo se genera sobre los campos sencillos
    System.out.println("Null value counts");
    Set<Integer> integers = nullValueCounts.keySet();
    for (Integer i : integers) {
      System.out.println(i + "->" + nullValueCounts.get(i));
    }

    System.out.println("value counts");
    integers = valueCounts.keySet();
    for (Integer i : integers) {
      System.out.println(i + "->" + valueCounts.get(i));
    }
  }

  @Test
  public void testMetricsForRepeatedValues() throws IOException {
    GenericRecord firstRecord = GenericRecord.create(SO_SIMPLE_SCHEMA);

    firstRecord.setField("booleanCol", true);
    firstRecord.setField("intCol", 3);
    firstRecord.setField("longCol", null);
    firstRecord.setField("floatCol", 2.0F);
    firstRecord.setField("doubleCol", 2.0D);
    firstRecord.setField("decimalCol", new BigDecimal("3.50"));
    firstRecord.setField("stringCol", "AAA");
    firstRecord.setField("dateCol", LocalDate.now()); // java.lang.Integer cannot be cast
    firstRecord.setField("timeCol", LocalTime.now()); // Long cannot be cast
    //firstRecord.setField("timestampCol", 0L); // Long cannot be cast
    firstRecord.setField("uuidCol", UUID.randomUUID());
    //firstRecord.setField("fixedCol", fixed); // test another
    //firstRecord.setField("binaryCol", "S".getBytes()); // byte array cannot be cast

    GenericRecord secondRecord = GenericRecord.create(SO_SIMPLE_SCHEMA);
    secondRecord.setField("booleanCol", true);
    secondRecord.setField("intCol", 3);
    secondRecord.setField("longCol", null);
    secondRecord.setField("floatCol", 2.0F);
    secondRecord.setField("doubleCol", 2.0D);
    secondRecord.setField("decimalCol", new BigDecimal("3.50"));
    secondRecord.setField("stringCol", "AAA");
    secondRecord.setField("dateCol", LocalDate.now());
    secondRecord.setField("timeCol", LocalTime.now());
    //secondRecord.setField("timestampCol", 0L);
    secondRecord.setField("uuidCol", UUID.randomUUID());
    //secondRecord.setField("fixedCol", fixed);
    //secondRecord.setField("binaryCol", "S".getBytes());

    Metrics metrics = writeFile(SO_SIMPLE_SCHEMA, Arrays.asList(firstRecord, secondRecord));

    /* TODO adjust values
    Assert.assertEquals(2L, (long) metrics.recordCount());
    assertCounts(1, 2L, 0L, metrics);
    assertCounts(2, 2L, 0L, metrics);
    assertCounts(3, 2L, 2L, metrics);
    assertCounts(4, 2L, 0L, metrics);
    assertCounts(5, 2L, 0L, metrics);
    assertCounts(6, 2L, 0L, metrics);
    assertCounts(7, 2L, 0L, metrics);
    assertCounts(8, 2L, 0L, metrics);
    assertCounts(9, 2L, 0L, metrics);
    assertCounts(10, 2L, 0L, metrics);
    assertCounts(11, 2L, 0L, metrics);
    assertCounts(12, 2L, 0L, metrics);
    assertCounts(13, 2L, 0L, metrics);
    */
  }

  public Metrics writeFile(Schema schema, List<Record> records) throws IOException {
    FileAppender avroAppender = Avro.write(Files.localOutput(temp.newFile()))
        .schema(schema)
        .createWriterFunc(DataWriter::create)
        .overwrite(true)
        .build();

    MetricsAppender metricsAppender = new MetricsAppender.Builder<>(avroAppender, schema)
        .useByteBufferForFixedType()
        .useDateTimeForTimestampType()

        .useLocalDateForDateType()
        .build();

    try {
      metricsAppender.addAll(records);
    } finally {
      metricsAppender.close();
    }

    return metricsAppender.metrics();
  }

  private void assertCounts(int fieldId, long valueCount, long nullValueCount, Metrics metrics) {
    Map<Integer, Long> valueCounts = metrics.valueCounts();
    Map<Integer, Long> nullValueCounts = metrics.nullValueCounts();
    Assert.assertEquals(valueCount, (long) valueCounts.get(fieldId));
    Assert.assertEquals(nullValueCount, (long) nullValueCounts.get(fieldId));
  }

  private <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    Map<Integer, ByteBuffer> lowerBounds = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = metrics.upperBounds();

    Assert.assertEquals(
        lowerBound,
        lowerBounds.containsKey(fieldId) ? fromByteBuffer(type, lowerBounds.get(fieldId)) : null);
    Assert.assertEquals(
        upperBound,
        upperBounds.containsKey(fieldId) ? fromByteBuffer(type, upperBounds.get(fieldId)) : null);
  }

}
