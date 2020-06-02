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
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsAppender;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Test Metrics for Avro.
 */
public class AvroMetrics extends TestMetrics {

  static final ImmutableSet<Object> BINARY_TYPES = ImmutableSet.of(Type.TypeID.BINARY,
      Type.TypeID.FIXED, Type.TypeID.UUID);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  public FileFormat fileFormat() {
    return FileFormat.AVRO;
  }

  @Override
  protected Metrics writeRecords(Schema schema, Record... records) throws IOException {
    return writeFile(schema, Arrays.asList(records));
  }

  @Override
  public Metrics writeRecords(Schema schema, GenericData.Record... records) throws IOException {
    return writeFileGeneric(schema, Arrays.asList(records));
  }

  @Override
  public Metrics writeRecords(Schema schema, Map<String, String> properties,
                           GenericData.Record... records) throws IOException {
    return null; // TODO
  }

  private boolean isBinaryType(Type type) {
    return BINARY_TYPES.contains(type.typeId());
  }

  @Override
  protected <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    super.assertBounds(fieldId, type, lowerBound, upperBound, metrics);
  }

  @Test
  public void testMetricsForListAndMapElements() throws IOException {
    Types.StructType structType = Types.StructType.of(
        required(1, "leafIntCol", Types.IntegerType.get()),
        optional(2, "leafStringCol", Types.StringType.get())
    );
    Schema schema = new Schema(
        optional(3, "intListCol", Types.ListType.ofRequired(4, Types.IntegerType.get())),
        optional(5, "mapCol", Types.MapType.ofRequired(6, 7, Types.StringType.get(), structType))
    );

    GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(schema.asStruct()));
    record.put("intListCol", Lists.newArrayList(10, 11, 12));
    GenericData.Record struct = new GenericData.Record(AvroSchemaUtil.convert(structType));
    struct.put("leafIntCol", 1);
    struct.put("leafStringCol", "BBB");
    Map<String, GenericData.Record> map = Maps.newHashMap();
    map.put("4", struct);
    record.put(1, map);

    Metrics metrics = writeRecords(schema, record);

    Assert.assertEquals(1L, (long) metrics.recordCount());
    assertCounts(1, 1, 0, metrics);
    assertBounds(1, Types.IntegerType.get(), 1, 1, metrics);
    assertCounts(2, 1, 0, metrics);
    assertBounds(2, Types.StringType.get(),
        CharBuffer.wrap("BBB"), CharBuffer.wrap("BBB"), metrics);

    assertCounts(4, 3, 0, metrics);
    assertBounds(4, Types.IntegerType.get(), 10, 12, metrics);
    assertCounts(6, 1, 0, metrics);
    assertBounds(6, Types.StringType.get(), CharBuffer.wrap("4"), CharBuffer.wrap("4"), metrics);
    assertCounts(7, 1, 0, metrics);
    assertBounds(7, structType, null, null, metrics);
  }

  private Metrics writeFile(Schema schema, List<Record> records) throws IOException {
    FileAppender avroAppender = Avro.write(Files.localOutput(temp.newFile()))
        .schema(schema)
        .createWriterFunc(DataWriter::create)
        .overwrite(true)
        .build();

    MetricsAppender metricsAppender = new MetricsAppender.Builder<>(avroAppender, schema)
        .useByteArrayForFixedType()
        .useLocalDateTimeForTimestampType()
        .useLocalDateForDateType()
        .useLocalTimeForTimeType()
        .build();

    try {
      metricsAppender.addAll(records);
    } finally {
      metricsAppender.close();
    }

    return metricsAppender.metrics();
  }

  private Metrics writeFileGeneric(Schema schema, List<GenericData.Record> records) throws IOException {
    FileAppender avroAppender = Avro.write(Files.localOutput(temp.newFile()))
        .schema(schema)
        .overwrite(true)
        .build();

    MetricsAppender metricsAppender = new MetricsAppender.Builder<>(avroAppender, schema)
        .useByteArrayForFixedType()
        .useLocalDateTimeForTimestampType()
        .useLocalDateForDateType()
        .useLocalTimeForTimeType()
        .useGenericRecord()
        .build();

    try {
      metricsAppender.addAll(records);
    } finally {
      metricsAppender.close();
    }

    return metricsAppender.metrics();
  }

}
