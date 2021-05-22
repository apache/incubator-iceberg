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

package org.apache.iceberg.parquet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_ENABLED;
import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestBloomRowGroupFilter {

  private static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(16, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      required(2, "id_long", LongType.get()),
      required(3, "id_double", DoubleType.get()),
      required(4, "id_float", FloatType.get()),
      required(5, "id_binary", StringType.get()),
      required(6, "random_binary", StringType.get()),
      required(7, "required", StringType.get()),
      optional(8, "non_bloom", StringType.get()),
      optional(9, "all_nulls", LongType.get()),
      optional(10, "some_nulls", StringType.get()),
      optional(11, "no_nulls", StringType.get()),
      optional(12, "all_nans", DoubleType.get()),
      optional(13, "some_nans", FloatType.get()),
      optional(14, "no_nans", DoubleType.get()),
      optional(15, "struct_not_null", structFieldType),
      optional(17, "not_in_file", FloatType.get()),
      optional(18, "no_stats", StringType.get())
  );

  private static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(16, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      required(2, "_id_long", LongType.get()),
      required(3, "_id_double", DoubleType.get()),
      required(4, "_id_float", FloatType.get()),
      required(5, "_id_binary", StringType.get()),
      required(6, "_random_binary", StringType.get()),
      required(7, "_required", StringType.get()),
      required(8, "_non_bloom", StringType.get()),
      optional(9, "_all_nulls", LongType.get()),
      optional(10, "_some_nulls", StringType.get()),
      optional(11, "_no_nulls", StringType.get()),
      optional(12, "_all_nans", DoubleType.get()),
      optional(13, "_some_nans", FloatType.get()),
      optional(14, "_no_nans", DoubleType.get()),
      optional(15, "_struct_not_null", _structFieldType),
      optional(18, "_no_stats", StringType.get())
  );

  private static final String TOO_LONG_FOR_STATS;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS = sb.toString();
  }

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;
  private static final int INT_VALUE_COUNT = INT_MAX_VALUE - INT_MIN_VALUE + 1;
  private static final long LONG_BASE = 100L;
  private static final double DOUBLE_BASE = 1000D;
  private static final float FLOAT_BASE = 10000F;
  private static final String BINARY_PREFIX = "BINARY测试_";

  private static final List<String> RANDOM_UUIDS;

  static {
    RANDOM_UUIDS = new ArrayList<>();
    for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
      RANDOM_UUIDS.add(UUID.randomUUID().toString());
    }
  }

  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;
  private BloomFilterReader bloomStore = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createInputFile() throws IOException {
    File parquetFile = temp.newFile();
    Assert.assertTrue(parquetFile.delete());

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .set(PARQUET_BLOOM_FILTER_ENABLED, "true")
        .set(PARQUET_BLOOM_FILTER_ENABLED + "#_non_bloom", "false")
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set("_id_long", LONG_BASE + INT_MIN_VALUE + i); // min=130L, max=179L, num-nulls=0
        builder.set("_id_double", DOUBLE_BASE + INT_MIN_VALUE + i); // min=1030D, max=1079D, num-nulls=0
        builder.set("_id_float", FLOAT_BASE + INT_MIN_VALUE + i); // min=10030F, max=10079F, num-nulls=0
        builder.set("_id_binary", BINARY_PREFIX + (INT_MIN_VALUE + i)); // min=BINARY测试_30, max=BINARY测试_79, num-nulls=0
        builder.set("_random_binary", RANDOM_UUIDS.get(i)); // required, random uuid, always non-null
        builder.set("_required", "req"); // required, always non-null
        builder.set("_non_bloom", RANDOM_UUIDS.get(i)); // bloom filter not enabled
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan

        Record structNotNull = new Record(structSchema);
        structNotNull.put("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        builder.set("_no_stats", TOO_LONG_FOR_STATS); // value longer than 4k will produce no stats

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);

    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile));

    Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
    rowGroupMetadata = reader.getRowGroups().get(0);
    parquetSchema = reader.getFileMetaData().getSchema();
    bloomStore = reader.getBloomFilterDataReader(rowGroupMetadata);
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNull("all_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNull("struct_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNull("all_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNull("struct_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testIsNaNs() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNaN("all_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNaN("some_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, isNaN("no_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testNotNaNs() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNaN("all_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNaN("some_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notNaN("no_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStartsWith() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("non_bloom", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: no bloom", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "req"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("some_nulls", "so"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("required", "reqs"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("some_nulls", "somex"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, startsWith("no_nulls", "xxx"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new ParquetBloomRowGroupFilter(SCHEMA, lessThan("missing", 5))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore));
  }

  @Test
  public void testColumnNotInFile() {
    Expression[] exprs = new Expression[] {
        lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
        equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
        greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file"),
        isNull("not_in_file"), notEqual("not_in_file", 1.0f)
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testColumnFallbackOrNotBloomFilterEnabled() {
    Expression[] exprs = new Expression[] {
        lessThan("non_bloom", "a"), lessThanOrEqual("non_bloom", "a"), equal("non_bloom", "a"),
        greaterThan("non_bloom", "a"), greaterThanOrEqual("non_bloom", "a"), notNull("non_bloom"),
        isNull("non_bloom"), notEqual("non_bloom", "a")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
      Assert.assertTrue("Should read: bloom filter cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStats() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("no_stats", "a"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: stats are missing but bloom filter is present", shouldRead);
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify it out
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE - 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: not(equal()) is rewritten as notEq()", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE + 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: not(equal()) is rewritten as notEq()", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(lessThan("id", INT_MIN_VALUE + 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: not(lessThan()) is rewritten as greaterThanOrEqual()", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(greaterThan("id", INT_MIN_VALUE + 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: not(greaterThan()) is rewritten as lessThanOrEqual()", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify it out
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, and(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MIN_VALUE + 30)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, and(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE + 1)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, and(equal("id", INT_MIN_VALUE + 25), equal("id", INT_MIN_VALUE)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), otherwise binding will simplify it out
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, or(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE + 1)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, or(equal("id", INT_MIN_VALUE - 25), equal("id", INT_MAX_VALUE - 19)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testLongEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_long below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_long below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_long equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_long between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_long equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_long above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_long above upper bound", shouldRead);
  }

  @Test
  public void testDoubleEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_double below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_double below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_double equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_double between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_double equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_double above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_double", DOUBLE_BASE + INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_double above upper bound", shouldRead);
  }

  @Test
  public void testFloatEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_float below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_float below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_float equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_float between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_float equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_float above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_float", FLOAT_BASE + INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_float above upper bound", shouldRead);
  }

  @Test
  public void testBinaryEq() {
    boolean shouldRead =
        new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + (INT_MIN_VALUE - 25)))
            .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_binary below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + (INT_MIN_VALUE - 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_binary below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_binary equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + (INT_MAX_VALUE - 4)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_binary between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id_binary equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + (INT_MAX_VALUE + 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_binary above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_binary", BINARY_PREFIX + (INT_MAX_VALUE + 6)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id_binary above upper bound", shouldRead);
  }

  @Test
  public void testRandomBinaryEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("random_binary", RANDOM_UUIDS.get(0)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should not read: random_binary selected from random_uuids", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("random_binary", RANDOM_UUIDS.get(15)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should not read: random_binary selected from random_uuids", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("random_binary", RANDOM_UUIDS.get(INT_VALUE_COUNT - 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should not read: random_binary selected from random_uuids", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("random_binary", UUID.randomUUID().toString()))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: cannot match a new generated random uuid", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE - 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE - 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE - 4)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE + 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE + 6)))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStringNotEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("no_nulls", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        equal("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("Required", "abc"), false)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should skip: contains only 'req'", shouldRead);
  }

  @Test
  public void testMissingBloomFilterForColumn() {
    TestHelpers.assertThrows("Should complain about missing bloom filter",
        IllegalStateException.class, "Failed to read required bloom filter for id: 10",
        () -> new ParquetBloomRowGroupFilter(SCHEMA, equal("some_nulls", "some"))
            .shouldRead(parquetSchema, rowGroupMetadata, new DummyBloomFilterReader(null, rowGroupMetadata)));
  }

  private static class DummyBloomFilterReader extends BloomFilterReader {
    DummyBloomFilterReader(ParquetFileReader fileReader, BlockMetaData block) {
      super(fileReader, block);
    }

    @Override
    public BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
      return null;
    }
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse(
        "Should not read: id below lower bound (5 < 30, 6 < 30). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse(
        "Should not read: id below lower bound (28 < 30, 29 < 30). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: in set is a subset of the dictionary", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse(
        "Should not read: id above upper bound (85 > 79, 86 > 79). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(
        SCHEMA,
        in("id", IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: the bloom is a subset of the in set", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(
        SCHEMA,
        in("id", IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE + 1).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: the bloom is equal to the in set", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("all_nulls", 1, 2))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: in on all nulls column (bloom is empty) ", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "some"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: some_nulls values are not within the set", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("no_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: in on no nulls column (empty string is not within the set)", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, in("no_nulls", "aaa", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: in on no nulls column (empty string is within the set)", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        notIn("id", IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA,
        notIn("id", IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE + 1).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("all_nulls", 1, 2))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("some_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: bloom filter doesn't help", shouldRead);
  }

  @Test
  public void testTypeConversions() {
    boolean shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MIN_VALUE + 1), true)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: Integer value promoted", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id_long", LONG_BASE + INT_MIN_VALUE - 1), true)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Integer value promoted", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", (long) (INT_MIN_VALUE + 1)), true)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertTrue("Should read: Long value truncated", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", (long) (INT_MIN_VALUE - 1)), true)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Long value truncated", shouldRead);

    shouldRead = new ParquetBloomRowGroupFilter(SCHEMA, equal("id", ((long) Integer.MAX_VALUE) + 1), true)
        .shouldRead(parquetSchema, rowGroupMetadata, bloomStore);
    Assert.assertFalse("Should not read: Long value out of Integer range", shouldRead);
  }
}
