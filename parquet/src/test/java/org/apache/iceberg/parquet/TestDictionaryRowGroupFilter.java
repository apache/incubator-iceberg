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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
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
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@RunWith(Parameterized.class)
public class TestDictionaryRowGroupFilter {

  private static final Types.StructType structFieldType =
          Types.StructType.of(Types.NestedField.required(9, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", StringType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", LongType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      optional(7, "non_dict", StringType.get()),
      optional(8, "struct_not_null", structFieldType),
      optional(10, "not_in_file", FloatType.get()),
      optional(11, "all_nans", DoubleType.get()),
      optional(12, "some_nans", FloatType.get()),
      optional(13, "no_nans", DoubleType.get()),
      optional(14, "decimal_fixed", DecimalType.of(20, 10)) // >18 precision to enforce FIXED_LEN_BYTE_ARRAY
  );

  private static final Types.StructType _structFieldType =
          Types.StructType.of(Types.NestedField.required(9, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      optional(2, "_no_stats", StringType.get()),
      required(3, "_required", StringType.get()),
      optional(4, "_all_nulls", LongType.get()),
      optional(5, "_some_nulls", StringType.get()),
      optional(6, "_no_nulls", StringType.get()),
      optional(7, "_non_dict", StringType.get()),
      optional(8, "_struct_not_null", _structFieldType),
      optional(11, "_all_nans", DoubleType.get()),
      optional(12, "_some_nans", FloatType.get()),
      optional(13, "_no_nans", DoubleType.get()),
      optional(14, "_decimal_fixed", DecimalType.of(20, 10)) // >18 precision to enforce FIXED_LEN_BYTE_ARRAY
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
  private static final BigDecimal DECIMAL_MIN_VALUE = new BigDecimal("-1234567890.0987654321");
  private static final BigDecimal DECIMAL_STEP = new BigDecimal("1234567890.0987654321").subtract(DECIMAL_MIN_VALUE)
      .divide(new BigDecimal(INT_MAX_VALUE - INT_MIN_VALUE), RoundingMode.HALF_UP);

  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;
  private DictionaryPageReadStore dictionaryStore = null;
  private final WriterVersion writerVersion;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static List<WriterVersion> writerVersions() {
    return Arrays.asList(PARQUET_1_0, PARQUET_2_0);
  }

  public TestDictionaryRowGroupFilter(WriterVersion writerVersion) {
    this.writerVersion = writerVersion;
  }

  @Before
  public void createInputFile() throws IOException {
    File parquetFile = temp.newFile();
    Assert.assertTrue(parquetFile.delete());

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .withWriterVersion(writerVersion)
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 20 copies of each record to ensure dictionary-encoding
      for (int copy = 0; copy < 20; copy += 1) {
        // create 50 records
        for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
          builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
          builder.set("_no_stats", TOO_LONG_FOR_STATS); // value longer than 4k will produce no stats
          builder.set("_required", "req"); // required, always non-null
          builder.set("_all_nulls", null); // never non-null
          builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
          builder.set("_no_nulls", ""); // optional, but always non-null
          builder.set("_non_dict", UUID.randomUUID().toString()); // not dictionary-encoded
          builder.set("_all_nans", Double.NaN); // never non-nan
          builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
          builder.set("_no_nans", 3D); // optional, but always non-nan

          // min=-1234567890.0987654321, max~=1234567890.0987654321 (depending on rounding), num-nulls=0
          builder.set("_decimal_fixed", DECIMAL_MIN_VALUE.add(DECIMAL_STEP.multiply(new BigDecimal(i))));

          Record structNotNull = new Record(structSchema);
          structNotNull.put("_int_field", INT_MIN_VALUE + i);
          builder.set("_struct_not_null", structNotNull); // struct with int

          appender.add(builder.build());
        }
      }
    }

    InputFile inFile = Files.localInput(parquetFile);

    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile));

    Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
    rowGroupMetadata = reader.getRowGroups().get(0);
    parquetSchema = reader.getFileMetaData().getSchema();
    dictionaryStore = reader.getNextDictionaryReader();
  }

  @Test
  public void testAssumptions() {
    // this case validates that other cases don't need to test expressions with null literals.
    TestHelpers.assertThrows("Should reject null literal in equal expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> equal("col", null));
    TestHelpers.assertThrows("Should reject null literal in notEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in lessThan expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> lessThan("col", null));
    TestHelpers.assertThrows("Should reject null literal in lessThanOrEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> lessThanOrEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in greaterThan expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> greaterThan("col", null));
    TestHelpers.assertThrows("Should reject null literal in greaterThanOrEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> greaterThanOrEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in startsWith expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> startsWith("col", null));
    TestHelpers.assertThrows("Should reject null literal in notStartsWith expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notStartsWith("col", null));
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("all_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("struct_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("all_nulls"))
           .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("struct_not_null"))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testIsNaNs() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNaN("all_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: all_nans column will contain NaN", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNaN("some_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: some_nans column will contain NaN", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNaN("no_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no_nans column will not contain NaN", shouldRead);
  }

  @Test
  public void testNotNaNs() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNaN("all_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: all_nans column will not contain non-NaN", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNaN("some_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: some_nans column will contain non-NaN", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNaN("no_nans"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: no_nans column will contain non-NaN", shouldRead);
  }

  @Test
  public void testStartsWith() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("non_dict", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: no dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "req"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("some_nulls", "so"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("no_stats", UUID.randomUUID().toString()))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no stats but dictionary is present", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "reqs"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("some_nulls", "somex"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("no_nulls", "xxx"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);
  }

  @Test
  public void testNotStartsWith() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("non_dict", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: no dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("required", "re"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("required", "req"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("some_nulls", "s!"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("no_stats", UUID.randomUUID().toString()))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: no stats but dictionary is present", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("required", "reqs"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("some_nulls", "somex"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("some_nulls", "some"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notStartsWith("no_nulls", "xxx"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);
  }


  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("missing", 5))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore));
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
      boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
      Assert.assertTrue("Should read: dictionary cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testColumnFallbackOrNotDictionaryEncoded() {
    Expression[] exprs = new Expression[] {
        lessThan("non_dict", "a"), lessThanOrEqual("non_dict", "a"), equal("non_dict", "a"),
        greaterThan("non_dict", "a"), greaterThanOrEqual("non_dict", "a"), notNull("non_dict"),
        isNull("non_dict"), notEqual("non_dict", "a")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
      Assert.assertTrue("Should read: dictionary cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStats() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("no_stats", "a"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: stats are missing but dictionary is present", shouldRead);
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", INT_MIN_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", INT_MAX_VALUE - 4))
          .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE - 25))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE - 25)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE - 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MIN_VALUE)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE - 4)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE + 1)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", INT_MAX_VALUE + 6)))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testStringNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: contains null != 'some'", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("no_nulls", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: contains only ''", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MIN_VALUE))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", INT_MAX_VALUE))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: many possible ids", shouldRead);

  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        equal("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MIN_VALUE - 1))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MIN_VALUE))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE - 4))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE + 1))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", INT_MAX_VALUE + 6))
            .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("no_Nulls", ""), false)
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should skip: contains only ''", shouldRead);
  }

  @Test
  public void testMissingDictionaryPageForColumn() {
    TestHelpers.assertThrows("Should complain about missing dictionary",
        IllegalStateException.class, "Failed to read required dictionary page for id: 5",
        () -> new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
            .shouldRead(parquetSchema, rowGroupMetadata, descriptor -> null));
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: in set is a subset of the dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79). The two sets are disjoint.",
        shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        in("id", IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: the dictionary is a subset of the in set", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        in("id", IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE + 1).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: the dictionary is equal to the in set", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("all_nulls", 1, 2))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: in on all nulls column (isFallback to be true) ", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "some"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("some_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: some_nulls values are not within the set", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("no_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: in on no nulls column (empty string is not within the set)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, in("no_nulls", "aaa", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: in on no nulls column (empty string is within the set)", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound (5 < 30, 6 < 30). The two sets are disjoint.", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id below lower bound (28 < 30, 29 < 30). The two sets are disjoint.", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: the notIn set is a subset of the dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound (80 > 79, 81 > 79). The two sets are disjoint.", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: id above upper bound (85 > 79, 86 > 79). The two sets are disjoint.", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        notIn("id", IntStream.range(INT_MIN_VALUE - 10, INT_MAX_VALUE + 10).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: the dictionary is a subset of the notIn set", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        notIn("id", IntStream.range(INT_MIN_VALUE, INT_MAX_VALUE + 1).boxed().collect(Collectors.toList()))
    ).shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: the dictionary is equal to the notIn set", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("all_nulls", 1, 2))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: notIn on all nulls column", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("some_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: notIn on some nulls column (any null matches the notIn)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", "bbb"))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should read: notIn on no nulls column (empty string is not within the set)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notIn("no_nulls", "aaa", ""))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: notIn on no nulls column (empty string is within the set)", shouldRead);
  }

  @Test
  public void testTypePromotion() {
    Schema promotedSchema = new Schema(required(1, "id", LongType.get()));
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(promotedSchema, equal("id", INT_MIN_VALUE + 1), true)
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertTrue("Should succeed with promoted schema", shouldRead);
  }

  @Test
  public void testFixedLenByteArray() {
    // This test is to validate the handling of FIXED_LEN_BYTE_ARRAY Parquet type being dictionary encoded.
    // (No need to validate all the possible predicates)

    Assume.assumeTrue("decimal_fixed is not dictionary encoded in case of writer version " + writerVersion,
        getColumnForName(rowGroupMetadata, "_decimal_fixed").getEncodings().contains(Encoding.RLE_DICTIONARY));

    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("decimal_fixed", BigDecimal.ZERO)).shouldRead(parquetSchema, rowGroupMetadata,
            dictionaryStore);
    Assert.assertTrue("Should read: Half of the decimal_fixed values are greater than 0", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("decimal_fixed", DECIMAL_MIN_VALUE))
        .shouldRead(parquetSchema, rowGroupMetadata, dictionaryStore);
    Assert.assertFalse("Should not read: No decimal_fixed values less than -1234567890.0987654321", shouldRead);
  }

  private ColumnChunkMetaData getColumnForName(BlockMetaData rowGroup, String columnName) {
    ColumnPath columnPath = ColumnPath.fromDotString(columnName);
    for (ColumnChunkMetaData column : rowGroup.getColumns()) {
      if (columnPath.equals(column.getPath())) {
        return column;
      }
    }
    throw new NoSuchElementException("No column in rowGroup for the name " + columnName);
  }
}
