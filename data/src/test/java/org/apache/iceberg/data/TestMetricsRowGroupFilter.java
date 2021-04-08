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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

@RunWith(Parameterized.class)
public class TestMetricsRowGroupFilter {

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[] parameters() {
    return new Object[] { "parquet", "orc" };
  }

  private final FileFormat format;

  public TestMetricsRowGroupFilter(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  private static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats_parquet", StringType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", DoubleType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      optional(7, "struct_not_null", structFieldType),
      optional(9, "not_in_file", FloatType.get()),
      optional(10, "str", StringType.get()),
      optional(11, "map_not_null",
          Types.MapType.ofRequired(12, 13, StringType.get(), IntegerType.get())),
      optional(14, "all_nans", DoubleType.get()),
      optional(15, "some_nans", FloatType.get()),
      optional(16, "no_nans", DoubleType.get())
  );

  private static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      optional(2, "_no_stats_parquet", StringType.get()),
      required(3, "_required", StringType.get()),
      optional(4, "_all_nulls", DoubleType.get()),
      optional(5, "_some_nulls", StringType.get()),
      optional(6, "_no_nulls", StringType.get()),
      optional(7, "_struct_not_null", _structFieldType),
      optional(10, "_str", StringType.get()),
      optional(14, "_all_nans", Types.DoubleType.get()),
      optional(15, "_some_nans", FloatType.get()),
      optional(16, "_no_nans", Types.DoubleType.get())
  );

  private static final String TOO_LONG_FOR_STATS_PARQUET;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS_PARQUET = sb.toString();
  }

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  private File orcFile = null;
  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createInputFile() throws IOException {
    switch (format) {
      case ORC:
        createOrcInputFile();
        break;
      case PARQUET:
        createParquetInputFile();
        break;
      default:
        throw new UnsupportedOperationException("Row group filter tests not supported for " + format);
    }
  }

  public void createOrcInputFile() throws IOException {
    this.orcFile = temp.newFile();
    Assert.assertTrue(orcFile.delete());

    OutputFile outFile = Files.localOutput(orcFile);
    try (FileAppender<GenericRecord> appender = ORC.write(outFile)
        .schema(FILE_SCHEMA)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      GenericRecord record = GenericRecord.create(FILE_SCHEMA);
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        record.setField("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        record.setField("_no_stats_parquet", TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
                                                                          // in Parquet, but will produce stats for ORC
        record.setField("_required", "req"); // required, always non-null
        record.setField("_all_nulls", null); // never non-null
        record.setField("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        record.setField("_no_nulls", ""); // optional, but always non-null
        record.setField("_str", i + "str" + i);
        record.setField("_all_nans", Double.NaN); // never non-nan
        record.setField("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        record.setField("_no_nans", 3D); // optional, but always non-nan

        GenericRecord structNotNull = GenericRecord.create(_structFieldType);
        structNotNull.setField("_int_field", INT_MIN_VALUE + i);
        record.setField("_struct_not_null", structNotNull); // struct with int

        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(orcFile);
    try (Reader reader = OrcFile.createReader(new Path(inFile.location()),
        OrcFile.readerOptions(new Configuration()))) {
      Assert.assertEquals("Should create only one stripe", 1, reader.getStripes().size());
    }

    orcFile.deleteOnExit();
  }

  private void createParquetInputFile() throws IOException {
    File parquetFile = temp.newFile();
    Assert.assertTrue(parquetFile.delete());

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set("_no_stats_parquet", TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
                                                                      // in Parquet
        builder.set("_required", "req"); // required, always non-null
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan
        builder.set("_str", i + "str" + i);

        Record structNotNull = new Record(structSchema);
        structNotNull.put("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    parquetFile.deleteOnExit();
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead;

    shouldRead = shouldRead(notNull("all_nulls"));
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = shouldRead(notNull("some_nulls"));
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = shouldRead(notNull("no_nulls"));
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);

    shouldRead = shouldRead(notNull("map_not_null"));
    Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = shouldRead(notNull("struct_not_null"));
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = shouldRead(isNull("some_nulls"));
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = shouldRead(isNull("no_nulls"));
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);

    shouldRead = shouldRead(isNull("map_not_null"));
    Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = shouldRead(isNull("struct_not_null"));
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = shouldRead(isNaN("all_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(isNaN("some_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(isNaN("no_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(isNaN("all_nulls"));
    Assert.assertFalse("Should skip: all null column will not contain nan value", shouldRead);
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(notNaN("all_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(notNaN("some_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(notNaN("no_nans"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

    shouldRead = shouldRead(notNaN("all_nulls"));
    Assert.assertTrue("Should read: NaN counts are not tracked in Parquet metrics", shouldRead);

  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(notNull("required"));
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = shouldRead(isNull("required"));
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testMissingColumn() {
    exceptionRule.expect(ValidationException.class);
    exceptionRule.expectMessage("Cannot find field 'missing'");
    exceptionRule.reportMissingExceptionWithMessage("Should complain about missing column in expression");
    shouldRead(lessThan("missing", 5));
  }

  @Test
  public void testColumnNotInFile() {
    Assume.assumeFalse(
        "If a column is not in file, ORC does NOT try to apply predicates assuming null values for the column",
        format == FileFormat.ORC);
    Expression[] cannotMatch = new Expression[] {
        lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
        equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
        greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
    };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = shouldRead(expr);
      Assert.assertFalse("Should skip when column is not in file (all nulls): " + expr, shouldRead);
    }

    Expression[] canMatch = new Expression[] {
        isNull("not_in_file"), notEqual("not_in_file", 1.0f)
    };

    for (Expression expr : canMatch) {
      boolean shouldRead = shouldRead(expr);
      Assert.assertTrue("Should read when column is not in file (all nulls): " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStatsParquet() {
    Assume.assumeTrue(format == FileFormat.PARQUET);
    Expression[] exprs = new Expression[] {
        lessThan("no_stats_parquet", "a"), lessThanOrEqual("no_stats_parquet", "b"), equal("no_stats_parquet", "c"),
        greaterThan("no_stats_parquet", "d"), greaterThanOrEqual("no_stats_parquet", "e"),
        notEqual("no_stats_parquet", "f"), isNull("no_stats_parquet"), notNull("no_stats_parquet"),
        startsWith("no_stats_parquet", "a"), notStartsWith("no_stats_parquet", "a")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(expr);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testZeroRecordFileParquet() {
    Assume.assumeTrue(format == FileFormat.PARQUET);
    BlockMetaData emptyBlock = new BlockMetaData();
    emptyBlock.setRowCount(0);

    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70), greaterThan("id", 78),
        greaterThanOrEqual("id", 90), notEqual("id", 101), isNull("some_nulls"),
        notNull("some_nulls")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldReadParquet(expr, true, parquetSchema, emptyBlock);
      Assert.assertFalse("Should never read 0-record file: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(and(lessThan("id", INT_MIN_VALUE - 25),
        greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead = shouldRead(and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = shouldRead(and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(or(lessThan("id", INT_MIN_VALUE - 25),
        greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = shouldRead(or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);
  }

  @Test
  public void testStringStartsWith() {
    Assume.assumeFalse("ORC row group filter does not support StringStartsWith", format == FileFormat.ORC);
    boolean shouldRead = shouldRead(startsWith("str", "1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "0st"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "1str1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "1str1_xgd"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "2str"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "9xstr"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "0S"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "x"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "9str9aaa"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);
  }

  @Test
  public void testStringNotStartsWith() {
    Assume.assumeFalse("ORC row group filter does not support StringStartsWith", format == FileFormat.ORC);
    boolean shouldRead = shouldRead(notStartsWith("str", "1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("str", "0st"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("str", "1str1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("str", "1str1_xgd"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("str", "2str"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("str", "9xstr"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("required", "r"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(notStartsWith("required", "requ"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("some_nulls", "ssome"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(notStartsWith("some_nulls", "som"));
    Assert.assertTrue("Should read: range matches", shouldRead);
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    Assert.assertFalse("Should skip: in on all nulls column", shouldRead);

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    Assert.assertTrue("Should read: in on no nulls column", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    Assert.assertTrue("Should read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    Assert.assertTrue("Should read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    Assert.assertTrue("Should read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    Assert.assertTrue("Should read: notIn on all nulls column", shouldRead);

    shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    Assert.assertTrue("Should read: notIn on some nulls column", shouldRead);

    shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    if (format == FileFormat.PARQUET) {
      // no_nulls column has all values == "", so notIn("no_nulls", "") should always be false and so should be skipped
      // However, the metrics evaluator in Parquets always reads row group for a notIn filter
      Assert.assertTrue("Should read: notIn on no nulls column", shouldRead);
    } else {
      Assert.assertFalse("Should skip: notIn on no nulls column", shouldRead);
    }
  }

  @Test
  public void testSomeNullsNotEq() {
    boolean shouldRead = shouldRead(notEqual("some_nulls", "some"));
    Assert.assertTrue("Should read: notEqual on some nulls column", shouldRead);
  }

  @Test
  public void testInLimitParquet() {
    Assume.assumeTrue(format == FileFormat.PARQUET);

    boolean shouldRead = shouldRead(in("id", 1, 2));
    Assert.assertFalse("Should not read if IN is evaluated", shouldRead);

    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }

    shouldRead = shouldRead(in("id", ids));
    Assert.assertTrue("Should read if IN is not evaluated", shouldRead);
  }

  @Test
  public void testParquetTypePromotion() {
    Assume.assumeTrue("Only valid for Parquet", format == FileFormat.PARQUET);
    Schema promotedSchema = new Schema(required(1, "id", Types.LongType.get()));
    boolean shouldRead = new ParquetMetricsRowGroupFilter(promotedSchema, equal("id", INT_MIN_VALUE + 1), true)
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should succeed with promoted schema", shouldRead);
  }

  private boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  private boolean shouldRead(Expression expression, boolean caseSensitive) {
    switch (format) {
      case ORC:
        return shouldReadOrc(expression, caseSensitive);
      case PARQUET:
        return shouldReadParquet(expression, caseSensitive, parquetSchema, rowGroupMetadata);
      default:
        throw new UnsupportedOperationException("Row group filter tests not supported for " + format);
    }
  }

  private boolean shouldReadOrc(Expression expression, boolean caseSensitive) {
    try (CloseableIterable<org.apache.iceberg.data.Record> reader = ORC.read(Files.localInput(orcFile))
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
        .filter(expression)
        .caseSensitive(caseSensitive)
        .build()) {
      return Lists.newArrayList(reader).size() > 0;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean shouldReadParquet(Expression expression, boolean caseSensitive, MessageType messageType,
      BlockMetaData blockMetaData) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, expression, caseSensitive)
        .shouldRead(messageType, blockMetaData);
  }

  private org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inFile.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream stream = inFile.newStream();
        return new DelegatingSeekableInputStream(stream) {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }
        };
      }
    };
  }
}
