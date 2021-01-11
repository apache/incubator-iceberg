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

package org.apache.iceberg.expressions;

import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.util.UnicodeUtil;
import org.junit.Assert;
import org.junit.Test;

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
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestInclusiveMetricsEvaluator {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", Types.IntegerType.get()),
      required(3, "required", Types.StringType.get()),
      optional(4, "all_nulls", Types.StringType.get()),
      optional(5, "some_nulls", Types.StringType.get()),
      optional(6, "no_nulls", Types.StringType.get()),
      optional(7, "all_nans", Types.DoubleType.get()),
      optional(8, "some_nans", Types.FloatType.get()),
      optional(9, "no_nans", Types.FloatType.get()),
      optional(10, "all_nulls_double", Types.DoubleType.get()),
      optional(11, "all_nans_v1_stats", Types.FloatType.get()),
      optional(12, "nan_and_null_only", Types.DoubleType.get()),
      optional(13, "no_nan_stats", Types.DoubleType.get()),
      optional(14, "some_empty", Types.StringType.get())
  );

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  private static final DataFile FILE = new TestDataFile("file.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.<Integer, Long>builder()
          .put(4, 50L)
          .put(5, 50L)
          .put(6, 50L)
          .put(7, 50L)
          .put(8, 50L)
          .put(9, 50L)
          .put(10, 50L)
          .put(11, 50L)
          .put(12, 50L)
          .put(13, 50L)
          .put(14, 50L)
          .build(),
      // null value counts
      ImmutableMap.<Integer, Long>builder()
          .put(4, 50L)
          .put(5, 10L)
          .put(6, 0L)
          .put(10, 50L)
          .put(11, 0L)
          .put(12, 1L)
          .put(14, 0L)
          .build(),
      // nan value counts
      ImmutableMap.of(
          7, 50L,
          8, 10L,
          9, 0L),
      // lower bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
          11, toByteBuffer(Types.FloatType.get(), Float.NaN),
          12, toByteBuffer(Types.DoubleType.get(), Double.NaN),
          14, toByteBuffer(Types.StringType.get(), "")),
      // upper bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
          11, toByteBuffer(Types.FloatType.get(), Float.NaN),
          12, toByteBuffer(Types.DoubleType.get(), Double.NaN),
          14, toByteBuffer(Types.StringType.get(), "房东整租霍营小区二层两居室")));

  private static final DataFile FILE_2 = new TestDataFile("file_2.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(3, 20L),
      // null value counts
      ImmutableMap.of(3, 2L),
      // nan value counts
      null,
      // lower bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "aa")),
      // upper bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "dC")));

  private static final DataFile FILE_3 = new TestDataFile("file_3.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(3, 20L),
      // null value counts
      ImmutableMap.of(3, 2L),
      // nan value counts
      null,
      // lower bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "1str1")),
      // upper bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "3str3")));

  private static final DataFile FILE_4 = new TestDataFile("file_4.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(3, 20L),
      // null value counts
      ImmutableMap.of(3, 2L),
      // nan value counts
      null,
      // lower bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")),
      // upper bounds
      ImmutableMap.of(3, toByteBuffer(StringType.get(), "イロハニホヘト")));

  @Test
  public void testAllNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("all_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: lessThan on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: lessThanOrEqual on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: greaterThan on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: greaterThanOrEqual on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: equal on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Should skip: startsWith on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("all_nulls", "a")).eval(FILE);
    Assert.assertFalse("Sould skip: notStartsWith on all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("no_nulls")).eval(FILE);
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("all_nulls")).eval(FILE);
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("no_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nans")).eval(FILE);
    Assert.assertTrue("Should read: at least one nan value in all nan column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("some_nans")).eval(FILE);
    Assert.assertTrue("Should read: at least one nan value in some nan column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("no_nans")).eval(FILE);
    Assert.assertFalse("Should skip: no-nans column contains no nan values", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nulls_double")).eval(FILE);
    Assert.assertFalse("Should skip: all-null column doesn't contain nan value", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("no_nan_stats")).eval(FILE);
    Assert.assertTrue("Should read: no guarantee on if contains nan value without nan stats", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nans_v1_stats")).eval(FILE);
    Assert.assertTrue("Should read: at least one nan value in all nan column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("nan_and_null_only")).eval(FILE);
    Assert.assertTrue("Should read: at least one nan value in nan and nulls only column", shouldRead);
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nans")).eval(FILE);
    Assert.assertFalse("Should skip: column with all nans will not contain non-nan", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("some_nans")).eval(FILE);
    Assert.assertTrue("Should read: at least one non-nan value in some nan column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("no_nans")).eval(FILE);
    Assert.assertTrue("Should read: at least one non-nan value in no nan column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nulls_double")).eval(FILE);
    Assert.assertTrue("Should read: at least one non-nan value in all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("no_nan_stats")).eval(FILE);
    Assert.assertTrue("Should read: no guarantee on if contains nan value without nan stats", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nans_v1_stats")).eval(FILE);
    Assert.assertTrue("Should read: no guarantee on if contains nan value without nan stats", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("nan_and_null_only")).eval(FILE);
    Assert.assertTrue("Should read: at least one null value in nan and nulls only column", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("required")).eval(FILE);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("required")).eval(FILE);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    AssertHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new InclusiveMetricsEvaluator(SCHEMA, lessThan("missing", 5)).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    DataFile missingStats = new TestDataFile("file.parquet", Row.of(), 50);

    Expression[] exprs = new Expression[] {
        lessThan("no_stats", 5), lessThanOrEqual("no_stats", 30), equal("no_stats", 70),
        greaterThan("no_stats", 78), greaterThanOrEqual("no_stats", 90), notEqual("no_stats", 101),
        isNull("no_stats"), notNull("no_stats"), isNaN("some_nans"), notNaN("some_nans")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(missingStats);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testZeroRecordFile() {
    DataFile empty = new TestDataFile("file.parquet", Row.of(), 0);

    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70), greaterThan("id", 78),
        greaterThanOrEqual("id", 90), notEqual("id", 101), isNull("some_nulls"),
        notNull("some_nulls"), isNaN("some_nans"), notNaN("some_nans"),
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(empty);
      Assert.assertFalse("Should never read 0-record file: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30))).eval(FILE);
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1))).eval(FILE);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE))).eval(FILE);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1))).eval(FILE);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19))).eval(FILE);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE + 6)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 6)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE + 6)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 6)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 1))).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE))).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE - 4))).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE))).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 1))).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 6))).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE - 25)), false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE - 1)), false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE)), false).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE - 4)), false).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE)), false).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE + 1)), false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE + 6)), false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    AssertHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'ID'",
        () -> new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", 5)), true).eval(FILE));
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "a"), true).eval(FILE);
    Assert.assertTrue("Should read: no stats", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "a"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aa"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aaa"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "1s"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "1str1x"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "ff"), true).eval(FILE_4);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aB"), true).eval(FILE_2);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "dWX"), true).eval(FILE_2);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "5"), true).eval(FILE_3);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "3str3x"), true).eval(FILE_3);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("some_empty", "房东整租霍"), true).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("all_nulls", ""), true).eval(FILE);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", aboveMax), true).eval(FILE_4);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);
  }

  @Test
  // More interesting test cases are included in TestNotStartsWith
  public void testStringNotStartsWith() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "a"), true).eval(FILE);
    Assert.assertTrue("Should read: no stats", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "a"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aa"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aaa"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "1s"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "1str1x"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "ff"), true).eval(FILE_4);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aB"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "dWX"), true).eval(FILE_2);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "5"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "3str3x"), true).eval(FILE_3);
    Assert.assertTrue("Should read: range matches", shouldRead);

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", aboveMax), true).eval(FILE_4);
    Assert.assertTrue("Should read: range matches", shouldRead);
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("all_nulls", "abc", "def")).eval(FILE);
    Assert.assertFalse("Should skip: in on all nulls column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("some_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        in("no_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should read: in on no nulls column", shouldRead);

    // should read as the number of elements in the in expression is too big
    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }
    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, in("id", ids)).eval(FILE);
    Assert.assertTrue("Should read: large in expression", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("all_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should read: notIn on all nulls column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("some_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should read: notIn on some nulls column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        notIn("no_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should read: notIn on no nulls column", shouldRead);

  }
}
