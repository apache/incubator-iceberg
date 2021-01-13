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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
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
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStrictMetricsEvaluator {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", IntegerType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", StringType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      required(7, "always_5", IntegerType.get()),
      optional(8, "all_nans", Types.DoubleType.get()),
      optional(9, "some_nans", Types.FloatType.get()),
      optional(10, "no_nans", Types.FloatType.get()),
      optional(11, "all_nulls_double", Types.DoubleType.get()),
      optional(12, "all_nans_v1_stats", Types.FloatType.get()),
      optional(13, "nan_and_null_only", Types.DoubleType.get()),
      optional(14, "no_nan_stats", Types.DoubleType.get())
  );

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  private static final DataFile FILE = new TestDataFile("file.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.<Integer, Long>builder()
          .put(4, 50L)
          .put(5, 50L)
          .put(6, 50L)
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
          .put(11, 50L)
          .put(12, 0L)
          .put(13, 1L)
          .build(),
      // nan value counts
      ImmutableMap.of(
          8, 50L,
          9, 10L,
          10, 0L),
      // lower bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
          7, toByteBuffer(IntegerType.get(), 5),
          12, toByteBuffer(Types.FloatType.get(), Float.NaN),
          13, toByteBuffer(Types.DoubleType.get(), Double.NaN)),
      // upper bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
          7, toByteBuffer(IntegerType.get(), 5),
          12, toByteBuffer(Types.FloatType.get(), Float.NaN),
          13, toByteBuffer(Types.DoubleType.get(), Double.NaN)));

  private static final DataFile FILE_2 = new TestDataFile("file_2.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(
          4, 50L,
          5, 50L,
          6, 50L,
          8, 50L),
      // null value counts
      ImmutableMap.of(
          4, 50L,
          5, 10L,
          6, 0L),
      // nan value counts
      null,
      // lower bounds
      ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")),
      // upper bounds
      ImmutableMap.of(5, toByteBuffer(StringType.get(), "eee")));

  private static final DataFile FILE_3 = new TestDataFile("file_3.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(
          4, 50L,
          5, 50L,
          6, 50L),
      // null value counts
      ImmutableMap.of(
          4, 50L,
          5, 10L,
          6, 0L),
      // nan value counts
      null,
      // lower bounds
      ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")),
      // upper bounds
      ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")));

  @Test
  public void testAllNulls() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, notNull("all_nulls")).eval(FILE);
    Assert.assertFalse("Should not match: no non-null value in all null column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNull("some_nulls")).eval(FILE);
    Assert.assertFalse("Should not match: column with some nulls contains a non-null value", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNull("no_nulls")).eval(FILE);
    Assert.assertTrue("Should match: non-null column contains no null values", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("all_nulls", "a")).eval(FILE);
    Assert.assertTrue("Should match: notEqual on all nulls column", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, isNull("all_nulls")).eval(FILE);
    Assert.assertTrue("Should match: all values are null", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNull("some_nulls")).eval(FILE);
    Assert.assertFalse("Should not match: not all values are null", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNull("no_nulls")).eval(FILE);
    Assert.assertFalse("Should not match: no values are null", shouldRead);
  }

  @Test
  public void testSomeNulls() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThan("some_nulls", "ggg")).eval(FILE_2);
    Assert.assertFalse("Should not match: lessThan on some nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThanOrEqual("some_nulls", "eee")).eval(FILE_2);
    Assert.assertFalse("Should not match: lessThanOrEqual on some nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThan("some_nulls", "aaa")).eval(FILE_2);
    Assert.assertFalse("Should not match: greaterThan on some nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThanOrEqual("some_nulls", "bbb")).eval(FILE_2);
    Assert.assertFalse("Should not match: greaterThanOrEqual on some nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("some_nulls", "bbb")).eval(FILE_3);
    Assert.assertFalse("Should not match: equal on some nulls column", shouldRead);
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("all_nans")).eval(FILE);
    Assert.assertTrue("Should match: all values are nan", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("some_nans")).eval(FILE);
    Assert.assertFalse("Should not match: at least one non-nan value in some nan column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("no_nans")).eval(FILE);
    Assert.assertFalse("Should not match: at least one non-nan value in no nan column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("all_nulls_double")).eval(FILE);
    Assert.assertFalse("Should not match: at least one non-nan value in all null column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("no_nan_stats")).eval(FILE);
    Assert.assertFalse("Should not match: cannot determine without nan stats", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("all_nans_v1_stats")).eval(FILE);
    Assert.assertFalse("Should not match: cannot determine without nan stats", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNaN("nan_and_null_only")).eval(FILE);
    Assert.assertFalse("Should not match: null values are not nan", shouldRead);
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("all_nans")).eval(FILE);
    Assert.assertFalse("Should not match: all values are nan", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("some_nans")).eval(FILE);
    Assert.assertFalse("Should not match: at least one nan value in some nan column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("no_nans")).eval(FILE);
    Assert.assertTrue("Should match: no value is nan", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("all_nulls_double")).eval(FILE);
    Assert.assertTrue("Should match: no nan value in all null column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("no_nan_stats")).eval(FILE);
    Assert.assertFalse("Should not match: cannot determine without nan stats", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("all_nans_v1_stats")).eval(FILE);
    Assert.assertFalse("Should not match: all values are nan", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notNaN("nan_and_null_only")).eval(FILE);
    Assert.assertFalse("Should not match: null values are not nan", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, notNull("required")).eval(FILE);
    Assert.assertTrue("Should match: required columns are always non-null", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, isNull("required")).eval(FILE);
    Assert.assertFalse("Should not match: required columns never contain null", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    AssertHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new StrictMetricsEvaluator(SCHEMA, lessThan("missing", 5)).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    DataFile missingStats = new TestDataFile("file.parquet", Row.of(), 50);

    Expression[] exprs = new Expression[] {
        lessThan("no_stats", 5), lessThanOrEqual("no_stats", 30), equal("no_stats", 70),
        greaterThan("no_stats", 78), greaterThanOrEqual("no_stats", 90), notEqual("no_stats", 101),
        isNull("no_stats"), notNull("no_stats"), isNaN("all_nans"), notNaN("all_nans")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, expr).eval(missingStats);
      Assert.assertFalse("Should never match when stats are missing for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testZeroRecordFile() {
    DataFile empty = new TestDataFile("file.parquet", Row.of(), 0);

    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70), greaterThan("id", 78),
        greaterThanOrEqual("id", 90), notEqual("id", 101), isNull("some_nulls"),
        notNull("some_nulls"), isNaN("all_nans"), notNaN("all_nans")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, expr).eval(empty);
      Assert.assertTrue("Should always match 0-record file: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertTrue("Should not match: not(false)", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertFalse("Should match: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA,
        and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE))).eval(FILE);
    Assert.assertFalse("Should not match: range may not overlap data", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30))).eval(FILE);
    Assert.assertFalse("Should not match: range does not overlap data", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        and(lessThan("id", INT_MAX_VALUE + 6), greaterThanOrEqual("id", INT_MIN_VALUE - 30))).eval(FILE);
    Assert.assertTrue("Should match: range includes all data", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1))).eval(FILE);
    Assert.assertFalse("Should not match: no matching values", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19))).eval(FILE);
    Assert.assertFalse("Should not match: some values do not match", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE))).eval(FILE);
    Assert.assertTrue("Should match: all values match >= 30", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: always false", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: 32 and greater not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThan("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: 79 not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThan("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should match: all values in range", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertFalse("Should not match: always false", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: 31 and greater not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertTrue("Should match: all values in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should match: all values in range", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: always false", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1)).eval(FILE);
    Assert.assertFalse("Should not match: 77 and less not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThan("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: 30 not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThan("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertTrue("Should match: all values in range", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: no values in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: 78 and lower are not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MIN_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: 30 not in range", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertTrue("Should match: all values in range", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertFalse("Should not match: all values != 5", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 30", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 75", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 79", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 80", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, equal("always_5", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertTrue("Should match: all values == 5", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 25)).eval(FILE);
    Assert.assertTrue("Should match: no values == 5", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    Assert.assertTrue("Should match: no values == 39", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 30", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE - 4)).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 75", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 79", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertTrue("Should match: no values == 80", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 6)).eval(FILE);
    Assert.assertTrue("Should read: no values == 85", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 25))).eval(FILE);
    Assert.assertTrue("Should match: no values == 5", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 1))).eval(FILE);
    Assert.assertTrue("Should match: no values == 39", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE))).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 30", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE - 4))).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 75", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE))).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 79", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 1))).eval(FILE);
    Assert.assertTrue("Should match: no values == 80", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 6))).eval(FILE);
    Assert.assertTrue("Should read: no values == 85", shouldRead);
  }


  @Test
  public void testIntegerIn() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)).eval(FILE);
    Assert.assertFalse("Should not match: all values != 5 and != 6", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 30 and != 31", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 75 and != 76", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 78 and != 79", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)).eval(FILE);
    Assert.assertFalse("Should not match: some values != 80 and != 81)", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("always_5", 5, 6)).eval(FILE);
    Assert.assertTrue("Should match: all values == 5", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("all_nulls", "abc", "def")).eval(FILE);
    Assert.assertFalse("Should not match: in on all nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("some_nulls", "abc", "def")).eval(FILE_3);
    Assert.assertFalse("Should not match: in on some nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        in("no_nulls", "abc", "def")).eval(FILE);
    Assert.assertFalse("Should not match: no_nulls field does not have bounds", shouldRead);
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)).eval(FILE);
    Assert.assertTrue("Should not match: all values !=5 and !=6", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE)).eval(FILE);
    Assert.assertFalse("Should not match: some values may be == 30", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 75 or == 76", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1)).eval(FILE);
    Assert.assertFalse("Should not match: some value may be == 79", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)).eval(FILE);
    Assert.assertTrue("Should match: no values == 80 or == 81", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("always_5", 5, 6)).eval(FILE);
    Assert.assertFalse("Should not match: all values == 5", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("all_nulls", "abc", "def")).eval(FILE);
    Assert.assertTrue("Should match: notIn on all nulls column", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("some_nulls", "abc", "def")).eval(FILE_3);
    Assert.assertTrue("Should match: notIn on some nulls column, 'bbb' > 'abc' and 'bbb' < 'def'", shouldRead);

    shouldRead = new StrictMetricsEvaluator(SCHEMA,
        notIn("no_nulls", "abc", "def")).eval(FILE);
    Assert.assertFalse("Should not match: no_nulls field does not have bounds", shouldRead);
  }
}
