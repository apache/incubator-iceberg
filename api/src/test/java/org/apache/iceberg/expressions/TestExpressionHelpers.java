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

import java.util.concurrent.Callable;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.rewriteNot;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;

public class TestExpressionHelpers {
  private final UnboundPredicate<?> pred = lessThan("x", 7);

  @Test
  public void testSimplifyOr() {
    Assert.assertEquals("alwaysTrue or pred => alwaysTrue",
        alwaysTrue(), or(alwaysTrue(), pred));
    Assert.assertEquals("pred or alwaysTrue => alwaysTrue",
        alwaysTrue(), or(pred, alwaysTrue()));

    Assert.assertEquals("alwaysFalse or pred => pred",
        pred, or(alwaysFalse(), pred));
    Assert.assertEquals("pred or alwaysTrue => pred",
        pred, or(pred, alwaysFalse()));
  }

  @Test
  public void testSimplifyAnd() {
    Assert.assertEquals("alwaysTrue and pred => pred",
        pred, and(alwaysTrue(), pred));
    Assert.assertEquals("pred and alwaysTrue => pred",
        pred, and(pred, alwaysTrue()));

    Assert.assertEquals("alwaysFalse and pred => alwaysFalse",
        alwaysFalse(), and(alwaysFalse(), pred));
    Assert.assertEquals("pred and alwaysFalse => alwaysFalse",
        alwaysFalse(), and(pred, alwaysFalse()));
  }

  @Test
  public void testSimplifyNot() {
    Assert.assertEquals("not(alwaysTrue) => alwaysFalse",
        alwaysFalse(), not(alwaysTrue()));
    Assert.assertEquals("not(alwaysFalse) => alwaysTrue",
        alwaysTrue(), not(alwaysFalse()));

    Assert.assertEquals("not(not(pred)) => pred",
        pred, not(not(pred)));
  }

  @Test
  public void testRewriteNot() {
    StructType struct = StructType.of(
        NestedField.optional(1, "a", Types.IntegerType.get()),
        NestedField.optional(2, "s", Types.StringType.get()));
    Expression[][] expressions = new Expression[][] {
        // (rewritten pred, original pred) pairs
        { isNull("a"), isNull("a") },
        { notNull("a"), not(isNull("a")) },
        { notNull("a"), notNull("a") },
        { isNull("a"), not(notNull("a")) },
        { equal("a", 5), equal("a", 5) },
        { notEqual("a", 5), not(equal("a", 5)) },
        { notEqual("a", 5), notEqual("a", 5) },
        { equal("a", 5), not(notEqual("a", 5)) },
        { in("a", 5, 6), in("a", 5, 6) },
        { notIn("a", 5, 6), not(in("a", 5, 6)) },
        { notIn("a", 5, 6), notIn("a", 5, 6) },
        { in("a", 5, 6), not(notIn("a", 5, 6)) },
        { lessThan("a", 5), lessThan("a", 5) },
        { greaterThanOrEqual("a", 5), not(lessThan("a", 5)) },
        { greaterThanOrEqual("a", 5), greaterThanOrEqual("a", 5) },
        { lessThan("a", 5), not(greaterThanOrEqual("a", 5)) },
        { lessThanOrEqual("a", 5), lessThanOrEqual("a", 5) },
        { greaterThan("a", 5), not(lessThanOrEqual("a", 5)) },
        { greaterThan("a", 5), greaterThan("a", 5) },
        { lessThanOrEqual("a", 5), not(greaterThan("a", 5)) },
        { or(equal("a", 5), isNull("a")), or(equal("a", 5), isNull("a")) },
        { and(notEqual("a", 5), notNull("a")), not(or(equal("a", 5), isNull("a"))) },
        { and(notEqual("a", 5), notNull("a")), and(notEqual("a", 5), notNull("a")) },
        { or(equal("a", 5), isNull("a")), not(and(notEqual("a", 5), notNull("a"))) },
        { or(equal("a", 5), notNull("a")), or(equal("a", 5), not(isNull("a"))) },
        { startsWith("s", "hello"), not(notStartsWith("s", "hello")) },
        { notStartsWith("s", "world"), not(startsWith("s", "world")) }
    };

    for (Expression[] pair : expressions) {
      // unbound rewrite
      Assert.assertEquals(String.format("rewriteNot(%s) should be %s", pair[1], pair[0]),
          pair[0].toString(), rewriteNot(pair[1]).toString());

      // bound rewrite
      Expression expectedBound = Binder.bind(struct, pair[0]);
      Expression toRewriteBound = Binder.bind(struct, pair[1]);
      Assert.assertEquals(String.format("rewriteNot(%s) should be %s", toRewriteBound, expectedBound),
          expectedBound.toString(), rewriteNot(toRewriteBound).toString());
    }
  }

  @Test
  public void testTransformExpressions() {
    Assert.assertEquals("Should produce the correct expression string",
        "year(ref(name=\"ts\")) == \"2019\"",
        equal(year("ts"), "2019").toString());
    Assert.assertEquals("Should produce the correct expression string",
        "month(ref(name=\"ts\")) == 1234",
        equal(month("ts"), 1234).toString());
    Assert.assertEquals("Should produce the correct expression string",
        "day(ref(name=\"ts\")) == \"2019-12-04\"",
        equal(day("ts"), "2019-12-04").toString());
    Assert.assertEquals("Should produce the correct expression string",
        "hour(ref(name=\"ts\")) == \"2019-12-04-10\"",
        equal(hour("ts"), "2019-12-04-10").toString());
    Assert.assertEquals("Should produce the correct expression string",
        "truncate[6](ref(name=\"str\")) == \"abcdef\"",
        equal(truncate("str", 6), "abcdef").toString());
    Assert.assertEquals("Should produce the correct expression string",
        "truncate[5](ref(name=\"i\")) == 10",
        equal(truncate("i", 5), 10).toString());
    Assert.assertEquals("Should produce the correct expression string",
        "bucket[16](ref(name=\"id\")) == 12",
        equal(bucket("id", 16), 12).toString());
  }

  @Test
  public void testNullName() {
    AssertHelpers.assertThrows("Should catch null column names when creating expressions",
        NullPointerException.class, "Name cannot be null", () -> equal((String) null, 5));
  }

  @Test
  public void testNullValueExpr() {
    AssertHelpers.assertThrows("Should catch null value expressions",
        NullPointerException.class, "Term cannot be null",
        () -> equal((UnboundTerm<Integer>) null, 5));
  }

  @Test
  public void testMultiAnd() {
    Expression expected = and(
        and(
          equal("a", 1),
          equal("b", 2)),
        equal("c", 3));

    Expression actual = and(
        equal("a", 1),
        equal("b", 2),
        equal("c", 3));

    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testInvalidateNaNInput() {
    assertInvalidateNaNThrows(() -> lessThan("a", Double.NaN));
    assertInvalidateNaNThrows(() -> lessThan(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> lessThanOrEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> lessThanOrEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> greaterThan("a", Double.NaN));
    assertInvalidateNaNThrows(() -> greaterThan(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> greaterThanOrEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> greaterThanOrEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> equal("a", Double.NaN));
    assertInvalidateNaNThrows(() -> equal(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> notEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> notEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> in("a", 1.0D, 2.0D, Double.NaN));
    assertInvalidateNaNThrows(() -> in(self("a"), 1.0D, 2.0D, Double.NaN));

    assertInvalidateNaNThrows(() -> notIn("a", 1.0D, 2.0D, Double.NaN));
    assertInvalidateNaNThrows(() -> notIn(self("a"), 1.0D, 2.0D, Double.NaN));

    assertInvalidateNaNThrows(() -> predicate(Expression.Operation.EQ, "a", Double.NaN));
  }

  private void assertInvalidateNaNThrows(Callable<UnboundPredicate<Double>> callable) {
    AssertHelpers.assertThrows("Should invalidate NaN input",
        IllegalArgumentException.class, "Cannot create expression literal from NaN",
        callable);
  }

  private <T> UnboundTerm<T> self(String name) {
    return new UnboundTransform<>(ref(name), Transforms.identity(Types.DoubleType.get()));
  }

}
