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

import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestExpressionBinding {
  private static final StructType STRUCT = StructType.of(
      required(0, "x", Types.IntegerType.get()),
      required(1, "y", Types.IntegerType.get()),
      required(2, "z", Types.IntegerType.get()),
      required(3, "data", Types.StringType.get())
  );

  @Test
  public void testMissingReference() {
    Expression expr = and(equal("t", 5), equal("x", 7));
    try {
      Binder.bind(STRUCT, expr);
      Assert.fail("Should not successfully bind to struct without field 't'");
    } catch (ValidationException e) {
      Assert.assertTrue("Should complain about missing field",
          e.getMessage().contains("Cannot find field 't' in struct:"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBoundExpressionFails() {
    Expression expr = not(equal("x", 7));
    Binder.bind(STRUCT, Binder.bind(STRUCT, expr));
  }

  @Test
  public void testSingleReference() {
    Expression expr = not(equal("x", 7));
    TestHelpers.assertAllReferencesBound("Single reference", Binder.bind(STRUCT, expr, true));
  }

  @Test
  public void testCaseInsensitiveReference() {
    Expression expr = not(equal("X", 7));
    TestHelpers.assertAllReferencesBound("Single reference", Binder.bind(STRUCT, expr, false));
  }

  @Test(expected = ValidationException.class)
  public void testCaseSensitiveReference() {
    Expression expr = not(equal("X", 7));
    Binder.bind(STRUCT, expr, true);
  }

  @Test
  public void testMultipleReferences() {
    Expression expr = or(and(equal("x", 7), lessThan("y", 100)), greaterThan("z", -100));
    TestHelpers.assertAllReferencesBound("Multiple references", Binder.bind(STRUCT, expr));
  }

  @Test
  public void testAnd() {
    Expression expr = and(equal("x", 7), lessThan("y", 100));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("And", boundExpr);

    // make sure the result is an And
    And and = TestHelpers.assertAndUnwrap(boundExpr, And.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> left = TestHelpers.assertAndUnwrap(and.left());
    Assert.assertEquals("Should bind x correctly", 0, left.term().ref().fieldId());
    BoundPredicate<?> right = TestHelpers.assertAndUnwrap(and.right());
    Assert.assertEquals("Should bind y correctly", 1, right.term().ref().fieldId());
  }

  @Test
  public void testOr() {
    Expression expr = or(greaterThan("z", -100), lessThan("y", 100));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("Or", boundExpr);

    // make sure the result is an Or
    Or or = TestHelpers.assertAndUnwrap(boundExpr, Or.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> left = TestHelpers.assertAndUnwrap(or.left());
    Assert.assertEquals("Should bind z correctly", 2, left.term().ref().fieldId());
    BoundPredicate<?> right = TestHelpers.assertAndUnwrap(or.right());
    Assert.assertEquals("Should bind y correctly", 1, right.term().ref().fieldId());
  }

  @Test
  public void testNot() {
    Expression expr = not(equal("x", 7));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("Not", boundExpr);

    // make sure the result is a Not
    Not not = TestHelpers.assertAndUnwrap(boundExpr, Not.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> child = TestHelpers.assertAndUnwrap(not.child());
    Assert.assertEquals("Should bind x correctly", 0, child.term().ref().fieldId());
  }

  @Test
  public void testStartsWith() {
    StructType struct = StructType.of(required(0, "s", Types.StringType.get()));
    Expression expr = startsWith("s", "abc");
    Expression boundExpr = Binder.bind(struct, expr);
    TestHelpers.assertAllReferencesBound("StartsWith", boundExpr);
    // make sure the expression is a StartsWith
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(boundExpr, BoundPredicate.class);
    Assert.assertEquals("Should be right operation", Expression.Operation.STARTS_WITH, pred.op());
    Assert.assertEquals("Should bind s correctly", 0, pred.term().ref().fieldId());
  }

  @Test
  public void testNotStartsWith() {
    StructType struct = StructType.of(required(21, "s", Types.StringType.get()));
    Expression expr = notStartsWith("s", "abc");
    Expression boundExpr = Binder.bind(struct, expr);
    TestHelpers.assertAllReferencesBound("NotStartsWith", boundExpr);
    // Make sure the expression is a NotStartsWith
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(boundExpr, BoundPredicate.class);
    Assert.assertEquals("Should be right operation", Expression.Operation.NOT_STARTS_WITH, pred.op());
    Assert.assertEquals("Should bind term to correct field id", 21, pred.term().ref().fieldId());
  }

  @Test
  public void testAlwaysTrue() {
    Assert.assertEquals("Should not change alwaysTrue",
        alwaysTrue(),
        Binder.bind(STRUCT, alwaysTrue()));
  }

  @Test
  public void testAlwaysFalse() {
    Assert.assertEquals("Should not change alwaysFalse",
        alwaysFalse(),
        Binder.bind(STRUCT, alwaysFalse()));
  }

  @Test
  public void testBasicSimplification() {
    // this tests that a basic simplification is done by calling the helpers in Expressions. those
    // are more thoroughly tested in TestExpressionHelpers.

    // the second predicate is always true once it is bound because z is an integer and the literal
    // is less than any 32-bit integer value
    Assert.assertEquals("Should simplify or expression to alwaysTrue",
        alwaysTrue(), Binder.bind(STRUCT, or(lessThan("y", 100), greaterThan("z", -9999999999L))));
    // similarly, the second predicate is always false
    Assert.assertEquals("Should simplify and expression to predicate",
        alwaysFalse(), Binder.bind(STRUCT, and(lessThan("y", 100), lessThan("z", -9999999999L))));

    Expression bound = Binder.bind(STRUCT, not(not(lessThan("y", 100))));
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    Assert.assertEquals("Should have the correct bound field", 1, pred.term().ref().fieldId());
  }

  @Test
  public void testTransformExpressionBinding() {
    Expression bound = Binder.bind(STRUCT, equal(bucket("x", 16), 10));
    TestHelpers.assertAllReferencesBound("BoundTransform", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    Assert.assertTrue("Should use a BoundTransform child", pred.term() instanceof BoundTransform);
    BoundTransform<?, ?> transformExpr = (BoundTransform<?, ?>) pred.term();
    Assert.assertEquals("Should use a bucket[16] transform", "bucket[16]", transformExpr.transform().toString());
  }
}
