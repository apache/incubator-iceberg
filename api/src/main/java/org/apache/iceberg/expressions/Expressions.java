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

import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

/**
 * Factory methods for creating {@link Expression expressions}.
 */
public class Expressions {
  private Expressions() {
  }

  public static Expression and(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysFalse() || right == alwaysFalse()) {
      return alwaysFalse();
    } else if (left == alwaysTrue()) {
      return right;
    } else if (right == alwaysTrue()) {
      return left;
    }
    return new And(left, right);
  }

  public static Expression and(Expression left, Expression right, Expression... expressions) {
    return Stream.of(expressions)
      .reduce(and(left, right), Expressions::and);
  }

  public static Expression or(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysTrue() || right == alwaysTrue()) {
      return alwaysTrue();
    } else if (left == alwaysFalse()) {
      return right;
    } else if (right == alwaysFalse()) {
      return left;
    }
    return new Or(left, right);
  }

  public static Expression not(Expression child) {
    Preconditions.checkNotNull(child, "Child expression cannot be null.");
    if (child == alwaysTrue()) {
      return alwaysFalse();
    } else if (child == alwaysFalse()) {
      return alwaysTrue();
    } else if (child instanceof Not) {
      return ((Not) child).child();
    }
    return new Not(child);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> bucket(String name, int numBuckets) {
    Transform<?, T> transform = (Transform<?, T>) Transforms.bucket(Types.StringType.get(), numBuckets);
    return new UnboundTransform<>(ref(name), transform);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> year(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.year(Types.TimestampType.withZone()));
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> month(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.month(Types.TimestampType.withZone()));
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> day(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.day(Types.TimestampType.withZone()));
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> hour(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.hour(Types.TimestampType.withZone()));
  }

  public static <T> UnboundTerm<T> truncate(String name, int width) {
    return new UnboundTransform<>(ref(name), Transforms.truncate(Types.LongType.get(), width));
  }

  public static <T> UnboundPredicate<T> isNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.IS_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> isNull(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.IS_NULL, expr);
  }

  public static <T> UnboundPredicate<T> notNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> notNull(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NULL, expr);
  }

  public static <T> UnboundPredicate<T> isNaN(String name) {
    return new UnboundPredicate<>(Expression.Operation.IS_NAN, ref(name));
  }

  public static <T> UnboundPredicate<T> isNaN(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.IS_NAN, expr);
  }

  public static <T> UnboundPredicate<T> notNaN(String name) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NAN, ref(name));
  }

  public static <T> UnboundPredicate<T> notNaN(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NAN, expr);
  }

  public static <T> UnboundPredicate<T> lessThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> lessThan(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT, expr, value);
  }

  public static <T> UnboundPredicate<T> lessThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> lessThanOrEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT_EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> greaterThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThan(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT, expr, value);
  }

  public static <T> UnboundPredicate<T> greaterThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThanOrEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT_EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> equal(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> equal(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> notEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> notEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_EQ, expr, value);
  }

  public static UnboundPredicate<String> startsWith(String name, String value) {
    return new UnboundPredicate<>(Expression.Operation.STARTS_WITH, ref(name), value);
  }

  public static UnboundPredicate<String> startsWith(UnboundTerm<String> expr, String value) {
    return new UnboundPredicate<>(Expression.Operation.STARTS_WITH, expr, value);
  }

  public static UnboundPredicate<String> notStartsWith(String name, String value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_STARTS_WITH, ref(name), value);
  }

  public static UnboundPredicate<String> notStartsWith(UnboundTerm<String> expr, String value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_STARTS_WITH, expr, value);
  }

  public static <T> UnboundPredicate<T> in(String name, T... values) {
    return predicate(Operation.IN, name, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> in(UnboundTerm<T> expr, T... values) {
    return predicate(Operation.IN, expr, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> in(String name, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for IN predicate.");
    return predicate(Operation.IN, ref(name), values);
  }

  public static <T> UnboundPredicate<T> in(UnboundTerm<T> expr, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for IN predicate.");
    return predicate(Operation.IN, expr, values);
  }

  public static <T> UnboundPredicate<T> notIn(String name, T... values) {
    return predicate(Operation.NOT_IN, name, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> notIn(UnboundTerm<T> expr, T... values) {
    return predicate(Operation.NOT_IN, expr, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> notIn(String name, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for NOT_IN predicate.");
    return predicate(Operation.NOT_IN, name, values);
  }

  public static <T> UnboundPredicate<T> notIn(UnboundTerm<T> expr, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for NOT_IN predicate.");
    return predicate(Operation.NOT_IN, expr, values);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, T value) {
    return predicate(op, name, Literals.from(value));
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, Literal<T> lit) {
    Preconditions.checkArgument(
        op != Operation.IS_NULL && op != Operation.NOT_NULL && op != Operation.IS_NAN && op != Operation.NOT_NAN,
        "Cannot create %s predicate inclusive a value", op);
    return new UnboundPredicate<T>(op, ref(name), lit);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, Iterable<T> values) {
    return predicate(op, ref(name), values);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name) {
    Preconditions.checkArgument(
        op == Operation.IS_NULL || op == Operation.NOT_NULL || op == Operation.IS_NAN || op == Operation.NOT_NAN,
        "Cannot create %s predicate without a value", op);
    return new UnboundPredicate<>(op, ref(name));
  }

  private static <T> UnboundPredicate<T> predicate(Operation op, UnboundTerm<T> expr, Iterable<T> values) {
    return new UnboundPredicate<>(op, expr, values);
  }

  public static True alwaysTrue() {
    return True.INSTANCE;
  }

  public static False alwaysFalse() {
    return False.INSTANCE;
  }

  public static Expression rewriteNot(Expression expr) {
    return ExpressionVisitors.visit(expr, RewriteNot.get());
  }

  /**
   * Constructs a reference for a given column.
   * <p>
   * The following are equivalent: equals("a", 5) and equals(ref("a"), 5).
   *
   * @param name a column name
   * @param <T> the Java type of this reference
   * @return a named reference
   */
  public static <T> NamedReference<T> ref(String name) {
    return new NamedReference<>(name);
  }

  /**
   * Constructs a transform expression for a given column.
   *
   * @param name a column name
   * @param transform a transform function
   * @param <T> the Java type of this term
   * @return an unbound transform expression
   */
  public static <T> UnboundTerm<T> transform(String name, Transform<?, T> transform) {
    return new UnboundTransform<>(ref(name), transform);
  }
}
