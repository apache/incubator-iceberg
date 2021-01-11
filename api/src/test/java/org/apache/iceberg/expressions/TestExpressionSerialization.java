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

import java.util.Collection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionSerialization {
  @Test
  public void testExpressions() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(34, "a", Types.IntegerType.get()),
        Types.NestedField.required(35, "s", Types.StringType.get())
    );

    Expression[] expressions = new Expression[] {
        Expressions.alwaysFalse(),
        Expressions.alwaysTrue(),
        Expressions.lessThan("x", 5),
        Expressions.lessThanOrEqual("y", -3),
        Expressions.greaterThan("z", 0),
        Expressions.greaterThanOrEqual("t", 129),
        Expressions.equal("col", "data"),
        Expressions.in("col", "a", "b"),
        Expressions.notIn("col", 1, 2, 3),
        Expressions.notEqual("col", "abc"),
        Expressions.notNull("maybeNull"),
        Expressions.isNull("maybeNull2"),
        Expressions.isNaN("maybeNaN"),
        Expressions.notNaN("maybeNaN2"),
        Expressions.startsWith("col", "abc"),
        Expressions.notStartsWith("col", "abc"),
        Expressions.not(Expressions.greaterThan("a", 10)),
        Expressions.and(Expressions.greaterThanOrEqual("a", 0), Expressions.lessThan("a", 3)),
        Expressions.or(Expressions.lessThan("a", 0), Expressions.greaterThan("a", 10)),
        Expressions.equal("a", 5).bind(schema.asStruct()),
        Expressions.in("a", 5, 6, 7).bind(schema.asStruct()),
        Expressions.notIn("s", "abc", "xyz").bind(schema.asStruct()),
        Expressions.isNull("a").bind(schema.asStruct()),
        Expressions.startsWith("s", "abc").bind(schema.asStruct()),
        Expressions.notStartsWith("s", "xyz").bind(schema.asStruct())
    };

    for (Expression expression : expressions) {
      Expression copy = TestHelpers.roundTripSerialize(expression);
      Assert.assertTrue(
          "Expression should equal the deserialized copy: " + expression + " != " + copy,
          equals(expression, copy));
    }
  }

  // You may be wondering why this isn't implemented as Expression.equals. The reason is that
  // expression equality implies equivalence, which is wider than structural equality. For example,
  // lessThan("a", 3) is equivalent to not(greaterThanOrEqual("a", 4)). To avoid confusion, equals
  // only guarantees object identity.

  private static boolean equals(Expression left, Expression right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (left instanceof Predicate) {
      if (!(left.getClass().isInstance(right))) {
        return false;
      }
      return equals((Predicate) left, (Predicate) right);
    }

    switch (left.op()) {
      case FALSE:
      case TRUE:
        return true;
      case NOT:
        return equals(((Not) left).child(), ((Not) right).child());
      case AND:
        return equals(((And) left).left(), ((And) right).left()) &&
            equals(((And) left).right(), ((And) right).right());
      case OR:
        return equals(((Or) left).left(), ((Or) right).left()) &&
            equals(((Or) left).right(), ((Or) right).right());
      default:
        return false;
    }
  }

  private static boolean equals(Term left, Term right) {
    if (left instanceof Reference && right instanceof Reference) {
      return equals((Reference<?>) left, (Reference<?>) right);
    } else if (left instanceof UnboundTransform && right instanceof UnboundTransform) {
      UnboundTransform<?, ?> unboundLeft = (UnboundTransform<?, ?>) left;
      UnboundTransform<?, ?> unboundRight = (UnboundTransform<?, ?>) right;
      if (equals(unboundLeft.ref(), unboundRight.ref()) &&
          unboundLeft.transform().toString().equals(unboundRight.transform().toString())) {
        return true;
      }

    } else if (left instanceof BoundTransform && right instanceof BoundTransform) {
      BoundTransform<?, ?> boundLeft = (BoundTransform<?, ?>) left;
      BoundTransform<?, ?> boundRight = (BoundTransform<?, ?>) right;
      if (equals(boundLeft.ref(), boundRight.ref()) &&
          boundLeft.transform().toString().equals(boundRight.transform().toString())) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
  private static boolean equals(Predicate left, Predicate right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (!equals(left.term(), right.term())) {
      return false;
    }

    if (left.op() == Operation.IS_NULL || left.op() == Operation.NOT_NULL ||
        left.op() == Operation.IS_NAN || left.op() == Operation.NOT_NAN) {
      return true;
    }

    if (left.getClass() != right.getClass()) {
      return false;
    }

    if (left instanceof UnboundPredicate) {
      UnboundPredicate lpred = (UnboundPredicate) left;
      UnboundPredicate rpred = (UnboundPredicate) right;
      if (left.op() == Operation.IN || left.op() == Operation.NOT_IN) {
        return equals(lpred.literals(), rpred.literals());
      }
      return lpred.literal().comparator()
          .compare(lpred.literal().value(), rpred.literal().value()) == 0;

    } else if (left instanceof BoundPredicate) {
      BoundPredicate lpred = (BoundPredicate) left;
      BoundPredicate rpred = (BoundPredicate) right;
      if (lpred.isLiteralPredicate() && rpred.isLiteralPredicate()) {
        return lpred.asLiteralPredicate().literal().comparator()
            .compare(lpred.asLiteralPredicate().literal().value(), rpred.asLiteralPredicate().literal().value()) == 0;
      } else if (lpred.isSetPredicate() && rpred.isSetPredicate()) {
        return equals(lpred.asSetPredicate().literalSet(), rpred.asSetPredicate().literalSet());
      } else {
        return lpred.isUnaryPredicate() && rpred.isUnaryPredicate();
      }

    } else {
      throw new UnsupportedOperationException(String.format(
          "Predicate equality check for %s is not supported", left.getClass()));
    }
  }

  private static boolean equals(Collection<Literal<?>> left, Collection<Literal<?>> right) {
    if (left.size() != right.size()) {
      return false;
    }
    return left.containsAll(right);
  }

  private static boolean equals(Reference<?> left, Reference<?> right) {
    if (left instanceof NamedReference) {
      if (!(right instanceof NamedReference)) {
        return false;
      }

      NamedReference lref = (NamedReference) left;
      NamedReference rref = (NamedReference) right;

      return lref.name().equals(rref.name());
    } else if (left instanceof BoundReference) {
      if (!(right instanceof BoundReference)) {
        return false;
      }

      BoundReference lref = (BoundReference) left;
      BoundReference rref = (BoundReference) right;

      return lref.fieldId() == rref.fieldId() &&
          lref.type().equals(rref.type());
    }

    return false;
  }
}
