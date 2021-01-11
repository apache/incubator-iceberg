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

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * Utils for traversing {@link Expression expressions}.
 */
public class ExpressionVisitors {

  private ExpressionVisitors() {
  }

  public abstract static class ExpressionVisitor<R> {
    public R alwaysTrue() {
      return null;
    }

    public R alwaysFalse() {
      return null;
    }

    public R not(R result) {
      return null;
    }

    public R and(R leftResult, R rightResult) {
      return null;
    }

    public R or(R leftResult, R rightResult) {
      return null;
    }

    public <T> R predicate(BoundPredicate<T> pred) {
      return null;
    }

    public <T> R predicate(UnboundPredicate<T> pred) {
      return null;
    }
  }

  /**
   * This base class is used by existing visitors that have not been updated to extend BoundExpressionVisitor.
   *
   * @deprecated use {@link BoundVisitor} instead
   */
  @Deprecated
  public abstract static class BoundExpressionVisitor<R> extends ExpressionVisitor<R> {
    public <T> R isNull(BoundReference<T> ref) {
      return null;
    }

    public <T> R notNull(BoundReference<T> ref) {
      return null;
    }

    public <T> R isNaN(BoundReference<T> ref) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement isNaN");
    }

    public <T> R notNaN(BoundReference<T> ref) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement notNaN");
    }

    public <T> R lt(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R ltEq(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R gt(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R gtEq(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R eq(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R notEq(BoundReference<T> ref, Literal<T> lit) {
      return null;
    }

    public <T> R in(BoundReference<T> ref, Set<T> literalSet) {
      throw new UnsupportedOperationException("In operation is not supported by the visitor");
    }

    public <T> R notIn(BoundReference<T> ref, Set<T> literalSet) {
      throw new UnsupportedOperationException("notIn operation is not supported by the visitor");
    }

    public <T> R startsWith(BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("startsWith operation is not supported by the visitor");
    }

    public <T> R notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("notStartsWith operation is not supported by the visitor");
    }

    @Override
    public <T> R predicate(BoundPredicate<T> pred) {
      ValidationException.check(pred.term() instanceof BoundReference,
          "Visitor %s does not support expression: %s", this, pred.term());

      if (pred.isLiteralPredicate()) {
        BoundLiteralPredicate<T> literalPred = pred.asLiteralPredicate();
        switch (pred.op()) {
          case LT:
            return lt((BoundReference<T>) pred.term(), literalPred.literal());
          case LT_EQ:
            return ltEq((BoundReference<T>) pred.term(), literalPred.literal());
          case GT:
            return gt((BoundReference<T>) pred.term(), literalPred.literal());
          case GT_EQ:
            return gtEq((BoundReference<T>) pred.term(), literalPred.literal());
          case EQ:
            return eq((BoundReference<T>) pred.term(), literalPred.literal());
          case NOT_EQ:
            return notEq((BoundReference<T>) pred.term(), literalPred.literal());
          case STARTS_WITH:
            return startsWith((BoundReference<T>) pred.term(),  literalPred.literal());
          case NOT_STARTS_WITH:
            return notStartsWith((BoundReference<T>) pred.term(),  literalPred.literal());
          default:
            throw new IllegalStateException("Invalid operation for BoundLiteralPredicate: " + pred.op());
        }

      } else if (pred.isUnaryPredicate()) {
        switch (pred.op()) {
          case IS_NULL:
            return isNull((BoundReference<T>) pred.term());
          case NOT_NULL:
            return notNull((BoundReference<T>) pred.term());
          case IS_NAN:
            return isNaN((BoundReference<T>) pred.term());
          case NOT_NAN:
            return notNaN((BoundReference<T>) pred.term());
          default:
            throw new IllegalStateException("Invalid operation for BoundUnaryPredicate: " + pred.op());
        }

      } else if (pred.isSetPredicate()) {
        switch (pred.op()) {
          case IN:
            return in((BoundReference<T>) pred.term(), pred.asSetPredicate().literalSet());
          case NOT_IN:
            return notIn((BoundReference<T>) pred.term(), pred.asSetPredicate().literalSet());
          default:
            throw new IllegalStateException("Invalid operation for BoundSetPredicate: " + pred.op());
        }
      }

      throw new IllegalStateException("Unsupported bound predicate: " + pred.getClass().getName());
    }

    @Override
    public <T> R predicate(UnboundPredicate<T> pred) {
      throw new UnsupportedOperationException("Not a bound predicate: " + pred);
    }
  }

  public abstract static class BoundVisitor<R> extends ExpressionVisitor<R> {
    public <T> R isNull(Bound<T> expr) {
      return null;
    }

    public <T> R notNull(Bound<T> expr) {
      return null;
    }

    public <T> R isNaN(Bound<T> expr) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement isNaN");
    }

    public <T> R notNaN(Bound<T> expr) {
      throw new UnsupportedOperationException(this.getClass().getName() + " does not implement notNaN");
    }

    public <T> R lt(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R ltEq(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R gt(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R gtEq(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R eq(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R notEq(Bound<T> expr, Literal<T> lit) {
      return null;
    }

    public <T> R in(Bound<T> expr, Set<T> literalSet) {
      throw new UnsupportedOperationException("In operation is not supported by the visitor");
    }

    public <T> R notIn(Bound<T> expr, Set<T> literalSet) {
      throw new UnsupportedOperationException("notIn operation is not supported by the visitor");
    }

    public <T> R startsWith(Bound<T> expr, Literal<T> lit) {
      throw new UnsupportedOperationException("Unsupported operation.");
    }

    public <T> R notStartsWith(Bound<T> expr, Literal<T> lit) {
      throw new UnsupportedOperationException("Unsupported operation.");
    }

    @Override
    public <T> R predicate(BoundPredicate<T> pred) {
      if (pred.isLiteralPredicate()) {
        BoundLiteralPredicate<T> literalPred = pred.asLiteralPredicate();
        switch (pred.op()) {
          case LT:
            return lt(pred.term(), literalPred.literal());
          case LT_EQ:
            return ltEq(pred.term(), literalPred.literal());
          case GT:
            return gt(pred.term(), literalPred.literal());
          case GT_EQ:
            return gtEq(pred.term(), literalPred.literal());
          case EQ:
            return eq(pred.term(), literalPred.literal());
          case NOT_EQ:
            return notEq(pred.term(), literalPred.literal());
          case STARTS_WITH:
            return startsWith(pred.term(),  literalPred.literal());
          case NOT_STARTS_WITH:
            return notStartsWith(pred.term(), literalPred.literal());
          default:
            throw new IllegalStateException("Invalid operation for BoundLiteralPredicate: " + pred.op());
        }

      } else if (pred.isUnaryPredicate()) {
        switch (pred.op()) {
          case IS_NULL:
            return isNull(pred.term());
          case NOT_NULL:
            return notNull(pred.term());
          case IS_NAN:
            return isNaN(pred.term());
          case NOT_NAN:
            return notNaN(pred.term());
          default:
            throw new IllegalStateException("Invalid operation for BoundUnaryPredicate: " + pred.op());
        }

      } else if (pred.isSetPredicate()) {
        switch (pred.op()) {
          case IN:
            return in(pred.term(), pred.asSetPredicate().literalSet());
          case NOT_IN:
            return notIn(pred.term(), pred.asSetPredicate().literalSet());
          default:
            throw new IllegalStateException("Invalid operation for BoundSetPredicate: " + pred.op());
        }
      }

      throw new IllegalStateException("Unsupported bound predicate: " + pred.getClass().getName());
    }

    @Override
    public <T> R predicate(UnboundPredicate<T> pred) {
      throw new UnsupportedOperationException("Not a bound predicate: " + pred);
    }
  }

  /**
   * Traverses the given {@link Expression expression} with a {@link ExpressionVisitor visitor}.
   * <p>
   * The visitor will be called to handle each node in the expression tree in postfix order. Result
   * values produced by child nodes are passed when parent nodes are handled.
   *
   * @param expr an expression to traverse
   * @param visitor a visitor that will be called to handle each node in the expression tree
   * @param <R> the return type produced by the expression visitor
   * @return the value returned by the visitor for the root expression node
   */
  public static <R> R visit(Expression expr, ExpressionVisitor<R> visitor) {
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        return visitor.predicate((BoundPredicate<?>) expr);
      } else {
        return visitor.predicate((UnboundPredicate<?>) expr);
      }
    } else {
      switch (expr.op()) {
        case TRUE:
          return visitor.alwaysTrue();
        case FALSE:
          return visitor.alwaysFalse();
        case NOT:
          Not not = (Not) expr;
          return visitor.not(visit(not.child(), visitor));
        case AND:
          And and = (And) expr;
          return visitor.and(visit(and.left(), visitor), visit(and.right(), visitor));
        case OR:
          Or or = (Or) expr;
          return visitor.or(visit(or.left(), visitor), visit(or.right(), visitor));
        default:
          throw new UnsupportedOperationException(
              "Unknown operation: " + expr.op());
      }
    }
  }

  /**
   * Traverses the given {@link Expression expression} with a {@link ExpressionVisitor visitor}.
   * <p>
   * The visitor will be called to handle only nodes required for determining result
   * in the expression tree in postfix order. Result values produced by child nodes
   * are passed when parent nodes are handled.
   *
   * @param expr an expression to traverse
   * @param visitor a visitor that will be called to handle each node in the expression tree
   * @return the value returned by the visitor for the root expression node
   */
  public static Boolean visitEvaluator(Expression expr, ExpressionVisitor<Boolean> visitor) {
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        return visitor.predicate((BoundPredicate<?>) expr);
      } else {
        return visitor.predicate((UnboundPredicate<?>) expr);
      }
    } else {
      switch (expr.op()) {
        case TRUE:
          return visitor.alwaysTrue();
        case FALSE:
          return visitor.alwaysFalse();
        case NOT:
          Not not = (Not) expr;
          return visitor.not(visitEvaluator(not.child(), visitor));
        case AND:
          And and = (And) expr;
          Boolean andLeftOperand = visitEvaluator(and.left(), visitor);
          if (!andLeftOperand) {
            return visitor.alwaysFalse();
          }
          return visitor.and(Boolean.TRUE, visitEvaluator(and.right(), visitor));
        case OR:
          Or or = (Or) expr;
          Boolean orLeftOperand = visitEvaluator(or.left(), visitor);
          if (orLeftOperand) {
            return visitor.alwaysTrue();
          }
          return visitor.or(Boolean.FALSE, visitEvaluator(or.right(), visitor));
        default:
          throw new UnsupportedOperationException(
              "Unknown operation: " + expr.op());
      }
    }
  }
}
