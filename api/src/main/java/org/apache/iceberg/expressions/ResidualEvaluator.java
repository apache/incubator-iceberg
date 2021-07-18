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

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.util.NaNUtil;

/**
 * Finds the residuals for an {@link Expression} the partitions in the given {@link PartitionSpec}.
 * <p>
 * A residual expression is made by partially evaluating an expression using partition values. For
 * example, if a table is partitioned by day(utc_timestamp) and is read with a filter expression
 * utc_timestamp &gt;= a and utc_timestamp &lt;= b, then there are 4 possible residuals expressions
 * for the partition data, d:
 * <ul>
 * <li>If d &gt; day(a) and d &lt; day(b), the residual is always true</li>
 * <li>If d == day(a) and d != day(b), the residual is utc_timestamp &gt;= a</li>
 * <li>if d == day(b) and d != day(a), the residual is utc_timestamp &lt;= b</li>
 * <li>If d == day(a) == day(b), the residual is utc_timestamp &gt;= a and utc_timestamp &lt;= b
 * </li>
 * </ul>
 * <p>
 * Partition data is passed using {@link StructLike}. Residuals are returned by
 * {@link #residualFor(StructLike)}.
 * <p>
 * This class is thread-safe.
 */
public class ResidualEvaluator implements Serializable {
  private static class UnpartitionedResidualEvaluator extends ResidualEvaluator {
    private final Expression expr;

    UnpartitionedResidualEvaluator(Expression expr) {
      super(PartitionSpec.unpartitioned(), expr, false);
      this.expr = expr;
    }

    @Override
    public Expression residualFor(StructLike ignored) {
      return expr;
    }
  }

  /**
   * Return a residual evaluator for an unpartitioned {@link PartitionSpec spec}.
   *
   * @param expr an expression
   * @return a residual evaluator that always returns the expression
   */
  public static ResidualEvaluator unpartitioned(Expression expr) {
    return new UnpartitionedResidualEvaluator(expr);
  }

  /**
   * Return a residual evaluator for a {@link PartitionSpec spec} and {@link Expression expression}.
   *
   * @param spec a partition spec
   * @param expr an expression
   * @return a residual evaluator for the expression
   */
  public static ResidualEvaluator of(PartitionSpec spec, Expression expr, boolean caseSensitive) {
    if (spec.fields().size() > 0) {
      return new ResidualEvaluator(spec, expr, caseSensitive);
    } else {
      return unpartitioned(expr);
    }
  }

  private final PartitionSpec spec;
  private final Expression expr;
  private final boolean caseSensitive;

  private ResidualEvaluator(PartitionSpec spec, Expression expr, boolean caseSensitive) {
    this.spec = spec;
    this.expr = expr;
    this.caseSensitive = caseSensitive;
  }

  /**
   * Returns a residual expression for the given partition values.
   *
   * @param partitionData partition data values
   * @return the residual of this evaluator's expression from the partition values
   */
  public Expression residualFor(StructLike partitionData) {
    return new ResidualVisitor().eval(partitionData);
  }

  private class ResidualVisitor extends BoundExpressionVisitor<Expression> {
    private StructLike struct;

    private Expression eval(StructLike dataStruct) {
      this.struct = dataStruct;
      return ExpressionVisitors.visit(expr, this);
    }

    @Override
    public Expression alwaysTrue() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
      return Expressions.alwaysFalse();
    }

    @Override
    public Expression not(Expression result) {
      return Expressions.not(result);
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    @Override
    public <T> Expression isNull(BoundReference<T> ref) {
      return (ref.eval(struct) == null) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notNull(BoundReference<T> ref) {
      return (ref.eval(struct) != null) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression isNaN(BoundReference<T> ref) {
      return NaNUtil.isNaN(ref.eval(struct)) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notNaN(BoundReference<T> ref) {
      return NaNUtil.isNaN(ref.eval(struct)) ? alwaysFalse() : alwaysTrue();
    }

    @Override
    public <T> Expression lt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) < 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression ltEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) <= 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression gt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) > 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression gtEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) >= 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression eq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) == 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.eval(struct), lit.value()) != 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression in(BoundReference<T> ref, Set<T> literalSet) {
      return literalSet.contains(ref.eval(struct)) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notIn(BoundReference<T> ref, Set<T> literalSet) {
      return literalSet.contains(ref.eval(struct)) ? alwaysFalse() : alwaysTrue();
    }

    @Override
    public <T> Expression startsWith(BoundReference<T> ref, Literal<T> lit) {
      return ((String) ref.eval(struct)).startsWith((String) lit.value()) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      return ((String) ref.eval(struct)).startsWith((String) lit.value()) ? alwaysFalse() : alwaysTrue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(BoundPredicate<T> pred) {
      // Get the strict projection and inclusive projection of this predicate in partition data,
      // then use them to determine whether to return the original predicate. The strict projection
      // returns true iff the original predicate would have returned true, so the predicate can be
      // eliminated if the strict projection evaluates to true. Similarly the inclusive projection
      // returns false iff the original predicate would have returned false, so the predicate can
      // also be eliminated if the inclusive projection evaluates to false.

      // If there is no strict projection or if it evaluates to false, then return the predicate.
      List<PartitionField> parts = spec.getFieldsBySourceId(pred.ref().fieldId());
      if (parts == null) {
        return pred; // not associated inclusive a partition field, can't be evaluated
      }

      for (PartitionField part : parts) {

        // checking the strict projection
        UnboundPredicate<?> strictProjection = ((Transform<T, ?>) part.transform()).projectStrict(part.name(), pred);
        Expression strictResult = null;

        if (strictProjection != null) {
          Expression bound = strictProjection.bind(spec.partitionType(), caseSensitive);
          if (bound instanceof BoundPredicate) {
            strictResult = super.predicate((BoundPredicate<?>) bound);
          } else {
            // if the result is not a predicate, then it must be a constant like alwaysTrue or alwaysFalse
            strictResult = bound;
          }
        }

        if (strictResult != null && strictResult.op() == Expression.Operation.TRUE) {
          // If strict is true, returning true
          return Expressions.alwaysTrue();
        }

        // checking the inclusive projection
        UnboundPredicate<?> inclusiveProjection = ((Transform<T, ?>) part.transform()).project(part.name(), pred);
        Expression inclusiveResult = null;
        if (inclusiveProjection != null) {
          Expression boundInclusive = inclusiveProjection.bind(spec.partitionType(), caseSensitive);
          if (boundInclusive instanceof BoundPredicate) {
            // using predicate method specific to inclusive
            inclusiveResult = super.predicate((BoundPredicate<?>) boundInclusive);
          } else {
            // if the result is not a predicate, then it must be a constant like alwaysTrue or alwaysFalse
            inclusiveResult = boundInclusive;
          }
        }

        if (inclusiveResult != null && inclusiveResult.op() == Expression.Operation.FALSE) {
          // If inclusive is false, returning false
          return Expressions.alwaysFalse();
        }

      }

      // neither strict not inclusive predicate was conclusive, returning the original pred
      return pred;
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      Expression bound = pred.bind(spec.schema().asStruct(), caseSensitive);

      if (bound instanceof BoundPredicate) {
        Expression boundResidual = predicate((BoundPredicate<?>) bound);
        if (boundResidual instanceof Predicate) {
          return pred; // replace inclusive original unbound predicate
        }
        return boundResidual; // use the non-predicate residual (e.g. alwaysTrue)
      }

      // if binding didn't result in a Predicate, return the expression
      return bound;
    }
  }
}
