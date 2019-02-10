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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import com.netflix.iceberg.types.Types;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Evaluates an {@link Expression} for data described by a {@link Types.StructType}.
 * <p>
 * Data rows must implement {@link StructLike} and are passed to {@link #eval(StructLike)}.
 * <p>
 * This class is thread-safe.
 */
public class Evaluator implements Serializable {
  private final Expression expr;
  private transient ThreadLocal<EvalVisitor> visitors = null;

  private EvalVisitor visitor() {
    if (visitors == null) {
      this.visitors = ThreadLocal.withInitial(EvalVisitor::new);
    }
    return visitors.get();
  }

  public Evaluator(Types.StructType struct, Expression unbound) {
    this.expr = Binder.bind(struct, unbound, true);
  }

  public boolean eval(StructLike data) {
    return visitor().eval(data);
  }

  private class EvalVisitor extends BoundExpressionVisitor<Boolean> {
    private StructLike struct;

    private boolean eval(StructLike row) {
      this.struct = row;
      return ExpressionVisitors.visit(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return true;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      return ref.get(struct) == null;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      return ref.get(struct) != null;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(ref.get(struct), lit.value()) < 0;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(ref.get(struct), lit.value()) <= 0;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(ref.get(struct), lit.value()) > 0;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(ref.get(struct), lit.value()) >= 0;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(ref.get(struct), lit.value()) == 0;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      return !eq(ref, lit);
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("In is not supported yet");
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Literal<T> lit) {
      return !in(ref, lit);
    }

    @Override
    public Boolean startsWith(BoundReference<String> ref, Literal<String> lit) {
      return ref.get(struct).startsWith(lit.value());
    }
  }
}
