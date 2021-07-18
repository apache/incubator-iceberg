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

import java.util.Comparator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BoundLiteralPredicate<T> extends BoundPredicate<T> {
  private final Literal<T> literal;

  BoundLiteralPredicate(Operation op, BoundTerm<T> term, Literal<T> lit) {
    super(op, term);
    Preconditions.checkArgument(op != Operation.IN && op != Operation.NOT_IN,
        "Bound literal predicate does not support operation: %s", op);
    this.literal = lit;
  }

  @Override
  public Expression negate() {
    return new BoundLiteralPredicate<>(op().negate(), term(), literal);
  }

  public Literal<T> literal() {
    return literal;
  }

  @Override
  public boolean isLiteralPredicate() {
    return true;
  }

  @Override
  public BoundLiteralPredicate<T> asLiteralPredicate() {
    return this;
  }

  @Override
  public boolean test(T value) {
    Comparator<T> cmp = literal.comparator();
    switch (op()) {
      case LT:
        return cmp.compare(value, literal.value()) < 0;
      case LT_EQ:
        return cmp.compare(value, literal.value()) <= 0;
      case GT:
        return cmp.compare(value, literal.value()) > 0;
      case GT_EQ:
        return cmp.compare(value, literal.value()) >= 0;
      case EQ:
        return cmp.compare(value, literal.value()) == 0;
      case NOT_EQ:
        return cmp.compare(value, literal.value()) != 0;
      case STARTS_WITH:
        return String.valueOf(value).startsWith((String) literal.value());
      case NOT_STARTS_WITH:
        return !String.valueOf(value).startsWith((String) literal.value());
      default:
        throw new IllegalStateException("Invalid operation for BoundLiteralPredicate: " + op());
    }
  }

  @Override
  public String toString() {
    switch (op()) {
      case LT:
        return term() + " < " + literal;
      case LT_EQ:
        return term() + " <= " + literal;
      case GT:
        return term() + " > " + literal;
      case GT_EQ:
        return term() + " >= " + literal;
      case EQ:
        return term() + " == " + literal;
      case NOT_EQ:
        return term() + " != " + literal;
      case STARTS_WITH:
        return term() + " startsWith \"" + literal + "\"";
      case NOT_STARTS_WITH:
        return term() + " notStartsWith \"" + literal + "\"";
      case IN:
        return term() + " in { " + literal + " }";
      case NOT_IN:
        return term() + " not in { " + literal + " }";
      default:
        return "Invalid literal predicate: operation = " + op();
    }
  }
}
