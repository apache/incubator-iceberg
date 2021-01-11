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

/**
 * Represents a boolean expression tree.
 */
public interface Expression extends Serializable {
  enum Operation {
    TRUE,
    FALSE,
    IS_NULL,
    NOT_NULL,
    IS_NAN,
    NOT_NAN,
    LT,
    LT_EQ,
    GT,
    GT_EQ,
    EQ,
    NOT_EQ,
    IN,
    NOT_IN,
    NOT,
    AND,
    OR,
    STARTS_WITH,
    NOT_STARTS_WITH;

    /**
     * Returns the operation used when this is negated.
     */
    public Operation negate() {
      switch (this) {
        case IS_NULL:
          return Operation.NOT_NULL;
        case NOT_NULL:
          return Operation.IS_NULL;
        case IS_NAN:
          return Operation.NOT_NAN;
        case NOT_NAN:
          return Operation.IS_NAN;
        case LT:
          return Operation.GT_EQ;
        case LT_EQ:
          return Operation.GT;
        case GT:
          return Operation.LT_EQ;
        case GT_EQ:
          return Operation.LT;
        case EQ:
          return Operation.NOT_EQ;
        case NOT_EQ:
          return Operation.EQ;
        case IN:
          return Operation.NOT_IN;
        case NOT_IN:
          return Operation.IN;
        case STARTS_WITH:
          return Operation.NOT_STARTS_WITH;
        case NOT_STARTS_WITH:
          return Operation.STARTS_WITH;
        default:
          throw new IllegalArgumentException("No negation for operation: " + this);
      }
    }

    /**
     * Returns the equivalent operation when the left and right operands are exchanged.
     */
    // Allow flipLR as a name because it's a public API
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public Operation flipLR() {
      switch (this) {
        case LT:
          return Operation.GT;
        case LT_EQ:
          return Operation.GT_EQ;
        case GT:
          return Operation.LT;
        case GT_EQ:
          return Operation.LT_EQ;
        case EQ:
          return Operation.EQ;
        case NOT_EQ:
          return Operation.NOT_EQ;
        case AND:
          return Operation.AND;
        case OR:
          return Operation.OR;
        default:
          throw new IllegalArgumentException("No left-right flip for operation: " + this);
      }
    }
  }

  /**
   * Returns the operation for an expression node.
   */
  Operation op();

  /**
   * Returns the negation of this expression, equivalent to not(this).
   */
  default Expression negate() {
    throw new UnsupportedOperationException(String.format("%s cannot be negated", this));
  }
}
