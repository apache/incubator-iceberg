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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.CharSequenceSet;

public class UnboundPredicate<T> extends Predicate<T, UnboundTerm<T>> implements Unbound<T, Expression> {
  private static final Joiner COMMA = Joiner.on(", ");

  private final List<Literal<T>> literals;

  UnboundPredicate(Operation op, UnboundTerm<T> term, T value) {
    this(op, term, Literals.from(value));
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term) {
    super(op, term);
    this.literals = null;
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term, Literal<T> lit) {
    super(op, term);
    this.literals = Lists.newArrayList(lit);
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term, Iterable<T> values) {
    super(op, term);
    this.literals = Lists.newArrayList(Iterables.transform(values, Literals::from));
  }

  private UnboundPredicate(Operation op, UnboundTerm<T> term, List<Literal<T>> literals) {
    super(op, term);
    this.literals = literals;
  }

  @Override
  public NamedReference<?> ref() {
    return term().ref();
  }

  @Override
  public Expression negate() {
    return new UnboundPredicate<>(op().negate(), term(), literals);
  }

  public Literal<T> literal() {
    Preconditions.checkArgument(op() != Operation.IN && op() != Operation.NOT_IN,
        "%s predicate cannot return a literal", op());
    return literals == null ? null : Iterables.getOnlyElement(literals);
  }

  public List<Literal<T>> literals() {
    return literals;
  }

  /**
   * Bind this UnboundPredicate, defaulting to case sensitive mode.
   *
   * Access modifier is package-private, to only allow use from existing tests.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  Expression bind(StructType struct) {
    return bind(struct, true);
  }

  /**
   * Bind this UnboundPredicate.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  @Override
  public Expression bind(StructType struct, boolean caseSensitive) {
    BoundTerm<T> bound = term().bind(struct, caseSensitive);

    if (literals == null) {
      return bindUnaryOperation(bound);
    }

    if (op() == Operation.IN || op() == Operation.NOT_IN) {
      return bindInOperation(bound);
    }

    return bindLiteralOperation(bound);
  }

  private Expression bindUnaryOperation(BoundTerm<T> boundTerm) {
    switch (op()) {
      case IS_NULL:
        if (boundTerm.ref().field().isRequired()) {
          return Expressions.alwaysFalse();
        }
        return new BoundUnaryPredicate<>(Operation.IS_NULL, boundTerm);
      case NOT_NULL:
        if (boundTerm.ref().field().isRequired()) {
          return Expressions.alwaysTrue();
        }
        return new BoundUnaryPredicate<>(Operation.NOT_NULL, boundTerm);
      case IS_NAN:
        if (floatingType(boundTerm.type().typeId())) {
          return new BoundUnaryPredicate<>(Operation.IS_NAN, boundTerm);
        } else {
          throw new ValidationException("IsNaN cannot be used with a non-floating-point column");
        }
      case NOT_NAN:
        if (floatingType(boundTerm.type().typeId())) {
          return new BoundUnaryPredicate<>(Operation.NOT_NAN, boundTerm);
        } else {
          throw new ValidationException("NotNaN cannot be used with a non-floating-point column");
        }
      default:
        throw new ValidationException("Operation must be IS_NULL, NOT_NULL, IS_NAN, or NOT_NAN");
    }
  }

  private boolean floatingType(Type.TypeID typeID) {
    return Type.TypeID.DOUBLE.equals(typeID) || Type.TypeID.FLOAT.equals(typeID);
  }

  private Expression bindLiteralOperation(BoundTerm<T> boundTerm) {
    Literal<T> lit = literal().to(boundTerm.type());

    if (lit == null) {
      throw new ValidationException("Invalid value for conversion to type %s: %s (%s)",
          boundTerm.type(), literal().value(), literal().value().getClass().getName());

    } else if (lit == Literals.aboveMax()) {
      switch (op()) {
        case LT:
        case LT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case GT:
        case GT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    } else if (lit == Literals.belowMin()) {
      switch (op()) {
        case GT:
        case GT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case LT:
        case LT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    }

    // TODO: translate truncate(col) == value to startsWith(value)
    return new BoundLiteralPredicate<>(op(), boundTerm, lit);
  }

  private Expression bindInOperation(BoundTerm<T> boundTerm) {
    List<Literal<T>> convertedLiterals = Lists.newArrayList(Iterables.filter(
        Lists.transform(literals, lit -> {
          Literal<T> converted = lit.to(boundTerm.type());
          ValidationException.check(converted != null,
              "Invalid value for conversion to type %s: %s (%s)", boundTerm.type(), lit, lit.getClass().getName());
          return converted;
        }),
        lit -> lit != Literals.aboveMax() && lit != Literals.belowMin()));

    if (convertedLiterals.isEmpty()) {
      switch (op()) {
        case IN:
          return Expressions.alwaysFalse();
        case NOT_IN:
          return Expressions.alwaysTrue();
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    Set<T> literalSet = setOf(convertedLiterals);
    if (literalSet.size() == 1) {
      switch (op()) {
        case IN:
          return new BoundLiteralPredicate<>(Operation.EQ, boundTerm, Iterables.get(convertedLiterals, 0));
        case NOT_IN:
          return new BoundLiteralPredicate<>(Operation.NOT_EQ, boundTerm, Iterables.get(convertedLiterals, 0));
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    return new BoundSetPredicate<>(op(), boundTerm, literalSet);
  }

  @Override
  public String toString() {
    switch (op()) {
      case IS_NULL:
        return "is_null(" + term() + ")";
      case NOT_NULL:
        return "not_null(" + term() + ")";
      case IS_NAN:
        return "is_nan(" + term() + ")";
      case NOT_NAN:
        return "not_nan(" + term() + ")";
      case LT:
        return term() + " < " + literal();
      case LT_EQ:
        return term() + " <= " + literal();
      case GT:
        return term() + " > " + literal();
      case GT_EQ:
        return term() + " >= " + literal();
      case EQ:
        return term() + " == " + literal();
      case NOT_EQ:
        return term() + " != " + literal();
      case STARTS_WITH:
        return term() + " startsWith \"" + literal() + "\"";
      case NOT_STARTS_WITH:
        return term() + " notStartsWith \"" + literal() + "\"";
      case IN:
        return term() + " in (" + COMMA.join(literals()) + ")";
      case NOT_IN:
        return term() + " not in (" + COMMA.join(literals()) + ")";
      default:
        return "Invalid predicate: operation = " + op();
    }
  }

  @SuppressWarnings("unchecked")
  static <T> Set<T> setOf(Iterable<Literal<T>> literals) {
    Literal<T> lit = Iterables.get(literals, 0);
    if (lit instanceof Literals.StringLiteral && lit.value() instanceof CharSequence) {
      Iterable<T> values = Iterables.transform(literals, Literal::value);
      Iterable<CharSequence> charSeqs = Iterables.transform(values, val -> (CharSequence) val);
      return (Set<T>) CharSequenceSet.of(charSeqs);
    } else {
      return Sets.newHashSet(Iterables.transform(literals, Literal::value));
    }
  }
}
