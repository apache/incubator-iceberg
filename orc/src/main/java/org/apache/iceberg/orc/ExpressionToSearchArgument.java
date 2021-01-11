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

package org.apache.iceberg.orc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

class ExpressionToSearchArgument extends ExpressionVisitors.BoundVisitor<ExpressionToSearchArgument.Action> {

  static SearchArgument convert(Expression expr, TypeDescription readSchema) {
    Map<Integer, String> idToColumnName = ORCSchemaUtil.idToOrcName(ORCSchemaUtil.convert(readSchema));
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    ExpressionVisitors.visit(expr, new ExpressionToSearchArgument(builder, idToColumnName)).invoke();
    return builder.build();
  }

  // Currently every predicate in ORC requires a PredicateLeaf.Type field which is not available for these Iceberg types
  private static final Set<TypeID> UNSUPPORTED_TYPES = ImmutableSet.of(
      TypeID.BINARY,
      TypeID.FIXED,
      TypeID.UUID,
      TypeID.STRUCT,
      TypeID.MAP,
      TypeID.LIST
  );

  private SearchArgument.Builder builder;
  private Map<Integer, String> idToColumnName;

  private ExpressionToSearchArgument(SearchArgument.Builder builder, Map<Integer, String> idToColumnName) {
    this.builder = builder;
    this.idToColumnName = idToColumnName;
  }

  @Override
  public Action alwaysTrue() {
    return () -> this.builder.literal(TruthValue.YES);
  }

  @Override
  public Action alwaysFalse() {
    return () -> this.builder.literal(TruthValue.NO);
  }

  @Override
  public Action not(Action child) {
    return () -> {
      this.builder.startNot();
      child.invoke();
      this.builder.end();
    };
  }

  @Override
  public Action and(Action leftChild, Action rightChild) {
    return () -> {
      this.builder.startAnd();
      leftChild.invoke();
      rightChild.invoke();
      this.builder.end();
    };
  }

  @Override
  public Action or(Action leftChild, Action rightChild) {
    return () -> {
      this.builder.startOr();
      leftChild.invoke();
      rightChild.invoke();
      this.builder.end();
    };
  }

  @Override
  public <T> Action isNull(Bound<T> expr) {
    return () -> this.builder.isNull(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()));
  }

  @Override
  public <T> Action notNull(Bound<T> expr) {
    return () -> this.builder.startNot()
        .isNull(idToColumnName.get(expr.ref().fieldId()),
            type(expr.ref().type()))
        .end();
  }

  @Override
  public <T> Action isNaN(Bound<T> expr) {
    return () -> this.builder.equals(
        idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), getNaNForType(expr.ref().type())));
  }

  private Object getNaNForType(Type type) {
    switch (type.typeId()) {
      case FLOAT:
        return Float.NaN;
      case DOUBLE:
        return Double.NaN;
      default:
        throw new IllegalArgumentException("Cannot get NaN value for type " + type.typeId());
    }
  }

  @Override
  public <T> Action notNaN(Bound<T> expr) {
    return () -> {
      this.builder.startOr();
      isNull(expr).invoke();
      this.builder.startNot();
      isNaN(expr).invoke();
      this.builder.end(); // end NOT
      this.builder.end(); // end OR
    };
  }

  @Override
  public <T> Action lt(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.lessThan(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action ltEq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.lessThanEquals(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action gt(Bound<T> expr, Literal<T> lit) {
    // ORC SearchArguments do not have a greaterThan predicate, so we use not(lessThanOrEquals)
    // e.g. x > 5 => not(x <= 5)
    return () -> this.builder.startNot()
          .lessThanEquals(idToColumnName.get(expr.ref().fieldId()),
              type(expr.ref().type()),
              literal(expr.ref().type(), lit.value()))
          .end();
  }

  @Override
  public <T> Action gtEq(Bound<T> expr, Literal<T> lit) {
    // ORC SearchArguments do not have a greaterThanOrEquals predicate, so we use not(lessThan)
    // e.g. x >= 5 => not(x < 5)
    return () -> this.builder.startNot()
          .lessThan(idToColumnName.get(expr.ref().fieldId()),
              type(expr.ref().type()),
              literal(expr.ref().type(), lit.value()))
          .end();
  }

  @Override
  public <T> Action eq(Bound<T> expr, Literal<T> lit) {
    return () -> this.builder.equals(idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literal(expr.ref().type(), lit.value()));
  }

  @Override
  public <T> Action notEq(Bound<T> expr, Literal<T> lit) {
    // NOTE: ORC uses SQL semantics for Search Arguments, so an expression like
    // `col != 1` will exclude rows where col is NULL along with rows where col = 1
    // In contrast, Iceberg's Expressions will keep rows with NULL values
    // So the equivalent ORC Search Argument for an Iceberg Expression `col != x`
    // is `col IS NULL OR col != x`
    return () -> {
      this.builder.startOr();
      isNull(expr).invoke();
      this.builder.startNot();
      eq(expr, lit).invoke();
      this.builder.end(); // end NOT
      this.builder.end(); // end OR
    };
  }

  @Override
  public <T> Action in(Bound<T> expr, Set<T> literalSet) {
    return () -> this.builder.in(
        idToColumnName.get(expr.ref().fieldId()),
        type(expr.ref().type()),
        literalSet.stream().map(lit -> literal(expr.ref().type(), lit)).toArray(Object[]::new));
  }

  @Override
  public <T> Action notIn(Bound<T> expr, Set<T> literalSet) {
    // NOTE: ORC uses SQL semantics for Search Arguments, so an expression like
    // `col NOT IN {1}` will exclude rows where col is NULL along with rows where col = 1
    // In contrast, Iceberg's Expressions will keep rows with NULL values
    // So the equivalent ORC Search Argument for an Iceberg Expression `col NOT IN {x}`
    // is `col IS NULL OR col NOT IN {x}`
    return () -> {
      this.builder.startOr();
      isNull(expr).invoke();
      this.builder.startNot();
      in(expr, literalSet).invoke();
      this.builder.end(); // end NOT
      this.builder.end(); // end OR
    };
  }

  @Override
  public <T> Action startsWith(Bound<T> expr, Literal<T> lit) {
    // Cannot push down STARTS_WITH operator to ORC, so return TruthValue.YES_NO_NULL which signifies
    // that this predicate cannot help with filtering
    return () -> this.builder.literal(TruthValue.YES_NO_NULL);
  }

  @Override
  public <T> Action notStartsWith(Bound<T> expr, Literal<T> lit) {
    // Cannot push down NOT_STARTS_WITH operator to ORC, so return TruthValue.YES_NO_NULL which signifies
    // that this predicate cannot help with filtering
    return () -> this.builder.literal(TruthValue.YES_NO_NULL);
  }

  @Override
  public <T> Action predicate(BoundPredicate<T> pred) {
    if (UNSUPPORTED_TYPES.contains(pred.ref().type().typeId())) {
      // Cannot push down predicates for types which cannot be represented in PredicateLeaf.Type, so return
      // TruthValue.YES_NO_NULL which signifies that this predicate cannot help with filtering
      return () -> this.builder.literal(TruthValue.YES_NO_NULL);
    } else {
      return super.predicate(pred);
    }
  }

  @FunctionalInterface
  interface Action {
    void invoke();
  }

  private PredicateLeaf.Type type(Type icebergType) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return PredicateLeaf.Type.BOOLEAN;
      case INTEGER:
      case LONG:
      case TIME:
        return PredicateLeaf.Type.LONG;
      case FLOAT:
      case DOUBLE:
        return PredicateLeaf.Type.FLOAT;
      case DATE:
        return PredicateLeaf.Type.DATE;
      case TIMESTAMP:
        return PredicateLeaf.Type.TIMESTAMP;
      case STRING:
        return PredicateLeaf.Type.STRING;
      case DECIMAL:
        return PredicateLeaf.Type.DECIMAL;
      default:
        throw new UnsupportedOperationException("Type " + icebergType + " not supported in ORC SearchArguments");
    }
  }

  private <T> Object literal(Type icebergType, T icebergLiteral) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
      case LONG:
      case TIME:
      case DOUBLE:
        return icebergLiteral;
      case INTEGER:
        return ((Integer) icebergLiteral).longValue();
      case FLOAT:
        return ((Float) icebergLiteral).doubleValue();
      case STRING:
        return icebergLiteral.toString();
      case DATE:
        return Date.valueOf(LocalDate.ofEpochDay((Integer) icebergLiteral));
      case TIMESTAMP:
        long microsFromEpoch = (Long) icebergLiteral;
        return Timestamp.from(Instant.ofEpochSecond(
            Math.floorDiv(microsFromEpoch, 1_000_000),
            Math.floorMod(microsFromEpoch, 1_000_000) * 1_000
        ));
      case DECIMAL:
        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) icebergLiteral, false));
      default:
        throw new UnsupportedOperationException("Type " + icebergType + " not supported in ORC SearchArguments");
    }
  }
}
