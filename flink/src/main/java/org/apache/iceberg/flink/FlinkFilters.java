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

package org.apache.iceberg.flink;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;

public class FlinkFilters {
  private FlinkFilters() {
  }

  private static final Pattern STARTS_WITH_PATTERN = Pattern.compile("([^%]+)%");

  private static final Map<FunctionDefinition, Operation> FILTERS = ImmutableMap
      .<FunctionDefinition, Operation>builder()
      .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
      .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
      .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
      .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
      .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
      .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
      .put(BuiltInFunctionDefinitions.IN, Operation.IN)
      .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
      .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Operation.NOT_NULL)
      .put(BuiltInFunctionDefinitions.AND, Operation.AND)
      .put(BuiltInFunctionDefinitions.OR, Operation.OR)
      .put(BuiltInFunctionDefinitions.NOT, Operation.NOT)
      .put(BuiltInFunctionDefinitions.LIKE, Operation.STARTS_WITH)
      .build();

  /**
   * convert flink expression to iceberg expression.
   * <p>
   * The BETWEEN, NOT_BETWEEN,IN expression will be converted by flink automatically. the BETWEEN will be converted to
   * (GT_EQ AND LT_EQ), the NOT_BETWEEN will be converted to (LT_EQ OR GT_EQ), the IN will be converted to OR, so we do
   * not add the conversion here
   *
   * @param flinkExpression the flink expression
   * @return the iceberg expression
   */
  public static Optional<Expression> convert(org.apache.flink.table.expressions.Expression flinkExpression) {
    if (!(flinkExpression instanceof CallExpression)) {
      return Optional.empty();
    }

    CallExpression call = (CallExpression) flinkExpression;
    Operation op = FILTERS.get(call.getFunctionDefinition());
    if (op != null) {
      switch (op) {
        case IS_NULL:
          Optional<String> name = toReference(singleton(call, FieldReferenceExpression.class).orElse(null));
          return name.map(Expressions::isNull);

        case NOT_NULL:
          Optional<String> nameNotNull = toReference(singleton(call, FieldReferenceExpression.class).orElse(null));
          return nameNotNull.map(Expressions::notNull);

        case LT:
          return convertComparisonExpression(Expressions::lessThan, Expressions::greaterThan, call);

        case LT_EQ:
          return convertComparisonExpression(Expressions::lessThanOrEqual, Expressions::greaterThanOrEqual, call);

        case GT:
          return convertComparisonExpression(Expressions::greaterThan, Expressions::lessThan, call);

        case GT_EQ:
          return convertComparisonExpression(Expressions::greaterThanOrEqual, Expressions::lessThanOrEqual, call);

        case EQ:
          return handleNaN(Expressions::equal, Expressions::isNaN, call);

        case NOT_EQ:
          return handleNaN(Expressions::notEqual, Expressions::notNaN, call);

        case NOT:
          Optional<Expression> child = convert(singleton(call, CallExpression.class).orElse(null));
          return child.map(Expressions::not);

        case AND:
          return convertLogicExpression(Expressions::and, call);

        case OR:
          return convertLogicExpression(Expressions::or, call);

        case STARTS_WITH:
          return convertLike(call);
      }
    }

    return Optional.empty();
  }

  private static <T extends ResolvedExpression> Optional<T> singleton(CallExpression call,
                                                                      Class<T> expectedChildClass) {
    List<ResolvedExpression> children = call.getResolvedChildren();
    if (children.size() != 1) {
      return Optional.empty();
    }

    ResolvedExpression child = children.get(0);
    if (!expectedChildClass.isInstance(child)) {
      return Optional.empty();
    }

    return Optional.of(expectedChildClass.cast(child));
  }

  private static Optional<Expression> convertLike(CallExpression call) {
    Tuple2<String, Object> tuple2 = convertBinaryExpress(call);
    if (tuple2 == null) {
      return Optional.empty();
    }

    String pattern = tuple2.f1.toString();
    Matcher matcher = STARTS_WITH_PATTERN.matcher(pattern);

    // exclude special char of LIKE
    // '_' is the wildcard of the SQL LIKE
    if (!pattern.contains("_") && matcher.matches()) {
      return Optional.of(Expressions.startsWith(tuple2.f0, matcher.group(1)));
    }

    return Optional.empty();
  }

  private static Optional<Expression> convertLogicExpression(BiFunction<Expression, Expression, Expression> function,
                                                             CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    Optional<Expression> left = convert(args.get(0));
    Optional<Expression> right = convert(args.get(1));
    if (left.isPresent() && right.isPresent()) {
      return Optional.of(function.apply(left.get(), right.get()));
    }

    return Optional.empty();
  }

  private static Optional<Expression> convertComparisonExpression(
      BiFunction<String, Object, Expression> function, BiFunction<String, Object, Expression> reversedFunction,
      CallExpression call) {
    Tuple2<String, Object> tuple2 = convertBinaryExpress(call);
    if (tuple2 != null) {
      if (literalOnRight(call.getResolvedChildren())) {
        return Optional.of(function.apply(tuple2.f0, tuple2.f1));
      } else {
        return Optional.of(reversedFunction.apply(tuple2.f0, tuple2.f1));
      }
    } else {
      return Optional.empty();
    }
  }

  private static Optional<String> toReference(org.apache.flink.table.expressions.Expression expression) {
    return expression instanceof FieldReferenceExpression ?
        Optional.of(((FieldReferenceExpression) expression).getName()) :
        Optional.empty();
  }

  private static Optional<Object> toLiteral(org.apache.flink.table.expressions.Expression expression) {
    // Not support null literal
    return expression instanceof ValueLiteralExpression ?
        convertLiteral((ValueLiteralExpression) expression) :
        Optional.empty();
  }

  private static Optional<Expression> handleNaN(BiFunction<String, Object, Expression> function,
                                                Function<String, Expression> functionNaN,
                                                CallExpression call) {
    Tuple2<String, Object> tuple2 = convertBinaryExpress(call);
    if (tuple2 == null) {
      return Optional.empty();
    }

    String name = tuple2.f0;
    Object value = tuple2.f1;

    if (NaNUtil.isNaN(value)) {
      return Optional.of(functionNaN.apply(name));
    } else {
      return Optional.of(function.apply(name, value));
    }
  }

  private static Optional<Object> convertLiteral(ValueLiteralExpression expression) {
    Optional<?> value = expression.getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion());
    return value.map(o -> {
      if (o instanceof LocalDateTime) {
        return DateTimeUtil.microsFromTimestamp((LocalDateTime) o);
      } else if (o instanceof Instant) {
        return DateTimeUtil.microsFromInstant((Instant) o);
      } else if (o instanceof LocalTime) {
        return DateTimeUtil.microsFromTime((LocalTime) o);
      } else if (o instanceof LocalDate) {
        return DateTimeUtil.daysFromDate((LocalDate) o);
      }

      return o;
    });
  }

  private static boolean literalOnRight(List<ResolvedExpression> args) {
    return args.get(0) instanceof FieldReferenceExpression && args.get(1) instanceof ValueLiteralExpression;
  }

  private static Tuple2<String, Object> convertBinaryExpress(CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    if (args.size() != 2) {
      return null;
    }

    Optional<String> name;
    Optional<Object> value;
    if (literalOnRight(args)) {
      name = toReference(args.get(0));
      value = toLiteral(args.get(1));
    } else {
      name = toReference(args.get(1));
      value = toLiteral(args.get(0));
    }

    if (name.isPresent() && value.isPresent()) {
      return Tuple2.of(name.get(), value.get());
    } else {
      return null;
    }
  }
}
