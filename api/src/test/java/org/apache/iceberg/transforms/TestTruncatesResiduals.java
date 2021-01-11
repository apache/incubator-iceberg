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

package org.apache.iceberg.transforms;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.startsWith;

public class TestTruncatesResiduals {

  /**
   * Test helper method to compute residual for a given partitionValue against a predicate
   * and assert the resulting residual expression is same as the expectedOp
   *
   * @param spec the partition spec
   * @param predicate predicate to calculate the residual against
   * @param partitionValue value of the partition to check the residual for
   * @param expectedOp expected operation to assert against
   * @param <T> Type parameter of partitionValue
   */
  public <T> void assertResidualValue(PartitionSpec spec, UnboundPredicate<?> predicate,
                                  T partitionValue, Expression.Operation expectedOp) {
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, predicate, true);
    Expression residual = resEval.residualFor(TestHelpers.Row.of(partitionValue));

    Assert.assertEquals(expectedOp, residual.op());
  }

  /**
   * Test helper method to compute residual for a given partitionValue against a predicate
   * and assert that the resulting expression is same as the original predicate
   *
   * @param spec the partition spec
   * @param predicate predicate to calculate the residual against
   * @param partitionValue value of the partition to check the residual for
   * @param <T> Type parameter of partitionValue
   */
  public <T> void assertResidualPredicate(PartitionSpec spec,
                                      UnboundPredicate<?> predicate, T partitionValue) {
    ResidualEvaluator resEval = ResidualEvaluator.of(spec, predicate, true);
    Expression residual = resEval.residualFor(TestHelpers.Row.of(partitionValue));

    UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);
    Assert.assertEquals(predicate.op(), unbound.op());
    Assert.assertEquals(predicate.ref().name(), unbound.ref().name());
    Assert.assertEquals(predicate.literal().value(), unbound.literal().value());
  }

  @Test
  public void testIntegerTruncateTransformResiduals() {
    Schema schema = new Schema(Types.NestedField.optional(50, "value", Types.IntegerType.get()));
    // valid partitions would be 0, 10, 20...90, 100 etc.
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    // less than lower bound
    assertResidualValue(spec, lessThan("value", 100), 110, Expression.Operation.FALSE);
    assertResidualValue(spec, lessThan("value", 100), 100, Expression.Operation.FALSE);
    assertResidualValue(spec, lessThan("value", 100), 90, Expression.Operation.TRUE);
    // less than upper bound
    assertResidualValue(spec, lessThan("value", 99), 100, Expression.Operation.FALSE);
    assertResidualPredicate(spec, lessThan("value", 99), 90);
    assertResidualValue(spec, lessThan("value", 99), 80, Expression.Operation.TRUE);

    // less than equals lower bound
    assertResidualValue(spec, lessThanOrEqual("value", 100), 110, Expression.Operation.FALSE);
    assertResidualPredicate(spec, lessThanOrEqual("value", 100), 100);
    assertResidualValue(spec, lessThanOrEqual("value", 100), 90, Expression.Operation.TRUE);
    // less than equals upper bound
    assertResidualValue(spec, lessThanOrEqual("value", 99), 100, Expression.Operation.FALSE);
    assertResidualValue(spec, lessThanOrEqual("value", 99), 90, Expression.Operation.TRUE);
    assertResidualValue(spec, lessThanOrEqual("value", 99), 80, Expression.Operation.TRUE);

    // greater than lower bound
    assertResidualValue(spec, greaterThan("value", 100), 110, Expression.Operation.TRUE);
    assertResidualPredicate(spec, greaterThan("value", 100), 100);
    assertResidualValue(spec, greaterThan("value", 100), 90, Expression.Operation.FALSE);
    // greater than upper bound
    assertResidualValue(spec, greaterThan("value", 99), 100, Expression.Operation.TRUE);
    assertResidualValue(spec, greaterThan("value", 99), 90, Expression.Operation.FALSE);
    assertResidualValue(spec, greaterThan("value", 99), 80, Expression.Operation.FALSE);

    // greater than equals lower bound
    assertResidualValue(spec, greaterThanOrEqual("value", 100), 110, Expression.Operation.TRUE);
    assertResidualValue(spec, greaterThanOrEqual("value", 100), 100, Expression.Operation.TRUE);
    assertResidualValue(spec, greaterThanOrEqual("value", 100), 90, Expression.Operation.FALSE);
    // greater than equals upper bound
    assertResidualValue(spec, greaterThanOrEqual("value", 99), 100, Expression.Operation.TRUE);
    assertResidualPredicate(spec, greaterThanOrEqual("value", 99), 90);
    assertResidualValue(spec, greaterThanOrEqual("value", 99), 80, Expression.Operation.FALSE);

    // equal lower bound
    assertResidualValue(spec, equal("value", 100), 110, Expression.Operation.FALSE);
    assertResidualPredicate(spec, equal("value", 100), 100);
    assertResidualValue(spec, equal("value", 100), 90, Expression.Operation.FALSE);
    // equal upper bound
    assertResidualValue(spec, equal("value", 99), 100, Expression.Operation.FALSE);
    assertResidualPredicate(spec, equal("value", 99), 90);
    assertResidualValue(spec, equal("value", 99), 80, Expression.Operation.FALSE);

    // not equal lower bound
    assertResidualValue(spec, notEqual("value", 100), 110, Expression.Operation.TRUE);
    assertResidualPredicate(spec, notEqual("value", 100), 100);
    assertResidualValue(spec, notEqual("value", 100), 90, Expression.Operation.TRUE);
    // not equal upper bound
    assertResidualValue(spec, notEqual("value", 99), 100, Expression.Operation.TRUE);
    assertResidualPredicate(spec, notEqual("value", 99), 90);
    assertResidualValue(spec, notEqual("value", 99), 80, Expression.Operation.TRUE);
  }

  @Test
  public void testStringTruncateTransformResiduals() {
    Schema schema = new Schema(Types.NestedField.optional(50, "value", Types.StringType.get()));
    // valid partitions would be two letter strings for eg: ab, bc etc
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 2).build();

    // less than
    assertResidualValue(spec, lessThan("value", "bcd"), "ab", Expression.Operation.TRUE);
    assertResidualPredicate(spec, lessThan("value", "bcd"), "bc");
    assertResidualValue(spec, lessThan("value", "bcd"), "cd", Expression.Operation.FALSE);

    // less than equals
    assertResidualValue(spec, lessThanOrEqual("value", "bcd"), "ab", Expression.Operation.TRUE);
    assertResidualPredicate(spec, lessThanOrEqual("value", "bcd"), "bc");
    assertResidualValue(spec, lessThanOrEqual("value", "bcd"), "cd", Expression.Operation.FALSE);

    // greater than
    assertResidualValue(spec, greaterThan("value", "bcd"), "ab", Expression.Operation.FALSE);
    assertResidualPredicate(spec, greaterThan("value", "bcd"), "bc");
    assertResidualValue(spec, greaterThan("value", "bcd"), "cd", Expression.Operation.TRUE);

    // greater than
    assertResidualValue(spec, greaterThanOrEqual("value", "bcd"), "ab", Expression.Operation.FALSE);
    assertResidualPredicate(spec, greaterThanOrEqual("value", "bcd"), "bc");
    assertResidualValue(spec, greaterThanOrEqual("value", "bcd"), "cd", Expression.Operation.TRUE);

    // equal
    assertResidualValue(spec, equal("value", "bcd"), "ab", Expression.Operation.FALSE);
    assertResidualPredicate(spec, equal("value", "bcd"), "bc");
    assertResidualValue(spec, equal("value", "bcd"), "cd", Expression.Operation.FALSE);

    // not equal
    assertResidualValue(spec, notEqual("value", "bcd"), "ab", Expression.Operation.TRUE);
    assertResidualPredicate(spec, notEqual("value", "bcd"), "bc");
    assertResidualValue(spec, notEqual("value", "bcd"), "cd", Expression.Operation.TRUE);

    // starts with
    assertResidualValue(spec, startsWith("value", "bcd"), "ab", Expression.Operation.FALSE);
    assertResidualPredicate(spec, startsWith("value", "bcd"), "bc");
    assertResidualValue(spec, startsWith("value", "bcd"), "cd", Expression.Operation.FALSE);

    // not starts with
    assertResidualValue(spec, notStartsWith("value", "bc"), "ab", Expression.Operation.TRUE);
    assertResidualValue(spec, notStartsWith("value", "bc"), "bc", Expression.Operation.FALSE);
    assertResidualPredicate(spec, notStartsWith("value", "bcd"), "ab");
    assertResidualValue(spec, notStartsWith("value", "bcd"), "bc", Expression.Operation.FALSE);
    assertResidualPredicate(spec, notStartsWith("value", "bcd"), "cd");
  }
}
