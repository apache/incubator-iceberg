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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.NaNUtil;

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

/**
 * Evaluates an {@link Expression} on a {@link DataFile} to test whether all rows in the file match.
 * <p>
 * This evaluation is strict: it returns true if all rows in a file must match the expression. For
 * example, if a file's ts column has min X and max Y, this evaluator will return true for ts &lt; Y+1
 * but not for ts &lt; Y-1.
 * <p>
 * Files are passed to {@link #eval(ContentFile)}, which returns true if all rows in the file must
 * contain matching rows and false if the file may contain rows that do not match.
 * <p>
 * Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if the first
 * value in a file is NaN, metrics of this file will report NaN for both upper and lower bound despite
 * that the column could contain non-NaN data. Thus in some scenarios explicitly checks for NaN is necessary
 * in order to not include files that may contain rows that don't match.
 */
public class StrictMetricsEvaluator {
  private final Schema schema;
  private final StructType struct;
  private final Expression expr;

  public StrictMetricsEvaluator(Schema schema, Expression unbound) {
    this.schema = schema;
    this.struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), true);
  }

  /**
   * Test whether all records within the file match the expression.
   *
   * @param file a data file
   * @return false if the file may contain any row that doesn't match the expression, true otherwise.
   */
  public boolean eval(ContentFile<?> file) {
    // TODO: detect the case where a column is missing from the file using file's max field id.
    return new MetricsEvalVisitor().eval(file);
  }

  private static final boolean ROWS_MUST_MATCH = true;
  private static final boolean ROWS_MIGHT_NOT_MATCH = false;

  private class MetricsEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, Long> nanCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;

    private boolean eval(ContentFile<?> file) {
      if (file.recordCount() <= 0) {
        return ROWS_MUST_MATCH;
      }

      this.valueCounts = file.valueCounts();
      this.nullCounts = file.nullValueCounts();
      this.nanCounts = file.nanValueCounts();
      this.lowerBounds = file.lowerBounds();
      this.upperBounds = file.upperBounds();

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return ROWS_MUST_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_MIGHT_NOT_MATCH; // no rows match
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
      // no need to check whether the field is required because binding evaluates that case
      // if the column has any non-null values, the expression does not match
      int id = ref.fieldId();
      Preconditions.checkNotNull(struct.field(id),
          "Cannot filter by nested column: %s", schema.findField(id));

      if (containsNullsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has any null values, the expression does not match
      int id = ref.fieldId();
      Preconditions.checkNotNull(struct.field(id),
          "Cannot filter by nested column: %s", schema.findField(id));

      if (nullCounts != null && nullCounts.containsKey(id) && nullCounts.get(id) == 0) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      if (containsNaNsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      if (nanCounts != null && nanCounts.containsKey(id) && nanCounts.get(id) == 0) {
        return ROWS_MUST_MATCH;
      }

      if (containsNullsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <----------Min----Max---X------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <----------Min----Max---X------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp <= 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <-------X---Min----Max---------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(field.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
          return ROWS_MIGHT_NOT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <-------X---Min----Max---------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(field.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
          return ROWS_MIGHT_NOT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp >= 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when Min == X == Max
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id) &&
          upperBounds != null && upperBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp != 0) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        cmp = lit.comparator().compare(upper, lit.value());
        if (cmp != 0) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when X < Min or Max < X because it is not in the range
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
          return ROWS_MIGHT_NOT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_MUST_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id) || canContainNaNs(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id) &&
          upperBounds != null && upperBounds.containsKey(id)) {
        // similar to the implementation in eq, first check if the lower bound is in the set
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));
        if (!literalSet.contains(lower)) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        // check if the upper bound is in the set
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));
        if (!literalSet.contains(upper)) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        // finally check if the lower bound and the upper bound are equal
        if (ref.comparator().compare(lower, upper) != 0) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        // All values must be in the set if the lower bound and the upper bound are in the set and are equal.
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      Collection<T> literals = literalSet;

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
          return ROWS_MIGHT_NOT_MATCH;
        }

        literals = literals.stream().filter(v -> ref.comparator().compare(lower, v) <= 0).collect(Collectors.toList());
        if (literals.isEmpty()) {  // if all values are less than lower bound, rows must match (notIn).
          return ROWS_MUST_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));
        literals = literals.stream().filter(v -> ref.comparator().compare(upper, v) >= 0).collect(Collectors.toList());
        if (literals.isEmpty()) { // if all remaining values are greater than upper bound, rows must match (notIn).
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    private boolean canContainNulls(Integer id) {
      return nullCounts == null || nullCounts.containsKey(id) && nullCounts.get(id) > 0;
    }

    private boolean canContainNaNs(Integer id) {
      // nan counts might be null for early version writers when nan counters are not populated.
      return nanCounts != null && nanCounts.containsKey(id) && nanCounts.get(id) > 0;
    }

    private boolean containsNullsOnly(Integer id) {
      return valueCounts != null && valueCounts.containsKey(id) &&
          nullCounts != null && nullCounts.containsKey(id) &&
          valueCounts.get(id) - nullCounts.get(id) == 0;
    }

    private boolean containsNaNsOnly(Integer id) {
      return nanCounts != null && nanCounts.containsKey(id) &&
          valueCounts != null && nanCounts.get(id).equals(valueCounts.get(id));
    }
  }
}
