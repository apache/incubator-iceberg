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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.NaNUtil;

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

/**
 * Evaluates an {@link Expression} on a {@link DataFile} to test whether rows in the file may match.
 * <p>
 * This evaluation is inclusive: it returns true if a file may match and false if it cannot match.
 * <p>
 * Files are passed to {@link #eval(ContentFile)}, which returns true if the file may contain matching
 * rows and false if the file cannot contain matching rows. Files may be skipped if and only if the
 * return value of {@code eval} is false.
 * <p>
 * Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if the first
 * value in a file is NaN, metrics of this file will report NaN for both upper and lower bound despite
 * that the column could contain non-NaN data. Thus in some scenarios explicitly checks for NaN is necessary
 * in order to not skip files that may contain matching data.
 */
public class InclusiveMetricsEvaluator {
  private static final int IN_PREDICATE_LIMIT = 200;

  private final Expression expr;

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param file a data file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(ContentFile<?> file) {
    // TODO: detect the case where a column is missing from the file using file's max field id.
    return new MetricsEvalVisitor().eval(file);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class MetricsEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, Long> nanCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;

    private boolean eval(ContentFile<?> file) {
      if (file.recordCount() == 0) {
        return ROWS_CANNOT_MATCH;
      }

      if (file.recordCount() < 0) {
        // we haven't implemented parsing record count from avro file and thus set record count -1
        // when importing avro tables to iceberg tables. This should be updated once we implemented
        // and set correct record count.
        return ROWS_MIGHT_MATCH;
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
      return ROWS_MIGHT_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
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
      // if the column has no null values, the expression cannot match
      Integer id = ref.fieldId();

      if (nullCounts != null && nullCounts.containsKey(id) && nullCounts.get(id) == 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no non-null values, the expression cannot match
      Integer id = ref.fieldId();

      if (containsNullsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      Integer id = ref.fieldId();

      if (nanCounts != null && nanCounts.containsKey(id) && nanCounts.get(id) == 0) {
        return ROWS_CANNOT_MATCH;
      }

      // when there's no nanCounts information, but we already know the column only contains null,
      // it's guaranteed that there's no NaN value
      if (containsNullsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      Integer id = ref.fieldId();

      if (containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(ref.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
          return ROWS_MIGHT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp >= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(ref.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
          return ROWS_MIGHT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp <= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(ref.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
          return ROWS_MIGHT_MATCH;
        }

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      Collection<T> literals = literalSet;

      if (literals.size() > IN_PREDICATE_LIMIT) {
        // skip evaluating the predicate if the number of values is too big
        return ROWS_MIGHT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(ref.type(), lowerBounds.get(id));

        if (NaNUtil.isNaN(lower)) {
          // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
          return ROWS_MIGHT_MATCH;
        }

        literals = literals.stream().filter(v -> ref.comparator().compare(lower, v) <= 0).collect(Collectors.toList());
        if (literals.isEmpty()) { // if all values are less than lower bound, rows cannot match.
          return ROWS_CANNOT_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));
        literals = literals.stream().filter(v -> ref.comparator().compare(upper, v) >= 0).collect(Collectors.toList());
        if (literals.isEmpty()) { // if all remaining values are greater than upper bound, rows cannot match.
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      if (containsNullsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      ByteBuffer prefixAsBytes = lit.toByteBuffer();

      Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        ByteBuffer lower = lowerBounds.get(id);
        // truncate lower bound so that its length in bytes is not greater than the length of prefix
        int length = Math.min(prefixAsBytes.remaining(), lower.remaining());
        int cmp = comparator.compare(BinaryUtil.truncateBinary(lower, length), prefixAsBytes);
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        ByteBuffer upper = upperBounds.get(id);
        // truncate upper bound so that its length in bytes is not greater than the length of prefix
        int length = Math.min(prefixAsBytes.remaining(), upper.remaining());
        int cmp = comparator.compare(BinaryUtil.truncateBinary(upper, length), prefixAsBytes);
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      // Iceberg does not implement SQL 3-boolean logic. Therefore, for null values, we have decided to
      // return ROWS_MIGHT_MATCH in order to allow the query engine to further evaluate the file, as
      // null does not start with any non-null value.
      if (containsNullsOnly(id)) {
        return ROWS_MIGHT_MATCH;
      }

      ByteBuffer prefixAsBytes = lit.toByteBuffer();

      Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

      // notStartsWith will match unless all values must start with the prefix. this happens when the lower and upper
      // bounds both start with the prefix.
      if (lowerBounds != null && upperBounds != null &&
          lowerBounds.containsKey(id) && upperBounds.containsKey(id)) {
        ByteBuffer lower = lowerBounds.get(id);
        // if lower is shorter than the prefix, it can't start with the prefix
        if (lower.remaining() < prefixAsBytes.remaining()) {
          return ROWS_MIGHT_MATCH;
        }

        // truncate lower bound to the prefix and check for equality
        int cmp = comparator.compare(BinaryUtil.truncateBinary(lower, prefixAsBytes.remaining()), prefixAsBytes);
        if (cmp == 0) {
          // the lower bound starts with the prefix; check the upper bound
          ByteBuffer upper = upperBounds.get(id);
          // if upper is shorter than the prefix, it can't start with the prefix
          if (upper.remaining() < prefixAsBytes.remaining()) {
            return ROWS_MIGHT_MATCH;
          }

          // truncate upper bound so that its length in bytes is not greater than the length of prefix
          cmp = comparator.compare(BinaryUtil.truncateBinary(upper, prefixAsBytes.remaining()), prefixAsBytes);
          if (cmp == 0) {
            // both bounds match the prefix, so all rows must match the prefix and none do not match
            return ROWS_CANNOT_MATCH;
          }
        }
      }

      return ROWS_MIGHT_MATCH;
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
