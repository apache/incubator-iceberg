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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * A sort order that defines how data and delete files should be ordered in a table.
 */
public class SortOrder implements Serializable {
  private static final SortOrder UNSORTED_ORDER = new SortOrder(new Schema(), 0, Collections.emptyList());

  private final Schema schema;
  private final int orderId;
  private final SortField[] fields;

  private transient volatile List<SortField> fieldList;

  private SortOrder(Schema schema, int orderId, List<SortField> fields) {
    this.schema = schema;
    this.orderId = orderId;
    this.fields = fields.toArray(new SortField[0]);
  }

  /**
   * Returns the {@link Schema} for this sort order
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Returns the ID of this sort order
   */
  public int orderId() {
    return orderId;
  }

  /**
   * Returns the list of {@link SortField sort fields} for this sort order
   */
  public List<SortField> fields() {
    return lazyFieldList();
  }

  /**
   * Returns true if the sort order is unsorted
   */
  public boolean isUnsorted() {
    return fields.length < 1;
  }

  /**
   * Checks whether this order satisfies another order.
   *
   * @param anotherSortOrder a different sort order
   * @return true if this order satisfies the given order
   */
  public boolean satisfies(SortOrder anotherSortOrder) {
    // any ordering satisfies an unsorted ordering
    if (anotherSortOrder.isUnsorted()) {
      return true;
    }

    // this ordering cannot satisfy an ordering with more sort fields
    if (anotherSortOrder.fields.length > fields.length) {
      return false;
    }

    // this ordering has either more or the same number of sort fields
    return IntStream.range(0, anotherSortOrder.fields.length)
        .allMatch(index -> fields[index].satisfies(anotherSortOrder.fields[index]));
  }

  /**
   * Checks whether this order is equivalent to another order while ignoring the order id.
   *
   * @param anotherSortOrder a different sort order
   * @return true if this order is equivalent to the given order
   */
  public boolean sameOrder(SortOrder anotherSortOrder) {
    return Arrays.equals(fields, anotherSortOrder.fields);
  }

  private List<SortField> lazyFieldList() {
    if (fieldList == null) {
      synchronized (this) {
        if (fieldList == null) {
          this.fieldList = ImmutableList.copyOf(fields);
        }
      }
    }
    return fieldList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (SortField field : fields) {
      sb.append("\n");
      sb.append("  ").append(field);
    }
    if (fields.length > 0) {
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SortOrder that = (SortOrder) other;
    return orderId == that.orderId && sameOrder(that);
  }

  @Override
  public int hashCode() {
    return 31 * Integer.hashCode(orderId) + Arrays.hashCode(fields);
  }

  /**
   * Returns a sort order for unsorted tables.
   *
   * @return an unsorted order
   */
  public static SortOrder unsorted() {
    return UNSORTED_ORDER;
  }

  /**
   * Creates a new {@link Builder sort order builder} for the given {@link Schema}.
   *
   * @param schema a schema
   * @return a sort order builder for the given schema
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * A builder used to create valid {@link SortOrder sort orders}.
   * <p>
   * Call {@link #builderFor(Schema)} to create a new builder.
   */
  public static class Builder implements SortOrderBuilder<Builder> {
    private final Schema schema;
    private final List<SortField> fields = Lists.newArrayList();
    private Integer orderId = null;
    private boolean caseSensitive = true;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    /**
     * Add an expression term to the sort, ascending with the given null order.
     *
     * @param term an expression term
     * @param nullOrder a null order (first or last)
     * @return this for method chaining
     */
    @Override
    public Builder asc(Term term, NullOrder nullOrder) {
      return addSortField(term, SortDirection.ASC, nullOrder);
    }

    /**
     * Add an expression term to the sort, ascending with the given null order.
     *
     * @param term an expression term
     * @param nullOrder a null order (first or last)
     * @return this for method chaining
     */
    @Override
    public Builder desc(Term term, NullOrder nullOrder) {
      return addSortField(term, SortDirection.DESC, nullOrder);
    }

    public Builder sortBy(String name, SortDirection direction, NullOrder nullOrder) {
      return addSortField(Expressions.ref(name), direction, nullOrder);
    }

    public Builder sortBy(Term term, SortDirection direction, NullOrder nullOrder) {
      return addSortField(term, direction, nullOrder);
    }


    public Builder withOrderId(int newOrderId) {
      this.orderId = newOrderId;
      return this;
    }

    public Builder caseSensitive(boolean sortCaseSensitive) {
      this.caseSensitive = sortCaseSensitive;
      return this;
    }

    Builder addSortField(Term term, SortDirection direction, NullOrder nullOrder) {
      Preconditions.checkArgument(term instanceof UnboundTerm, "Term must be unbound");
      // ValidationException is thrown by bind if binding fails so we assume that boundTerm is correct
      BoundTerm<?> boundTerm = ((UnboundTerm<?>) term).bind(schema.asStruct(), caseSensitive);
      int sourceId = boundTerm.ref().fieldId();
      SortField sortField = new SortField(toTransform(boundTerm), sourceId, direction, nullOrder);
      fields.add(sortField);
      return this;
    }

    Builder addSortField(String transformAsString, int sourceId, SortDirection direction, NullOrder nullOrder) {
      Types.NestedField column = schema.findField(sourceId);
      Preconditions.checkNotNull(column, "Cannot find source column: %s", sourceId);
      Transform<?, ?> transform = Transforms.fromString(column.type(), transformAsString);
      SortField sortField = new SortField(transform, sourceId, direction, nullOrder);
      fields.add(sortField);
      return this;
    }

    public SortOrder build() {
      if (fields.isEmpty()) {
        if (orderId != null && orderId != 0) {
          throw new IllegalArgumentException("Unsorted order ID must be 0");
        }
        return SortOrder.unsorted();
      }

      if (orderId != null && orderId == 0) {
        throw new IllegalArgumentException("Sort order ID 0 is reserved for unsorted order");
      }

      // default ID to 1 as 0 is reserved for unsorted order
      int actualOrderId = orderId != null ? orderId : 1;
      SortOrder sortOrder = new SortOrder(schema, actualOrderId, fields);
      checkCompatibility(sortOrder, schema);
      return sortOrder;
    }

    private Transform<?, ?> toTransform(BoundTerm<?> term) {
      if (term instanceof BoundReference) {
        return Transforms.identity(term.type());
      } else if (term instanceof BoundTransform) {
        return ((BoundTransform<?, ?>) term).transform();
      } else {
        throw new ValidationException("Invalid term: %s, expected either a bound reference or transform", term);
      }
    }
  }

  public static void checkCompatibility(SortOrder sortOrder, Schema schema) {
    for (SortField field : sortOrder.fields) {
      Type sourceType = schema.findType(field.sourceId());
      ValidationException.check(
          sourceType != null,
          "Cannot find source column for sort field: %s", field);
      ValidationException.check(
          sourceType.isPrimitiveType(),
          "Cannot sort by non-primitive source field: %s", sourceType);
      ValidationException.check(
          field.transform().canTransform(sourceType),
          "Invalid source type %s for transform: %s",
          sourceType, field.transform());
    }
  }
}
