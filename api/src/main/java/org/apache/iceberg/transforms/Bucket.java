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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Type.TypeID;

abstract class Bucket<T> implements Transform<T, Integer> {
  private static final HashFunction MURMUR3 = Hashing.murmur3_32();

  @SuppressWarnings("unchecked")
  static <T> Bucket<T> get(Type type, int numBuckets) {
    Preconditions.checkArgument(numBuckets > 0,
        "Invalid number of buckets: %s (must be > 0)", numBuckets);

    switch (type.typeId()) {
      case DATE:
      case INTEGER:
        return (Bucket<T>) new BucketInteger(numBuckets);
      case TIME:
      case TIMESTAMP:
      case LONG:
        return (Bucket<T>) new BucketLong(numBuckets);
      case DECIMAL:
        return (Bucket<T>) new BucketDecimal(numBuckets);
      case STRING:
        return (Bucket<T>) new BucketString(numBuckets);
      case FIXED:
      case BINARY:
        return (Bucket<T>) new BucketByteBuffer(numBuckets);
      case UUID:
        return (Bucket<T>) new BucketUUID(numBuckets);
      default:
        throw new IllegalArgumentException("Cannot bucket by type: " + type);
    }
  }

  private final int numBuckets;

  private Bucket(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public Integer numBuckets() {
    return numBuckets;
  }

  @VisibleForTesting
  abstract int hash(T value);

  @Override
  public Integer apply(T value) {
    if (value == null) {
      return null;
    }
    return (hash(value) & Integer.MAX_VALUE) % numBuckets;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Bucket)) {
      return false;
    }

    Bucket<?> bucket = (Bucket<?>) o;
    return numBuckets == bucket.numBuckets;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(numBuckets);
  }

  @Override
  public String toString() {
    return "bucket[" + numBuckets + "]";
  }

  @Override
  public UnboundPredicate<Integer> project(String name, BoundPredicate<T> predicate) {
    if (predicate.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, name, predicate);
    }

    if (predicate.isUnaryPredicate()) {
      return Expressions.predicate(predicate.op(), name);
    } else if (predicate.isLiteralPredicate() && predicate.op() == Expression.Operation.EQ) {
      return Expressions.predicate(
          predicate.op(), name, apply(predicate.asLiteralPredicate().literal().value()));
    } else if (predicate.isSetPredicate() && predicate.op() == Expression.Operation.IN) { // notIn can't be projected
      return ProjectionUtil.transformSet(name, predicate.asSetPredicate(), this);
    }

    // comparison predicates can't be projected, notEq can't be projected
    // TODO: small ranges can be projected.
    // for example, (x > 0) and (x < 3) can be turned into in({1, 2}) and projected.
    return null;
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<T> predicate) {
    if (predicate.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, name, predicate);
    }

    if (predicate.isUnaryPredicate()) {
      return Expressions.predicate(predicate.op(), name);
    } else if (predicate.isLiteralPredicate() && predicate.op() == Expression.Operation.NOT_EQ) {
      // TODO: need to translate not(eq(...)) into notEq in expressions
      return Expressions.predicate(predicate.op(), name, apply(predicate.asLiteralPredicate().literal().value()));
    } else if (predicate.isSetPredicate() && predicate.op() == Expression.Operation.NOT_IN) {
      return ProjectionUtil.transformSet(name, predicate.asSetPredicate(), this);
    }

    // no strict projection for comparison or equality
    return null;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  private static class BucketInteger extends Bucket<Integer> {
    private BucketInteger(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(Integer value) {
      return MURMUR3.hashLong(value.longValue()).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.INTEGER || type.typeId() == TypeID.DATE;
    }
  }

  private static class BucketLong extends Bucket<Long> {
    private BucketLong(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(Long value) {
      return MURMUR3.hashLong(value).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.LONG ||
          type.typeId() == TypeID.TIME ||
          type.typeId() == TypeID.TIMESTAMP;

    }
  }

  // bucketing by Double is not allowed by the spec, but this has the float hash implementation
  static class BucketFloat extends Bucket<Float> {
    // used by tests because the factory method will not instantiate a bucket function for floats
    BucketFloat(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(Float value) {
      return MURMUR3.hashLong(Double.doubleToLongBits((double) value)).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.FLOAT;
    }
  }

  // bucketing by Double is not allowed by the spec, but this has the double hash implementation
  static class BucketDouble extends Bucket<Double> {
    // used by tests because the factory method will not instantiate a bucket function for doubles
    BucketDouble(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(Double value) {
      return MURMUR3.hashLong(Double.doubleToLongBits(value)).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.DOUBLE;
    }
  }

  private static class BucketString extends Bucket<CharSequence> {
    private BucketString(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(CharSequence value) {
      return MURMUR3.hashString(value, StandardCharsets.UTF_8).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.STRING;
    }
  }

  private static class BucketByteBuffer extends Bucket<ByteBuffer> {
    private static final Set<TypeID> SUPPORTED_TYPES = Sets.newHashSet(
        TypeID.BINARY, TypeID.FIXED);

    private BucketByteBuffer(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(ByteBuffer value) {
      if (value.hasArray()) {
        return MURMUR3.hashBytes(value.array(),
            value.arrayOffset() + value.position(),
            value.arrayOffset() + value.remaining()).asInt();
      } else {
        int position = value.position();
        byte[] copy = new byte[value.remaining()];
        try {
          value.get(copy);
        } finally {
          // make sure the buffer position is unchanged
          value.position(position);
        }
        return MURMUR3.hashBytes(copy).asInt();
      }
    }

    @Override
    public boolean canTransform(Type type) {
      return SUPPORTED_TYPES.contains(type.typeId());
    }
  }

  private static class BucketUUID extends Bucket<UUID> {
    private BucketUUID(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(UUID value) {
      return MURMUR3.newHasher(16)
              .putLong(Long.reverseBytes(value.getMostSignificantBits()))
              .putLong(Long.reverseBytes(value.getLeastSignificantBits()))
              .hash().asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.UUID;
    }
  }

  private static class BucketDecimal extends Bucket<BigDecimal> {
    private BucketDecimal(int numBuckets) {
      super(numBuckets);
    }

    @Override
    public int hash(BigDecimal value) {
      return MURMUR3.hashBytes(value.unscaledValue().toByteArray()).asInt();
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == TypeID.DECIMAL;
    }
  }
}
