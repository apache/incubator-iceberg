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

package org.apache.iceberg.flink.source.enumerator;

import javax.annotation.Nullable;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

class IcebergEnumeratorPosition {

  @Nullable private final Long startSnapshotId;
  @Nullable private final Long startSnapshotTimestampMs;
  private final long endSnapshotId;
  private final long endSnapshotTimestampMs;

  private IcebergEnumeratorPosition(Builder builder) {
    this.startSnapshotId = builder.startSnapshotId;
    this.startSnapshotTimestampMs = builder.startSnapshotTimestampMs;
    this.endSnapshotId = builder.endSnapshotId;
    this.endSnapshotTimestampMs = builder.endSnapshotTimestampMs;
  }

  @Nullable
  public Long startSnapshotId() {
    return startSnapshotId;
  }

  @Nullable
  public Long startSnapshotTimestampMs() {
    return startSnapshotTimestampMs;
  }

  public long endSnapshotId() {
    return endSnapshotId;
  }

  public long endSnapshotTimestampMs() {
    return endSnapshotTimestampMs;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Long startSnapshotId;
    private Long startSnapshotTimestampMs;
    private Long endSnapshotId;
    private Long endSnapshotTimestampMs;

    private Builder() {
    }

    public Builder startSnapshotId(Long value) {
      this.startSnapshotId = value;
      return this;
    }

    public Builder startSnapshotTimestampMs(Long value) {
      this.startSnapshotTimestampMs = value;
      return this;
    }

    public Builder endSnapshotId(long value) {
      this.endSnapshotId = value;
      return this;
    }

    public Builder endSnapshotTimestampMs(long value) {
      this.endSnapshotTimestampMs = value;
      return this;
    }

    public IcebergEnumeratorPosition build() {
      checkRequired();
      return new IcebergEnumeratorPosition(this);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(endSnapshotId, "Must set endSnapshotId");
      Preconditions.checkNotNull(endSnapshotTimestampMs, "Must set endSnapshotTimestampMs");
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("startSnapshotId", startSnapshotId)
        .add("startSnapshotTimestampMs", startSnapshotTimestampMs)
        .add("endSnapshotId", endSnapshotId)
        .add("endSnapshotTimestampMs", endSnapshotTimestampMs)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        startSnapshotId,
        startSnapshotTimestampMs,
        endSnapshotId,
        endSnapshotTimestampMs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergEnumeratorPosition other = (IcebergEnumeratorPosition) o;
    return Objects.equal(startSnapshotId, other.startSnapshotId()) &&
        Objects.equal(startSnapshotTimestampMs, other.startSnapshotTimestampMs()) &&
        Objects.equal(endSnapshotId, other.endSnapshotId()) &&
        Objects.equal(endSnapshotTimestampMs, other.endSnapshotTimestampMs());
  }
}
