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

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

/**
 * Enumerator state for checkpointing
 */
public class IcebergEnumeratorState implements Serializable {

  @Nullable
  private final IcebergEnumeratorPosition enumeratorPosition;
  private final Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits;

  public IcebergEnumeratorState(Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits) {
    this(null, pendingSplits);
  }

  public IcebergEnumeratorState(
      @Nullable IcebergEnumeratorPosition enumeratorPosition,
      Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits) {
    this.enumeratorPosition = enumeratorPosition;
    this.pendingSplits = pendingSplits;
  }

  @Nullable
  public IcebergEnumeratorPosition enumeratorPosition() {
    return enumeratorPosition;
  }

  public Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits() {
    return pendingSplits;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        enumeratorPosition,
        pendingSplits);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergEnumeratorState other = (IcebergEnumeratorState) o;
    return Objects.equal(enumeratorPosition, other.enumeratorPosition()) &&
        Objects.equal(pendingSplits, other.pendingSplits());
  }
}
