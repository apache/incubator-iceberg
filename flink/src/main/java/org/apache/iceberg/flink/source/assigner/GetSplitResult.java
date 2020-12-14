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

package org.apache.iceberg.flink.source.assigner;

import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

@Internal
public class GetSplitResult {

  public enum Status {

    AVAILABLE,

    /**
     * There are pending splits. But they can't be assigned
     * due to constraints (like event time alignment)
     */
    CONSTRAINED,

    /**
     * Assigner doesn't have pending splits.
     */
    UNAVAILABLE
  }

  private final Status status;
  @Nullable
  private final IcebergSourceSplit split;

  public GetSplitResult(Status status) {
    this(status, null);
  }

  public GetSplitResult(Status status, @Nullable IcebergSourceSplit split) {
    if (null == split && status == Status.AVAILABLE) {
      throw new IllegalArgumentException("Available status must have a non-null split");
    }
    this.status = status;
    this.split = split;
  }

  public Status status() {
    return status;
  }

  public IcebergSourceSplit split() {
    return split;
  }
}
