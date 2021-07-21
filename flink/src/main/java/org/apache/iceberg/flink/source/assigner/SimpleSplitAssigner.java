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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;

/**
 * This assigner hands out splits without any guarantee in order or locality.
 * <p>
 * Since all methods are called in the source coordinator thread by enumerator,
 * there is no need for locking.
 */
public class SimpleSplitAssigner implements SplitAssigner {

  private final Deque<IcebergSourceSplit> pendingSplits;
  private CompletableFuture<Void> availableFuture;

  SimpleSplitAssigner(Deque<IcebergSourceSplit> pendingSplits) {
    this.pendingSplits = pendingSplits;
  }

  public SimpleSplitAssigner() {
    this(new ArrayDeque<>());
  }

  public SimpleSplitAssigner(Map<IcebergSourceSplit, IcebergSourceSplitStatus> state) {
    this(new ArrayDeque<>(state.keySet()));
  }

  @Override
  public GetSplitResult getNext(@Nullable String hostname) {
    if (pendingSplits.isEmpty()) {
      return new GetSplitResult(GetSplitResult.Status.UNAVAILABLE);
    } else {
      IcebergSourceSplit split = pendingSplits.poll();
      return new GetSplitResult(GetSplitResult.Status.AVAILABLE, split);
    }
  }

  @Override
  public void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    pendingSplits.addAll(splits);
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    pendingSplits.addAll(splits);
    completeAvailableFuturesIfNeeded();
  }

  /**
   * Simple assigner only tracks unassigned splits
   */
  @Override
  public Map<IcebergSourceSplit, IcebergSourceSplitStatus> state() {
    return pendingSplits.stream()
        .collect(Collectors.toMap(
            split -> split,
            split -> IcebergSourceSplitStatus.UNASSIGNED));
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
