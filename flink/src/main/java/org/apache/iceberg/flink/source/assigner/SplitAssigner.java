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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;

/**
 * SplitAssigner interface is extracted out as a separate component so that
 * we can plug in different split assignment strategy for different requirements. E.g.
 * <ul>
 * <li> Simple assigner with no ordering guarantee or locality aware optimization.</li>
 * <li> Locality aware assigner that prefer splits that are local.</li>
 * <li> Snapshot aware assigner that assign splits based on the order they are committed.</li>
 * <li> Event time alignment assigner that assign splits satisfying certain time ordering
 *      within a single source or across sources.</li>
 * </ul>
 *
 * <p>
 * Enumerator should call the assigner APIs from the coordinator thread.
 * This is to simplify the thread safety for assigner implementation.
 */
public interface SplitAssigner extends AutoCloseable {

  /**
   * Some assigners may need to start background threads or perform other activity such as
   * registering as listeners to updates from other event sources e.g., watermark tracker.
   */
  default void start() {
  }

  /**
   * Some assigners may need to perform certain actions
   * when their corresponding enumerators are closed
   */
  @Override
  default void close() {
  }

  /**
   * Request a new split from the assigner
   * when enumerator trying to assign splits to awaiting readers.
   * <p>
   * If enumerator wasn't able to assign the split (e.g., reader disconnected),
   * enumerator should call {@link SplitAssigner#onUnassignedSplits} to return the split.
   */
  GetSplitResult getNext(@Nullable String hostname);

  /**
   * Add new splits discovered by enumerator
   */
  void onDiscoveredSplits(Collection<IcebergSourceSplit> splits);

  /**
   * Forward addSplitsBack event (for failed reader) to assigner
   */
  void onUnassignedSplits(Collection<IcebergSourceSplit> splits);

  /**
   * Some assigner (like event time alignment) may rack in-progress splits
   * to advance watermark upon completed splits
   */
  default void onCompletedSplits(Collection<String> completedSplitIds) {
  }

  /**
   * Get assigner state for checkpointing.
   * This is a super-set API that works for all currently imagined assigners.
   */
  Map<IcebergSourceSplit, IcebergSourceSplitStatus> state();

  /**
   * Enumerator can get a notification via CompletableFuture
   * when the assigner has more splits available later.
   * Enumerator should schedule assignment in the thenAccept action of the future.
   * <p>
   * Assigner will return the same future if this method is called again
   * before the previous future is completed.
   * <p>
   * The future can be completed from other thread,
   * e.g. the coordinator thread from another thread
   * for event time alignment.
   * <p>
   * If enumerator need to trigger action upon the future completion,
   * it may want to run it in the coordinator thread
   * using {@link SplitEnumeratorContext#runInCoordinatorThread(Runnable)}.
   */
  CompletableFuture<Void> isAvailable();
}
