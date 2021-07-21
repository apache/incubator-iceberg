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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.assigner.GetSplitResult;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: publish enumerator monitor metrics like number of pending metrics
 * after FLINK-21000 is resolved
 */
abstract class AbstractIcebergEnumerator implements
    SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext;
  private final SplitAssigner assigner;
  private final Map<Integer, String> readersAwaitingSplit;
  private final AtomicReference<CompletableFuture<Void>> availableFuture;

  AbstractIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner) {
    this.enumeratorContext = enumeratorContext;
    this.assigner = assigner;
    this.readersAwaitingSplit = new LinkedHashMap<>();
    this.availableFuture = new AtomicReference<>();
  }

  @Override
  public void start() {
    assigner.start();
  }

  @Override
  public void close() throws IOException {
    assigner.close();
  }

  /**
   * Iceberg source uses a custom event inside handleSourceEvent
   * so that we can piggyback the finishedSplitIds in SplitRequestEvent.
   * We can move the logic back into this method once FLINK-21364 is resolved.
   */
  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    throw new UnsupportedOperationException("Iceberg source uses its own SplitRequestEvent");
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof IcebergSourceEvents.SplitRequestEvent) {
      final IcebergSourceEvents.SplitRequestEvent splitRequestEvent =
          (IcebergSourceEvents.SplitRequestEvent) sourceEvent;
      LOG.info("Received request split event from subtask {}", subtaskId);
      assigner.onCompletedSplits(splitRequestEvent.finishedSplitIds());
      readersAwaitingSplit.put(subtaskId, splitRequestEvent.requesterHostname());
      assignSplits();
    } else {
      throw new IllegalArgumentException(String.format(
          "Received unknown event %s from subtask %d", sourceEvent, subtaskId));
    }
  }

  @Override
  public void addSplitsBack(List<IcebergSourceSplit> splits, int subtaskId) {
    LOG.info("Add {} splits back to the pool for failed subtask {}: {}",
        splits.size(), subtaskId, splits);
    assigner.onUnassignedSplits(splits);
    assignSplits();
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Added reader: {}", subtaskId);
  }

  private void assignSplits() {
    LOG.info("Assigning splits for {} awaiting readers", readersAwaitingSplit.size());
    final Iterator<Map.Entry<Integer, String>> awaitingReader =
        readersAwaitingSplit.entrySet().iterator();
    while (awaitingReader.hasNext()) {
      final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
      // if the reader that requested another split has failed in the meantime, remove
      // it from the list of waiting readers
      if (!enumeratorContext.registeredReaders().containsKey(nextAwaiting.getKey())) {
        awaitingReader.remove();
        continue;
      }

      final int awaitingSubtask = nextAwaiting.getKey();
      final String hostname = nextAwaiting.getValue();
      final GetSplitResult getResult = assigner.getNext(hostname);
      if (getResult.status() == GetSplitResult.Status.AVAILABLE) {
        LOG.info("Assign split to subtask {}: {}", awaitingSubtask, getResult.split());
        enumeratorContext.assignSplit(getResult.split(), awaitingSubtask);
        awaitingReader.remove();
      } else if (getResult.status() == GetSplitResult.Status.CONSTRAINED) {
        getAvailableFutureIfNeeded();
        break;
      } else if (getResult.status() == GetSplitResult.Status.UNAVAILABLE) {
        if (shouldWaitForMoreSplits()) {
          getAvailableFutureIfNeeded();
          break;
        } else {
          LOG.info("No more splits available for subtask {}", awaitingSubtask);
          enumeratorContext.signalNoMoreSplits(awaitingSubtask);
          awaitingReader.remove();
        }
      } else {
        throw new IllegalArgumentException("Unsupported status: " + getResult.status());
      }
    }
  }

  /**
   * return true if enumerator should wait for splits
   * like in the continuous enumerator case
   */
  protected abstract boolean shouldWaitForMoreSplits();

  private synchronized void getAvailableFutureIfNeeded() {
    if (availableFuture.get() != null) {
      return;
    }
    final CompletableFuture<Void> future = assigner.isAvailable()
        .thenAccept(ignore ->
            // Must run assignSplits in coordinator thread
            // because the future may be completed from other threads.
            // E.g., in event time alignment assigner,
            // watermark advancement from another source may
            // cause the available future to be completed
            enumeratorContext.runInCoordinatorThread(() -> {
              LOG.debug("Executing callback of assignSplits");
              availableFuture.set(null);
              assignSplits();
            }));
    availableFuture.set(future);
    LOG.debug("Registered callback for future available splits");
  }
}
