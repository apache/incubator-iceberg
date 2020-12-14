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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.SplitHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestSimpleSplitAssigner {

  @Test
  public void testEmptyInitialization() {
    final SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  /**
   * Test a sequence of interactions for StaticEnumerator
   */
  @Test
  public void testStaticEnumeratorSequence() {
    final SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assigner.onDiscoveredSplits(SplitHelpers.createMockedSplits(2));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(SplitHelpers.createMockedSplits(1));
    assertSnapshot(assigner, 2);

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  /**
   * Test a sequence of interactions for ContinuousEnumerator
   */
  @Test
  public void testContinuousEnumeratorSequence() {
    final SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);

    assertAvailableFuture(assigner, 1, () -> assigner.onDiscoveredSplits(SplitHelpers.createMockedSplits(1)));
    assertAvailableFuture(assigner, 1, () -> assigner.onUnassignedSplits(SplitHelpers.createMockedSplits(1)));

    assigner.onDiscoveredSplits(SplitHelpers.createMockedSplits(2));
    assertSnapshot(assigner, 2);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  private void assertAvailableFuture(SimpleSplitAssigner assigner, int splitCount, Runnable addSplitsRunnable) {
    // register callback
    final AtomicBoolean futureCompleted = new AtomicBoolean();
    final CompletableFuture<Void> future = assigner.isAvailable();
    future.thenAccept(ignored -> futureCompleted.set(true));
    // calling isAvailable again should return the same object reference
    // note that thenAccept will return a new future.
    // we want to assert the same instance on the assigner returned future
    Assert.assertSame(future, assigner.isAvailable());

    // now add some splits
    addSplitsRunnable.run();
    Assert.assertEquals(true, futureCompleted.get());

    for (int i = 0; i < splitCount; ++i) {
      assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    }
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  private void assertGetNext(SimpleSplitAssigner assigner, GetSplitResult.Status status) {
    final GetSplitResult result = assigner.getNext(null);
    Assert.assertEquals(status, result.status());
    switch (status) {
      case AVAILABLE:
        Assert.assertNotNull(result.split());
        break;
      case CONSTRAINED:
      case UNAVAILABLE:
        Assert.assertNull(result.split());
        break;
      default:
        Assert.fail("Unknown status: " + status);
    }
  }

  private void assertSnapshot(SimpleSplitAssigner assigner, int splitCount) {
    final Map<IcebergSourceSplit, IcebergSourceSplitStatus> stateBeforeGet = assigner.state();
    Assert.assertEquals(splitCount, stateBeforeGet.size());
  }
}
