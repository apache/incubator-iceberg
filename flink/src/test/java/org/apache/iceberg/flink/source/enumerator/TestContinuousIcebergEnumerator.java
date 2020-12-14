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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.SplitHelpers;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class TestContinuousIcebergEnumerator {

  @Test
  public void testDiscoverSplitWhenNoReaderRegistered() {
    final TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    final TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplitsEmpty = enumerator.snapshotState().pendingSplits();
    Assert.assertEquals(0, pendingSplitsEmpty.size());

    // make one split available and trigger the periodic discovery
    final List<IcebergSourceSplit> splits = SplitHelpers.createMockedSplits(1);
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits = enumerator.snapshotState().pendingSplits();
    Assert.assertEquals(1, pendingSplits.size());
    Assert.assertTrue(pendingSplits.containsKey(splits.get(0)));
    Assert.assertEquals(IcebergSourceSplitStatus.UNASSIGNED, pendingSplits.get(splits.get(0)));
  }

  @Test
  public void testDiscoverWhenReaderRegistered() {
    final TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    final TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    // register one reader, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    // make one split available and trigger the periodic discovery
    final List<IcebergSourceSplit> splits = SplitHelpers.createMockedSplits(1);
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Assert.assertTrue(enumerator.snapshotState().pendingSplits().isEmpty());
    Assert.assertThat(context.getSplitAssignments().get(2).getAssignedSplits(), CoreMatchers.hasItem(splits.get(0)));
  }

  @Test
  public void testRequestingReaderUnavailableWhenSplitDiscovered() {
    final TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    final TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    // register one reader, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    // remove the reader (like in a failure)
    context.registeredReaders().remove(2);

    // make one split available and trigger the periodic discovery
    final List<IcebergSourceSplit> splits = SplitHelpers.createMockedSplits(1);
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Assert.assertFalse(context.getSplitAssignments().containsKey(2));
    Assert.assertThat(enumerator.snapshotState().pendingSplits().keySet(), CoreMatchers.hasItem(splits.get(0)));

    // register the reader again, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    Assert.assertTrue(enumerator.snapshotState().pendingSplits().isEmpty());
    Assert.assertThat(context.getSplitAssignments().get(2).getAssignedSplits(), CoreMatchers.hasItem(splits.get(0)));
  }

  private static ContinuousIcebergEnumerator createEnumerator(
      final SplitEnumeratorContext<IcebergSourceSplit> context,
      final IcebergEnumeratorConfig config,
      final ContinuousSplitPlanner splitPlanner) {

    final ContinuousIcebergEnumerator enumerator =
        new ContinuousIcebergEnumerator(
            context,
            new SimpleSplitAssigner(Collections.emptyMap()),
            null,
            config,
            splitPlanner);
    enumerator.start();
    return enumerator;
  }

}
