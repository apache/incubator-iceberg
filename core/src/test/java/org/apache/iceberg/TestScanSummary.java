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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.ScanSummary.timestampRange;
import static org.apache.iceberg.ScanSummary.toMillis;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;

@RunWith(Parameterized.class)
public class TestScanSummary extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestScanSummary(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testSnapshotTimeRangeValidation() {
    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    // expire the first snapshot
    table.expireSnapshots()
        .expireOlderThan(t1)
        .commit();

    Assert.assertEquals("Should have one snapshot",
        1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("Snapshot should be the second snapshot created",
        secondSnapshotId, table.currentSnapshot().snapshotId());

    // this should include the first snapshot, but it was removed from the dataset
    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t2));

    AssertHelpers.assertThrows("Should fail summary because range may include expired snapshots",
        IllegalArgumentException.class, "may include expired snapshots",
        () -> new ScanSummary.Builder(scan).build());
  }

  @Test
  public void testTimestampRanges() {
    long lower = 1542750188523L;
    long upper = 1542750695131L;

    Assert.assertEquals("Should use inclusive bound",
        Pair.of(Long.MIN_VALUE, upper),
        timestampRange(ImmutableList.of(lessThanOrEqual("ts_ms", upper))));

    Assert.assertEquals("Should use lower value for upper bound",
        Pair.of(Long.MIN_VALUE, upper),
        timestampRange(ImmutableList.of(
            lessThanOrEqual("ts_ms", upper + 918234),
            lessThanOrEqual("ts_ms", upper))));

    Assert.assertEquals("Should make upper bound inclusive",
        Pair.of(Long.MIN_VALUE, upper - 1),
        timestampRange(ImmutableList.of(lessThan("ts_ms", upper))));

    Assert.assertEquals("Should use inclusive bound",
        Pair.of(lower, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(greaterThanOrEqual("ts_ms", lower))));

    Assert.assertEquals("Should use upper value for lower bound",
        Pair.of(lower, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower - 918234),
            greaterThanOrEqual("ts_ms", lower))));

    Assert.assertEquals("Should make lower bound inclusive",
        Pair.of(lower + 1, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(greaterThan("ts_ms", lower))));

    Assert.assertEquals("Should set both bounds for equals",
        Pair.of(lower, lower),
        timestampRange(ImmutableList.of(equal("ts_ms", lower))));

    Assert.assertEquals("Should set both bounds",
        Pair.of(lower, upper - 1),
        timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower),
            lessThan("ts_ms", upper))));

    // >= lower and < lower is an empty range
    AssertHelpers.assertThrows("Should reject empty ranges",
        IllegalArgumentException.class, "No timestamps can match filters",
        () -> timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower),
            lessThan("ts_ms", lower))));
  }

  @Test
  public void testToMillis() {
    long millis = 1542750947417L;
    Assert.assertEquals(1542750947000L, toMillis(millis / 1000));
    Assert.assertEquals(1542750947417L, toMillis(millis));
    Assert.assertEquals(1542750947417L, toMillis(millis * 1000 + 918));
  }
}
