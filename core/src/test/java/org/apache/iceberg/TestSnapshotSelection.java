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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotSelection extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSnapshotSelection(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testSnapshotSelectionById() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    Assert.assertEquals("Table should have two snapshots", 2, Iterables.size(table.snapshots()));
    validateSnapshot(null, table.snapshot(firstSnapshot.snapshotId()), FILE_A);
    validateSnapshot(firstSnapshot, table.snapshot(secondSnapshot.snapshotId()), FILE_B);
  }

  @Test
  public void testSnapshotStatsForAddedFiles() {
    DataFile fileWithStats = DataFiles.builder(SPEC)
        .withPath("/path/to/data-with-stats.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0")
        .withRecordCount(10)
        .withMetrics(new Metrics(3L,
            null, // no column sizes
            ImmutableMap.of(1, 3L), // value count
            ImmutableMap.of(1, 0L), // null count
            ImmutableMap.of(1, longToBuffer(20L)), // lower bounds
            ImmutableMap.of(1, longToBuffer(22L)))) // upper bounds
        .build();

    table.newFastAppend()
        .appendFile(fileWithStats)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    Iterable<DataFile> addedFiles = snapshot.addedFiles();
    Assert.assertEquals(1, Iterables.size(addedFiles));
    DataFile dataFile = Iterables.getOnlyElement(addedFiles);
    Assert.assertNotNull("Value counts should be not null", dataFile.valueCounts());
    Assert.assertNotNull("Null value counts should be not null", dataFile.nullValueCounts());
    Assert.assertNotNull("Lower bounds should be not null", dataFile.lowerBounds());
    Assert.assertNotNull("Upper bounds should be not null", dataFile.upperBounds());
  }

  private ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
