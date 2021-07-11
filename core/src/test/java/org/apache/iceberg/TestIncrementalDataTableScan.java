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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIncrementalDataTableScan extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestIncrementalDataTableScan(int formatVersion) {
    super(formatVersion);
  }

  @Before
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();
  }

  @Test
  public void testInvalidScans() {
    add(table.newAppend(), files("A"));
    AssertHelpers.assertThrows(
        "from and to snapshots cannot be the same, since from snapshot is exclusive and not part of the scan",
        IllegalArgumentException.class, "from and to snapshot ids cannot be the same",
        () -> appendsBetweenScan(1, 1));

    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E"));
    AssertHelpers.assertThrows(
        "Check refinement api",
        IllegalArgumentException.class, "from snapshot id 1 not in existing snapshot ids range (2, 4]",
        () -> table.newScan().dataBetween(2, 5).dataBetween(1, 4));
    AssertHelpers.assertThrows(
        "Check refinement api",
        IllegalArgumentException.class, "to snapshot id 3 not in existing snapshot ids range (1, 2]",
        () -> table.newScan().dataBetween(1, 2).dataBetween(1, 3));
  }

  @Test
  public void testAppends() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E")); // 5

    class MyListener implements Listener<IncrementalScanEvent> {

      IncrementalScanEvent lastEvent = null;

      public void notify(IncrementalScanEvent event) {
        this.lastEvent = event;
      }

      public IncrementalScanEvent event() {
        return lastEvent;
      }
    }

    MyListener listener1 = new MyListener();
    Listeners.register(listener1, IncrementalScanEvent.class);
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));
    Assert.assertTrue(listener1.event().fromSnapshotId() == 1);
    Assert.assertTrue(listener1.event().toSnapshotId() == 5);
    filesMatch(Lists.newArrayList("C", "D", "E"), appendsBetweenScan(2, 5));
    Assert.assertTrue(listener1.event().fromSnapshotId() == 2);
    Assert.assertTrue(listener1.event().toSnapshotId() == 5);
    Assert.assertEquals(table.schema(), listener1.event().projection());
    Assert.assertEquals(Expressions.alwaysTrue(), listener1.event().filter());
    Assert.assertEquals("test", listener1.event().tableName());
  }

  @Test
  public void testReplaceOverwritesDeletes() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E")); // 5
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));

    replace(table.newRewrite(), files("A", "B", "C"), files("F", "G")); // 6
    // Replace commits are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 6));
    filesMatch(Lists.newArrayList("E"), appendsBetweenScan(4, 6));
    // 6th snapshot is a replace. No new content is added
    Assert.assertTrue("Replace commits are ignored", appendsBetweenScan(5, 6).isEmpty());
    delete(table.newDelete(), files("D")); // 7
    // 7th snapshot is a delete.
    Assert.assertTrue("Replace and delete commits are ignored", appendsBetweenScan(5, 7).isEmpty());
    Assert.assertTrue("Delete commits are ignored", appendsBetweenScan(6, 7).isEmpty());
    add(table.newAppend(), files("I")); // 8
    // snapshots 6 and 7 are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E", "I"), appendsBetweenScan(1, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(6, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(7, 8));

    overwrite(table.newOverwrite(), files("H"), files("E")); // 9
    AssertHelpers.assertThrows(
        "Overwrites are not supported for Incremental scan", UnsupportedOperationException.class,
        "Found overwrite operation, cannot support incremental data in snapshots (8, 9]",
        () -> appendsBetweenScan(8, 9));
  }

  @Test
  public void testTransactions() {
    Transaction transaction = table.newTransaction();

    add(transaction.newAppend(), files("A")); // 1
    add(transaction.newAppend(), files("B"));
    add(transaction.newAppend(), files("C"));
    add(transaction.newAppend(), files("D"));
    add(transaction.newAppend(), files("E")); // 5
    transaction.commitTransaction();
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));

    transaction = table.newTransaction();
    replace(transaction.newRewrite(), files("A", "B", "C"), files("F", "G")); // 6
    transaction.commitTransaction();
    // Replace commits are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 6));
    filesMatch(Lists.newArrayList("E"), appendsBetweenScan(4, 6));
    // 6th snapshot is a replace. No new content is added
    Assert.assertTrue("Replace commits are ignored", appendsBetweenScan(5, 6).isEmpty());

    transaction = table.newTransaction();
    delete(transaction.newDelete(), files("D")); // 7
    transaction.commitTransaction();
    // 7th snapshot is a delete.
    Assert.assertTrue("Replace and delete commits are ignored", appendsBetweenScan(5, 7).isEmpty());
    Assert.assertTrue("Delete commits are ignored", appendsBetweenScan(6, 7).isEmpty());

    transaction = table.newTransaction();
    add(transaction.newAppend(), files("I")); // 8
    transaction.commitTransaction();
    // snapshots 6, 7 and 8 are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E", "I"), appendsBetweenScan(1, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(6, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(7, 8));
  }

  @Test
  public void testRollbacks() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C")); // 3
    // Go back to snapshot "B"
    table.rollback().toSnapshotId(2).commit(); // 2
    Assert.assertEquals(2, table.currentSnapshot().snapshotId());
    filesMatch(Lists.newArrayList("B"), appendsBetweenScan(1, 2));
    filesMatch(Lists.newArrayList("B"), appendsAfterScan(1));

    Transaction transaction = table.newTransaction();
    add(transaction.newAppend(), files("D")); // 4
    add(transaction.newAppend(), files("E")); // 5
    add(transaction.newAppend(), files("F"));
    transaction.commitTransaction();
    // Go back to snapshot "E"
    table.rollback().toSnapshotId(5).commit();
    Assert.assertEquals(5, table.currentSnapshot().snapshotId());
    filesMatch(Lists.newArrayList("B", "D", "E"), appendsBetweenScan(1, 5));
    filesMatch(Lists.newArrayList("B", "D", "E"), appendsAfterScan(1));
  }

  @Test
  public void testIgnoreResiduals() throws IOException {
    add(table.newAppend(), files("A"));
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));

    TableScan scan1 = table.newScan()
        .filter(Expressions.equal("id", 5))
        .dataBetween(1, 3);

    try (CloseableIterable<CombinedScanTask> tasks = scan1.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", com.google.common.collect.Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }

    TableScan scan2 = table.newScan()
        .filter(Expressions.equal("id", 5))
        .dataBetween(1, 3)
        .ignoreResiduals();

    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", com.google.common.collect.Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
            .withPath(name + ".parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
  }

  private static void add(AppendFiles appendFiles, List<DataFile> adds) {
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private static void delete(DeleteFiles deleteFiles, List<DataFile> deletes) {
    for (DataFile f : deletes) {
      deleteFiles.deleteFile(f);
    }
    deleteFiles.commit();
  }

  private static void replace(RewriteFiles rewriteFiles, List<DataFile> deletes, List<DataFile> adds) {
    rewriteFiles.rewriteFiles(Sets.newHashSet(deletes), Sets.newHashSet(adds));
    rewriteFiles.commit();
  }

  private static void overwrite(OverwriteFiles overwriteFiles, List<DataFile> adds, List<DataFile> deletes) {
    for (DataFile f : adds) {
      overwriteFiles.addFile(f);
    }
    for (DataFile f : deletes) {
      overwriteFiles.deleteFile(f);
    }
    overwriteFiles.commit();
  }

  private static List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestIncrementalDataTableScan::file);
  }

  private List<String> appendsAfterScan(long fromSnapshotId) {
    final TableScan appendsAfter = table.newScan().dataAfter(fromSnapshotId);
    return filesToScan(appendsAfter);
  }

  private List<String> appendsBetweenScan(long fromSnapshotId, long toSnapshotId) {
    Snapshot s1 = table.snapshot(fromSnapshotId);
    Snapshot s2 = table.snapshot(toSnapshotId);
    TableScan appendsBetween = table.newScan().dataBetween(s1.snapshotId(), s2.snapshotId());
    return filesToScan(appendsBetween);
  }

  private static List<String> filesToScan(TableScan tableScan) {
    Iterable<String> filesToRead = Iterables.transform(tableScan.planFiles(), t -> {
      String path = t.file().path().toString();
      return path.split("\\.")[0];
    });
    return Lists.newArrayList(filesToRead);
  }

  private static void filesMatch(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
}
