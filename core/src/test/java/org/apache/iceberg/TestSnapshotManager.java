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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotManager extends TableTestBase {

  // replacement for FILE_A
  static final DataFile REPLACEMENT_FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a-replacement.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  // conflict in the same partition as FILE_A
  static final DataFile CONFLICT_FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a-conflict.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSnapshotManager(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testCherryPickDynamicOverwrite() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions()
        .addFile(REPLACEMENT_FILE_A)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    // pick the snapshot into the current state
    table.manageSnapshots()
        .cherrypick(staged.snapshotId())
        .commit();

    Assert.assertNotEquals("Should not fast-forward", staged.snapshotId(), table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteWithoutParent() {
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions()
        .addFile(REPLACEMENT_FILE_A)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    // pick the snapshot into the current state
    table.manageSnapshots()
        .cherrypick(staged.snapshotId())
        .commit();

    Assert.assertNotEquals("Should not fast-forward", staged.snapshotId(), table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteConflict() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions()
        .addFile(REPLACEMENT_FILE_A)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend()
        .appendFile(CONFLICT_FILE_A)
        .commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    AssertHelpers.assertThrows("Should reject partition replacement when a partition has been modified",
        ValidationException.class, "Cannot cherry-pick replace partitions with changed partition",
        () -> table.manageSnapshots()
        .cherrypick(staged.snapshotId())
        .commit());

    Assert.assertEquals("Failed cherry-pick should not change the table state",
        lastSnapshotId, table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A, CONFLICT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteDeleteConflict() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions()
        .addFile(REPLACEMENT_FILE_A)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add FILE_B s
    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    // delete FILE_A so the replace operation is no longer valid
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    AssertHelpers.assertThrows("Should reject partition replacement when a partition has been modified",
        ValidationException.class, "Missing required files to delete",
        () -> table.manageSnapshots()
            .cherrypick(staged.snapshotId())
            .commit());

    Assert.assertEquals("Failed cherry-pick should not change the table state",
        lastSnapshotId, table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B);
  }

  @Test
  public void testCherryPickFromBranch() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    long branchSnapshotId = table.currentSnapshot().snapshotId();

    // add a second commit before replacing FILE_A
    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    // replace FILE_A
    table.newReplacePartitions()
        .addFile(REPLACEMENT_FILE_A)
        .commit();
    long replaceSnapshotId = table.currentSnapshot().snapshotId();

    // rewrite history so the replacement is in a branch, not base directly on an ancestor of the current state
    table.manageSnapshots()
        .rollbackTo(branchSnapshotId)
        .commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    AssertHelpers.assertThrows("Should reject partition replacement when a partition has been modified",
        ValidationException.class, "Cannot cherry-pick overwrite not based on an ancestor of the current state",
        () -> table.manageSnapshots()
            .cherrypick(replaceSnapshotId)
            .commit());

    Assert.assertEquals("Failed cherry-pick should not change the table state",
        lastSnapshotId, table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @Test
  public void testCherryPickOverwrite() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // stage an overwrite to replace FILE_A
    table.newOverwrite()
        .deleteFile(FILE_A)
        .addFile(REPLACEMENT_FILE_A)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend()
        .appendFile(FILE_B)
        .commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    AssertHelpers.assertThrows("Should reject partition replacement when a partition has been modified",
        ValidationException.class, "Cannot fast-forward to non-append snapshot",
        () -> table.manageSnapshots()
            .cherrypick(staged.snapshotId())
            .commit());

    Assert.assertEquals("Failed cherry-pick should not change the table state",
        lastSnapshotId, table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A, FILE_B);
  }
}
