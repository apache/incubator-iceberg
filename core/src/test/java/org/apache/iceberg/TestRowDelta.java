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

import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDelta extends V2TableTestBase {
  @Test
  public void testAddDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 1", 1, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    Assert.assertEquals("Delta commit should use operation 'overwrite'", DataOperations.OVERWRITE, snap.operation());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistDefaults() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A
    table.newOverwrite()
        .deleteFile(FILE_A)
        .addFile(FILE_A2)
        .commit();

    // delete FILE_B
    table.newDelete()
        .deleteFile(FILE_B)
        .commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class, "Cannot commit, missing data files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());

    table.newRowDelta()
        .addDeletes(FILE_B_DELETES)
        .validateDataFilesExist(ImmutableList.of(FILE_B.path()))
        .validateFromSnapshot(validateFromSnapshotId)
        .commit();

    Assert.assertEquals("Table should have one new delete manifest",
        1, table.currentSnapshot().deleteManifests().size());
    ManifestFile deletes = table.currentSnapshot().deleteManifests().get(0);
    validateDeleteManifest(deletes,
        seqs(4),
        ids(table.currentSnapshot().snapshotId()),
        files(FILE_B_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistOverwrite() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A
    table.newOverwrite()
        .deleteFile(FILE_A)
        .addFile(FILE_A2)
        .commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class, "Cannot commit, missing data files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());
  }

  @Test
  public void testValidateDataFilesExistReplacePartitions() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A's partition
    table.newReplacePartitions()
        .addFile(FILE_A2)
        .commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class, "Cannot commit, missing data files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());
  }

  @Test
  public void testValidateDataFilesExistFromSnapshot() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A's partition
    table.newReplacePartitions()
        .addFile(FILE_A2)
        .commit();

    // test changes to the table back to the snapshot where FILE_A was overwritten
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();
    long replaceSnapshotId = table.currentSnapshot().snapshotId();

    // even though FILE_A was deleted, it happened before the "from" snapshot, so the validation allows it
    table.newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(validateFromSnapshotId)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests().size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(replaceSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A deleted
    validateManifest(
        snap.dataManifests().get(1),
        seqs(2, 1),
        ids(replaceSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals("Should have 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(3),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistRewrite() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // rewrite FILE_A
    table.newRewrite()
        .rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_A2))
        .commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class, "Cannot commit, missing data files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());
  }

  @Test
  public void testValidateDataFilesExistValidateDeletes() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class, "Cannot commit, missing data files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateDeletedFiles()
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());
  }

  @Test
  public void testValidateNoConflicts() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newAppend()
        .appendFile(FILE_A2)
        .commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A2 was added",
        ValidationException.class, "Found conflicting files",
        () -> table.newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateNoConflictingAppends(Expressions.equal("data", "u")) // bucket16("u") -> 0
            .commit());

    Assert.assertEquals("Table state should not be modified by failed RowDelta operation",
        appendSnapshotId, table.currentSnapshot().snapshotId());

    Assert.assertEquals("Table should not have any delete manifests",
        0, table.currentSnapshot().deleteManifests().size());
  }

  @Test
  public void testValidateNoConflictsFromSnapshot() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newAppend()
        .appendFile(FILE_A2)
        .commit();

    // even though FILE_A2 was added, it happened before the "from" snapshot, so the validation allows it
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    table.newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateDeletedFiles()
        .validateFromSnapshot(validateFromSnapshotId)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .validateNoConflictingAppends(Expressions.equal("data", "u")) // bucket16("u") -> 0
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests().size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(validateFromSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A added
    validateManifest(
        snap.dataManifests().get(1),
        seqs(1),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals("Should have 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(3),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testOverwriteWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting by a filter will also remove delete files that match because all matching data files are removed.
    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal(Expressions.bucket("data", 16), 0))
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplacePartitionsWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting the partition will also remove delete files that match because all matching data files are removed.
    table.newReplacePartitions()
        .addFile(FILE_A2)
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 2 data manifests", 2, snap.dataManifests().size());
    int deleteManifestPos = snap.dataManifests().get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        snap.dataManifests().get(deleteManifestPos),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        snap.dataManifests().get(appendManifestPos),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testDeleteByExpressionWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting with a filter will also remove delete files that match because all matching data files are removed.
    table.newDelete()
        .deleteFromRowFilter(Expressions.alwaysTrue())
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 2),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.DELETED));
  }

  @Test
  public void testDeleteDataFileWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, deleteSnap.dataManifests().size());
    validateManifest(
        deleteSnap.dataManifests().get(0),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, deleteSnap.deleteManifests().size());
    validateDeleteManifest(
        deleteSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next commit, causing the min sequence number of all data files
    // to be 2, the largest known sequence number. this will cause FILE_A_DELETES to be removed because it is too old
    // to apply to any data files.
    table.newDelete()
        .deleteFile("no-such-file")
        .commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 0 data manifests", 0, nextSnap.dataManifests().size());
    Assert.assertEquals("Should produce 1 delete manifest", 1, nextSnap.deleteManifests().size());
    validateDeleteManifest(
        nextSnap.deleteManifests().get(0),
        seqs(3),
        ids(nextSnap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.DELETED));
  }

  @Test
  public void testFastAppendDoesNotRemoveStaleDeleteFiles() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, deleteSnap.dataManifests().size());
    validateManifest(
        deleteSnap.dataManifests().get(0),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, deleteSnap.deleteManifests().size());
    validateDeleteManifest(
        deleteSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next merging commit, but FastAppend will not remove it
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, nextSnap.dataManifests().size());
    int deleteManifestPos = nextSnap.dataManifests().get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        nextSnap.dataManifests().get(deleteManifestPos),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        nextSnap.dataManifests().get(appendManifestPos),
        seqs(3),
        ids(nextSnap.snapshotId()),
        files(FILE_B),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, nextSnap.deleteManifests().size());
    validateDeleteManifest(
        nextSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesToRecentRewrite() {
    table.newAppend()
            .appendFile(FILE_A)
            .appendFile(FILE_B)
            .commit();

    long expireSnapshotId = table.currentSnapshot().snapshotId();

    table.newRewrite()
            .rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_C, FILE_D))
            .commit();

    // overwrite FILE_B
    table.newOverwrite()
            .deleteFile(FILE_B)
            .commit();

    Assert.assertEquals("Table should not have any delete manifests",
            0, table.currentSnapshot().deleteManifests().size());

    table.expireSnapshots().expireSnapshotId(expireSnapshotId).commit();

    AssertHelpers.assertThrows("Should fail to add FILE_A_DELETES because FILE_A is missing",
            ValidationException.class, "Cannot commit, missing data files",
            () -> table.newRowDelta()
                    .addDeletes(FILE_A_DELETES)
                    .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                    .commit());

    table.newRowDelta()
            .addDeletes(FILE_C_DELETES)
            .validateDataFilesExist(ImmutableList.of(FILE_C.path()))
            .commit();

    Assert.assertEquals("Table should have one new delete manifest",
            1, table.currentSnapshot().deleteManifests().size());
    ManifestFile deletes = table.currentSnapshot().deleteManifests().get(0);
    validateDeleteManifest(deletes,
            seqs(4),
            ids(table.currentSnapshot().snapshotId()),
            files(FILE_C_DELETES),
            statuses(Status.ADDED));
  }
}
