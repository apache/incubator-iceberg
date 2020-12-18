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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.Test;

public class TestSequenceNumberForV2Table extends TableTestBase {

  public TestSequenceNumberForV2Table() {
    super(2);
  }

  @Test
  public void testRewrite() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap1, snap2, 2, FILE_B);
    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    table.rewriteManifests().clusterBy(file -> "").commit();
    Snapshot snap3 = table.currentSnapshot();
    ManifestFile newManifest = snap3.allManifests().stream()
        .filter(manifest -> manifest.snapshotId() == snap3.snapshotId())
        .collect(Collectors.toList()).get(0);

    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());

    // FILE_A and FILE_B in manifest may reorder
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(newManifest, FILE_IO,
        table.location(), table.properties()).entries()) {
      if (entry.file().path().equals(FILE_A.path())) {
        V2Assert.assertEquals("FILE_A sequence number should be 1", 1, entry.sequenceNumber().longValue());
      }

      if (entry.file().path().equals(FILE_B.path())) {
        V2Assert.assertEquals("FILE_b sequence number should be 2", 2, entry.sequenceNumber().longValue());
      }
    }

  }

  @Test
  public void testCommitConflict() {
    AppendFiles appendA = table.newFastAppend();
    appendA.appendFile(FILE_A).apply();

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        .commit();

    table.ops().failCommits(1);

    AssertHelpers.assertThrows("Should reject commit",
        CommitFailedException.class, "Injected failure",
        () -> table.newFastAppend().appendFile(FILE_B).commit());

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "5")
        .commit();

    appendA.commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_C);
    appendFiles.apply();
    table.newFastAppend().appendFile(FILE_D).commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap1, snap2, 2, FILE_D);
    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_D));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    appendFiles.commit();
    Snapshot snap3 = table.currentSnapshot();
    long commitId3 = snap3.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateManifest(manifestFile, seqs(3), ids(commitId3), files(FILE_C));
    validateSnapshot(snap2, snap3, 3, FILE_C);
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testRollBack() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap1, snap2, 2, FILE_B);
    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    table.manageSnapshots().rollbackTo(commitId1).commit();
    Snapshot snap3 = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    table.newFastAppend().appendFile(FILE_C).commit();
    Snapshot snap4 = table.currentSnapshot();
    long commitId4 = snap4.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap3, snap4, 3, FILE_C);
    validateManifest(manifestFile, seqs(3), ids(commitId4), files(FILE_C));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 3, snap4.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testSingleTransaction() {
    Transaction txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_A).commit();
    txn.commitTransaction();
    Snapshot snap = table.currentSnapshot();
    long commitId = snap.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testConcurrentTransaction() {
    Transaction txn1 = table.newTransaction();
    Transaction txn2 = table.newTransaction();
    Transaction txn3 = table.newTransaction();
    Transaction txn4 = table.newTransaction();

    txn1.newFastAppend().appendFile(FILE_A).commit();
    txn3.newOverwrite().addFile(FILE_C).commit();
    txn4.newDelete().deleteFile(FILE_A).commit();
    txn2.newAppend().appendFile(FILE_B).commit();

    txn1.commitTransaction();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile1 = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile1, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    txn2.commitTransaction();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap1, snap2, 2, FILE_B);
    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    txn3.commitTransaction();
    Snapshot snap3 = table.currentSnapshot();
    long commitId3 = snap3.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().stream()
        .filter(manifest -> manifest.snapshotId() == commitId3)
        .collect(Collectors.toList()).get(0);
    validateManifest(manifestFile, seqs(3), ids(commitId3), files(FILE_C));
    validateSnapshot(snap2, snap3, 3, FILE_C);
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());

    txn4.commitTransaction();
    Snapshot snap4 = table.currentSnapshot();
    long commitId4 = snap4.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().stream()
        .filter(manifest -> manifest.snapshotId() == commitId4)
        .collect(Collectors.toList()).get(0);
    validateManifest(manifestFile, seqs(4), ids(commitId4), files(FILE_A), statuses(Status.DELETED));
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, snap4.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 4", 4, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testMultipleOperationsTransaction() {
    Transaction txn = table.newTransaction();
    txn.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = txn.table().currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = snap1.allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());

    Set<DataFile> toAddFiles = new HashSet<>();
    Set<DataFile> toDeleteFiles = new HashSet<>();
    toAddFiles.add(FILE_B);
    toDeleteFiles.add(FILE_A);
    txn.newRewrite().rewriteFiles(toDeleteFiles, toAddFiles).commit();
    txn.commitTransaction();

    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    manifestFile = snap2.allManifests().stream()
        .filter(manifest -> manifest.snapshotId() == commitId2)
        .collect(Collectors.toList()).get(0);

    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testExpirationInTransaction() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(snap1, snap2, 2, FILE_B);
    validateManifest(manifestFile, seqs(2), ids(commitId2), files(FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    Transaction txn = table.newTransaction();
    txn.expireSnapshots().expireSnapshotId(commitId1).commit();
    txn.commitTransaction();
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testTransactionFailure() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = table.currentSnapshot().allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A, FILE_B);
    validateManifest(manifestFile, seqs(1, 1), ids(commitId1, commitId1), files(FILE_A, FILE_B));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        .commit();

    table.ops().failCommits(1);

    Transaction txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_C).commit();

    AssertHelpers.assertThrows("Transaction commit should fail",
        CommitFailedException.class, "Injected failure", txn::commitTransaction);

    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testCherryPicking() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = snap1.allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    Snapshot snap2 = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    // pick the snapshot that's staged but not committed
    Snapshot stagedSnapshot = readMetadata().snapshots().get(1);
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, stagedSnapshot.sequenceNumber());

    // table has new commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    Snapshot snap3 = table.currentSnapshot();
    long commitId3 = snap3.snapshotId();
    manifestFile = snap3.allManifests().get(0);
    validateManifest(manifestFile, seqs(3), ids(commitId3), files(FILE_C));
    validateSnapshot(snap2, snap3, 3, FILE_C);
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(stagedSnapshot.snapshotId()).commit();
    Snapshot snap4 = table.currentSnapshot();
    long commitId4 = snap4.snapshotId();
    manifestFile = table.currentSnapshot().allManifests().get(0);
    validateManifest(manifestFile, seqs(4), ids(commitId4), files(FILE_B));
    validateSnapshot(snap3, snap4, 4, FILE_B);
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, snap4.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 4", 4, readMetadata().lastSequenceNumber());
  }

  @Test
  public void testCherryPickFastForward() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    ManifestFile manifestFile = snap1.allManifests().get(0);
    validateSnapshot(null, snap1, 1, FILE_A);
    validateManifest(manifestFile, seqs(1), ids(commitId1), files(FILE_A));
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();
    Snapshot snap2 = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());

    // pick the snapshot that's staged but not committed
    Snapshot stagedSnapshot = readMetadata().snapshots().get(1);
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, stagedSnapshot.sequenceNumber());

    // cherry-pick snapshot, this will fast forward
    table.manageSnapshots().cherrypick(stagedSnapshot.snapshotId()).commit();
    Snapshot snap3 = table.currentSnapshot();
    long commitId3 = snap3.snapshotId();
    manifestFile = snap3.allManifests().get(0);
    validateManifest(manifestFile, seqs(2), ids(commitId3), files(FILE_B));
    validateSnapshot(snap2, snap3, 2, FILE_B);
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
  }

}
