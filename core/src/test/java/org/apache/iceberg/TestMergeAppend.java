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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.relocated.com.google.common.collect.Iterators.concat;

@RunWith(Parameterized.class)
public class TestMergeAppend extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMergeAppend(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, committedSnapshot.allManifests().size());

    long snapshotId = committedSnapshot.snapshotId();

    validateManifest(committedSnapshot.allManifests().get(0),
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newAppend()
        .appendManifest(manifest)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    Assert.assertEquals("Should create 1 manifest for initial write", 1, committedSnapshot.allManifests().size());

    long snapshotId = committedSnapshot.snapshotId();
    validateManifest(committedSnapshot.allManifests().get(0),
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 2 added files",
        "2", committedSnapshot.summary().get("added-data-files"));
  }

  @Test
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .appendManifest(manifest)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    Assert.assertEquals("Should create 2 manifests for initial write",
        2, committedSnapshot.allManifests().size());

    long snapshotId = committedSnapshot.snapshotId();

    validateManifest(committedSnapshot.allManifests().get(0),
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    validateManifest(committedSnapshot.allManifests().get(1),
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testMergeWithAppendFilesAndManifest() throws IOException {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .appendManifest(manifest)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    long snapshotId = committedSnapshot.snapshotId();

    Assert.assertEquals("Should create 1 merged manifest", 1, committedSnapshot.allManifests().size());

    validateManifest(committedSnapshot.allManifests().get(0),
        seqs(1, 1, 1, 1),
        ids(snapshotId, snapshotId, snapshotId, snapshotId),
        files(FILE_C, FILE_D, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.ADDED, Status.ADDED)
    );
  }

  @Test
  public void testMergeWithExistingManifest() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();
    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    TableMetadata base = readMetadata();
    Snapshot commitBefore = table.currentSnapshot();
    long baseId = commitBefore.snapshotId();
    validateSnapshot(null, commitBefore, 1, FILE_A, FILE_B);

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, commitBefore.allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest,
        seqs(1, 1),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Snapshot committedAfter = table.currentSnapshot();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, committedAfter.allManifests().size());
    ManifestFile newManifest = committedAfter.allManifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long snapshotId = committedAfter.snapshotId();

    validateManifest(newManifest,
        seqs(2, 2, 1, 1),
        ids(snapshotId, snapshotId, baseId, baseId),
        concat(files(FILE_C, FILE_D), files(initialManifest)),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING, Status.EXISTING)
    );
  }

  @Test
  public void testManifestMergeMinCount() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        // Each initial v1/v2 ManifestFile is 5661/6397 bytes respectively. Merging two of the given
        // manifests make one v1/v2 ManifestFile of 5672/6408 bytes respectively, so 15000 bytes
        // limit will give us two bins with three manifest/data files.
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "15000")
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .commit();

    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    Assert.assertEquals("Should contain 2 merged manifest for first write",
        2, readMetadata().currentSnapshot().allManifests().size());
    validateManifest(snap1.allManifests().get(0),
        seqs(1),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));
    validateManifest(snap1.allManifests().get(1),
        seqs(1, 1),
        ids(commitId1, commitId1),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    Assert.assertEquals("Should contain 3 merged manifest for second write",
        3, readMetadata().currentSnapshot().allManifests().size());
    validateManifest(snap2.allManifests().get(0),
        seqs(2),
        ids(commitId2),
        files(FILE_A),
        statuses(Status.ADDED));
    validateManifest(snap2.allManifests().get(1),
        seqs(2, 2),
        ids(commitId2, commitId2),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));
    validateManifest(snap2.allManifests().get(2),
        seqs(1, 1, 1),
        ids(commitId1, commitId1, commitId1),
        files(FILE_A, FILE_C, FILE_D),
        statuses(Status.EXISTING, Status.EXISTING, Status.EXISTING));

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 3 added files",
        "3", readMetadata().currentSnapshot().summary().get("added-data-files"));
  }

  @Test
  public void testManifestsMergeIntoOne() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    TableMetadata base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());
    long commitId1 = snap1.snapshotId();

    Assert.assertEquals("Should contain 1 manifest", 1, snap1.allManifests().size());
    validateManifest(snap1.allManifests().get(0), seqs(1), ids(commitId1), files(FILE_A), statuses(Status.ADDED));

    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    Assert.assertEquals("Should contain 2 manifests", 2, snap2.allManifests().size());
    validateManifest(snap2.allManifests().get(0),
        seqs(2),
        ids(commitId2),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifest(snap2.allManifests().get(1),
        seqs(1),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));

    table.newAppend()
        .appendManifest(writeManifest("input-m0.avro",
            manifestEntry(ManifestEntry.Status.ADDED, null, FILE_C)))
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    Assert.assertEquals("Should contain 3 manifests", 3, snap3.allManifests().size());
    long commitId3 = snap3.snapshotId();
    validateManifest(snap3.allManifests().get(0),
        seqs(3),
        ids(commitId3),
        files(FILE_C),
        statuses(Status.ADDED));
    validateManifest(snap3.allManifests().get(1),
        seqs(2),
        ids(commitId2),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifest(snap3.allManifests().get(2),
        seqs(1),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));

    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    table.newAppend()
        .appendManifest(writeManifest("input-m1.avro",
            manifestEntry(ManifestEntry.Status.ADDED, null, FILE_D)))
        .commit();
    Snapshot snap4 = table.currentSnapshot();

    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, snap4.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 4", 4, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    long commitId4 = snap4.snapshotId();
    Assert.assertEquals("Should only contains 1 merged manifest", 1, snap4.allManifests().size());
    validateManifest(snap4.allManifests().get(0),
        seqs(4, 3, 2, 1),
        ids(commitId4, commitId3, commitId2, commitId1),
        files(FILE_D, FILE_C, FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING, Status.EXISTING, Status.EXISTING));
  }

  @Test
  public void testManifestDoNotMergeMinCount() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .commit();

    Assert.assertNotNull("Should create a snapshot", table.currentSnapshot());
    V1Assert.assertEquals("Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    Snapshot committed = table.currentSnapshot();

    Assert.assertEquals("Should contain 3 merged manifest after 1st write write",
        3, committed.allManifests().size());

    long snapshotId = table.currentSnapshot().snapshotId();

    validateManifest(committed.allManifests().get(0),
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED)
    );
    validateManifest(committed.allManifests().get(1),
        seqs(1),
        ids(snapshotId),
        files(FILE_C),
        statuses(Status.ADDED)
    );
    validateManifest(committed.allManifests().get(2),
        seqs(1),
        ids(snapshotId),
        files(FILE_D),
        statuses(Status.ADDED)
    );

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 4 added files",
        "4", committed.summary().get("added-data-files"));
  }

  @Test
  public void testMergeWithExistingManifestAfterDelete() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot snap = table.currentSnapshot();
    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest,
        seqs(1, 1),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnapshot = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, deleteSnapshot.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    TableMetadata delete = readMetadata();
    long deleteId = delete.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 filtered manifest for delete",
        1, delete.currentSnapshot().allManifests().size());
    ManifestFile deleteManifest = delete.currentSnapshot().allManifests().get(0);

    validateManifest(deleteManifest,
        seqs(2, 1),
        ids(deleteId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, committedSnapshot.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, committedSnapshot.allManifests().size());
    ManifestFile newManifest = committedSnapshot.allManifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long snapshotId = committedSnapshot.snapshotId();

    // the deleted entry from the previous manifest should be removed
    validateManifestEntries(newManifest,
        ids(snapshotId, snapshotId, baseId),
        files(FILE_C, FILE_D, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING));
  }

  @Test
  public void testMinMergeCount() {
    // only merge when there are at least 4 manifests
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot snap1 = table.currentSnapshot();
    long idFileA = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A);

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    Snapshot snap2 = table.currentSnapshot();
    long idFileB = snap2.snapshotId();
    validateSnapshot(snap1, snap2, 2, FILE_B);

    Assert.assertEquals("Should have 2 manifests from setup writes",
        2, readMetadata().currentSnapshot().allManifests().size());

    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    Snapshot snap3 = table.currentSnapshot();
    long idFileC = snap3.snapshotId();
    validateSnapshot(snap2, snap3, 3, FILE_C);

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should have 3 unmerged manifests",
        3, base.currentSnapshot().allManifests().size());
    Set<ManifestFile> unmerged = Sets.newHashSet(base.currentSnapshot().allManifests());

    table.newAppend()
        .appendFile(FILE_D)
        .commit();
    Snapshot committed = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, committed.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 4", 4, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should contain 1 merged manifest after the 4th write",
        1, committed.allManifests().size());
    ManifestFile newManifest = committed.allManifests().get(0);
    Assert.assertFalse("Should not contain previous manifests", unmerged.contains(newManifest));

    long lastSnapshotId = committed.snapshotId();

    validateManifest(newManifest,
        seqs(4, 3, 2, 1),
        ids(lastSnapshotId, idFileC, idFileB, idFileA),
        files(FILE_D, FILE_C, FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING, Status.EXISTING, Status.EXISTING)
    );
  }

  @Test
  public void testMergeSizeTargetWithExistingManifest() {
    // use a small limit on manifest size to prevent merging
    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10")
        .commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot snap = table.currentSnapshot();
    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest,
        seqs(1, 1),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
    Snapshot committed = table.currentSnapshot();

    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, committed.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should contain 2 unmerged manifests after second write",
        2, committed.allManifests().size());
    ManifestFile newManifest = committed.allManifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = committed.snapshotId();
    validateManifest(newManifest,
        seqs(2, 2),
        ids(pendingId, pendingId),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED)
    );

    validateManifest(committed.allManifests().get(1),
        seqs(1, 1),
        ids(baseId, baseId),
        files(initialManifest),
        statuses(Status.ADDED, Status.ADDED)
    );
  }

  @Test
  public void testChangedPartitionSpec() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot snap = table.currentSnapshot();
    long commitId = snap.snapshotId();
    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest,
        seqs(1, 1),
        ids(commitId, commitId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    Snapshot snap2 = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    DataFile newFileY = DataFiles.builder(newSpec)
        .withPath("/path/to/data-y.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .withRecordCount(1)
        .build();

    table.newAppend()
        .appendFile(newFileY)
        .commit();

    Snapshot lastSnapshot = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, lastSnapshot.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should use 2 manifest files",
        2, lastSnapshot.allManifests().size());

    // new manifest comes first
    validateManifest(lastSnapshot.allManifests().get(0),
        seqs(2),
        ids(lastSnapshot.snapshotId()),
        files(newFileY),
        statuses(Status.ADDED)
    );

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, lastSnapshot.allManifests().get(1));
  }

  @Test
  public void testChangedPartitionSpecMergeExisting() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Snapshot snap1 = table.currentSnapshot();
    long id1 = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A);

    // create a second compatible manifest
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Snapshot snap2 = table.currentSnapshot();
    long id2 = snap2.snapshotId();
    validateSnapshot(snap1, snap2, 2, FILE_B);

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should contain 2 manifests",
        2, base.currentSnapshot().allManifests().size());
    ManifestFile manifest = base.currentSnapshot().allManifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    Snapshot snap3 = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    DataFile newFileY = DataFiles.builder(table.spec())
        .withPath("/path/to/data-y.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .withRecordCount(1)
        .build();

    table.newAppend()
        .appendFile(newFileY)
        .commit();
    Snapshot lastSnapshot = table.currentSnapshot();
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, lastSnapshot.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should use 2 manifest files",
        2, lastSnapshot.allManifests().size());
    Assert.assertFalse("First manifest should not be in the new snapshot",
        lastSnapshot.allManifests().contains(manifest));

    validateManifest(lastSnapshot.allManifests().get(0),
        seqs(3),
        ids(lastSnapshot.snapshotId()),
        files(newFileY),
        statuses(Status.ADDED)
    );
    validateManifest(lastSnapshot.allManifests().get(1),
        seqs(2, 1),
        ids(id2, id1),
        files(FILE_B, FILE_A),
        statuses(Status.EXISTING, Status.EXISTING)
    );
  }

  @Test
  public void testFailure() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();
    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest, seqs(1), ids(baseId), files(FILE_A), statuses(Status.ADDED));

    table.ops().failCommits(5);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.allManifests().size());
    ManifestFile newManifest = pending.allManifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Should only contain 1 manifest file",
        1, table.currentSnapshot().allManifests().size());

    validateManifest(table.currentSnapshot().allManifests().get(0),
        seqs(1),
        ids(baseId),
        files(initialManifest),
        statuses(Status.ADDED)
    );

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AppendFiles append = table.newAppend().appendManifest(manifest);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.allManifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);
    V2Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testRecovery() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest, seqs(1), ids(baseId), files(FILE_A), statuses(Status.ADDED));

    table.ops().failCommits(3);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.allManifests().size());
    ManifestFile newManifest = pending.allManifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, table.currentSnapshot().sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    append.commit();
    Snapshot snapshot = table.currentSnapshot();
    long snapshotId = snapshot.snapshotId();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, table.currentSnapshot().sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should reuse the new manifest", new File(newManifest.path()).exists());
    Assert.assertEquals("Should commit the same new manifest during retry",
        Lists.newArrayList(newManifest), metadata.currentSnapshot().allManifests());

    Assert.assertEquals("Should only contain 1 merged manifest file",
        1, table.currentSnapshot().allManifests().size());
    ManifestFile manifestFile = snapshot.allManifests().get(0);
    validateManifest(manifestFile,
        seqs(2, 1),
        ids(snapshotId, baseId),
        files(FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING));
  }

  @Test
  public void testAppendManifestWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newAppend()
        .appendManifest(manifest)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    long snapshotId = snapshot.snapshotId();
    validateSnapshot(null, snapshot, 1, FILE_A, FILE_B);

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 committed manifest", 1, manifests.size());
    ManifestFile manifestFile = snapshot.allManifests().get(0);
    validateManifest(manifestFile,
        seqs(1, 1),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 2 added files",
        "2", snapshot.summary().get("added-data-files"));
    Assert.assertEquals("Summary metadata should include 2 added records",
        "2", snapshot.summary().get("added-records"));
    Assert.assertEquals("Summary metadata should include 2 files in total",
        "2", snapshot.summary().get("total-data-files"));
    Assert.assertEquals("Summary metadata should include 2 records in total",
        "2", snapshot.summary().get("total-records"));
  }

  @Test
  public void testMergedAppendManifestCleanupWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    ManifestFile manifest1 = writeManifestWithName("manifest-file-1.avro", FILE_A, FILE_B);
    table.newAppend()
        .appendManifest(manifest1)
        .commit();

    Snapshot snap1 = table.currentSnapshot();
    long commitId1 = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A, FILE_B);

    Assert.assertEquals("Should have only 1 manifest", 1, snap1.allManifests().size());
    validateManifest(table.currentSnapshot().allManifests().get(0),
        seqs(1, 1),
        ids(commitId1, commitId1),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
    Assert.assertTrue("Unmerged append manifest should not be deleted", new File(manifest1.path()).exists());

    ManifestFile manifest2 = writeManifestWithName("manifest-file-2.avro", FILE_C, FILE_D);
    table.newAppend()
        .appendManifest(manifest2)
        .commit();

    Snapshot snap2 = table.currentSnapshot();
    long commitId2 = snap2.snapshotId();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, table.currentSnapshot().sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Manifests should be merged into 1", 1, snap2.allManifests().size());
    validateManifest(table.currentSnapshot().allManifests().get(0),
        seqs(2, 2, 1, 1),
        ids(commitId2, commitId2, commitId1, commitId1),
        files(FILE_C, FILE_D, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING, Status.EXISTING));

    Assert.assertFalse("Merged append manifest should be deleted", new File(manifest2.path()).exists());
  }

  @Test
  public void testAppendManifestFailureWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .commit();

    table.ops().failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    AppendFiles append = table.newAppend();
    append.appendManifest(manifest);

    AssertHelpers.assertThrows("Should reject commit",
        CommitFailedException.class, "Injected failure",
        append::commit);

    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    Assert.assertTrue("Append manifest should not be deleted", new File(manifest.path()).exists());
  }

  @Test
  public void testInvalidAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifestWithExistingFiles = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(Status.EXISTING, null, FILE_A));
    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest with existing files",
        () -> table.newAppend()
            .appendManifest(manifestWithExistingFiles)
            .commit());
    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());

    ManifestFile manifestWithDeletedFiles = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(Status.DELETED, null, FILE_A));
    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest with deleted files",
        () -> table.newAppend()
            .appendManifest(manifestWithDeletedFiles)
            .commit());
    Assert.assertEquals("Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
  }


  @Test
  public void testUpdatePartitionSpecFieldIdsForV1Table() {
    TableMetadata base = readMetadata();

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("id", 16)
        .identity("data")
        .bucket("data", 4)
        .bucket("data", 16, "data_partition") // reuse field id although different target name
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    List<PartitionSpec> partitionSpecs = table.ops().current().specs();
    PartitionSpec partitionSpec = partitionSpecs.get(0);
    Assert.assertEquals(1000, partitionSpec.lastAssignedFieldId());

    Types.StructType structType = partitionSpec.partitionType();
    List<Types.NestedField> fields = structType.fields();
    Assert.assertEquals(1, fields.size());
    Assert.assertEquals("data_bucket", fields.get(0).name());
    Assert.assertEquals(1000, fields.get(0).fieldId());

    partitionSpec = partitionSpecs.get(1);
    Assert.assertEquals(1003, partitionSpec.lastAssignedFieldId());

    structType = partitionSpec.partitionType();
    fields = structType.fields();
    Assert.assertEquals(4, fields.size());
    Assert.assertEquals("id_bucket", fields.get(0).name());
    Assert.assertEquals(1000, fields.get(0).fieldId());
    Assert.assertEquals("data", fields.get(1).name());
    Assert.assertEquals(1001, fields.get(1).fieldId());
    Assert.assertEquals("data_bucket", fields.get(2).name());
    Assert.assertEquals(1002, fields.get(2).fieldId());
    Assert.assertEquals("data_partition", fields.get(3).name());
    Assert.assertEquals(1003, fields.get(3).fieldId());
  }

  @Test
  public void testManifestEntryFieldIdsForChangedPartitionSpecForV1Table() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Snapshot snap = table.currentSnapshot();
    long commitId = snap.snapshotId();
    validateSnapshot(null, snap, 1, FILE_A);
    TableMetadata base = readMetadata();

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);
    validateManifest(initialManifest, seqs(1), ids(commitId), files(FILE_A), statuses(Status.ADDED));

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("id", 8)
        .bucket("data", 8)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    // create a new with the table's current spec
    DataFile newFile = DataFiles.builder(table.spec())
        .withPath("/path/to/data-x.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("id_bucket=1/data_bucket=1")
        .withRecordCount(1)
        .build();

    table.newAppend()
        .appendFile(newFile)
        .commit();
    Snapshot committedSnapshot = table.currentSnapshot();

    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, committedSnapshot.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Assert.assertEquals("Should use 2 manifest files",
        2, committedSnapshot.allManifests().size());

    // new manifest comes first
    validateManifest(committedSnapshot.allManifests().get(0),
        seqs(2),
        ids(committedSnapshot.snapshotId()), files(newFile),
        statuses(Status.ADDED)
    );

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, committedSnapshot.allManifests().get(1));

    // field ids of manifest entries in two manifests with different specs of the same source field should be different
    ManifestEntry<DataFile> entry = ManifestFiles.read(committedSnapshot.allManifests().get(0), FILE_IO,
        table.location(), table.ops().current().shouldUseRelativePaths())
        .entries().iterator().next();
    Types.NestedField field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    Assert.assertEquals(1000, field.fieldId());
    Assert.assertEquals("id_bucket", field.name());
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(1);
    Assert.assertEquals(1001, field.fieldId());
    Assert.assertEquals("data_bucket", field.name());

    entry = ManifestFiles.read(committedSnapshot.allManifests().get(1), FILE_IO,
        table.location(), table.ops().current().shouldUseRelativePaths()).entries().iterator().next();
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    Assert.assertEquals(1000, field.fieldId());
    Assert.assertEquals("data_bucket", field.name());
  }

  @Test
  public void testDefaultPartitionSummaries() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Set<String> partitionSummaryKeys = table.currentSnapshot().summary().keySet().stream()
        .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
        .collect(Collectors.toSet());
    Assert.assertEquals("Should include no partition summaries by default", 0, partitionSummaryKeys.size());

    String summariesIncluded = table.currentSnapshot().summary()
        .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    Assert.assertEquals("Should not set partition-summaries-included to true", "false", summariesIncluded);

    String changedPartitions = table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    Assert.assertEquals("Should set changed partition count", "1", changedPartitions);
  }

  @Test
  public void testIncludedPartitionSummaries() {
    table.updateProperties()
        .set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "1")
        .commit();

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Set<String> partitionSummaryKeys = table.currentSnapshot().summary().keySet().stream()
        .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
        .collect(Collectors.toSet());
    Assert.assertEquals("Should include a partition summary", 1, partitionSummaryKeys.size());

    String summariesIncluded = table.currentSnapshot().summary()
        .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    Assert.assertEquals("Should set partition-summaries-included to true", "true", summariesIncluded);

    String changedPartitions = table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    Assert.assertEquals("Should set changed partition count", "1", changedPartitions);

    String partitionSummary = table.currentSnapshot().summary()
        .get(SnapshotSummary.CHANGED_PARTITION_PREFIX + "data_bucket=0");
    Assert.assertEquals("Summary should include 1 file with 1 record that is 10 bytes",
        "added-data-files=1,added-records=1,added-files-size=10", partitionSummary);
  }

  @Test
  public void testIncludedPartitionSummaryLimit() {
    table.updateProperties()
        .set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "1")
        .commit();

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Set<String> partitionSummaryKeys = table.currentSnapshot().summary().keySet().stream()
        .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
        .collect(Collectors.toSet());
    Assert.assertEquals("Should include no partition summaries, over limit", 0, partitionSummaryKeys.size());

    String summariesIncluded = table.currentSnapshot().summary()
        .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    Assert.assertEquals("Should not set partition-summaries-included to true", "false", summariesIncluded);

    String changedPartitions = table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    Assert.assertEquals("Should set changed partition count", "2", changedPartitions);
  }
}
