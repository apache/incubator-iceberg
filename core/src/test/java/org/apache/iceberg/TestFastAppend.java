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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFastAppend extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestFastAppend(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Table should start with last-sequence-number 0", 0, base.lastSequenceNumber());

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot snap = table.currentSnapshot();

    validateSnapshot(base.currentSnapshot(), snap, 1, FILE_A, FILE_B);

    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());
  }

  @Test
  public void testEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Table should start with last-sequence-number 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newFastAppend()
        .appendManifest(manifest)
        .commit();

    Snapshot snap = table.currentSnapshot();

    validateSnapshot(base.currentSnapshot(), snap, 1, FILE_A, FILE_B);

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 2 added files",
        "2", snap.summary().get("added-data-files"));

    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());
  }

  @Test
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Table should start with last-sequence-number 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .appendManifest(manifest)
        .commit();

    Snapshot snap = table.currentSnapshot();

    long commitId = snap.snapshotId();

    validateManifest(snap.allManifests().get(0),
        seqs(1, 1),
        ids(commitId, commitId),
        files(FILE_C, FILE_D));
    validateManifest(snap.allManifests().get(1),
        seqs(1, 1),
        ids(commitId, commitId),
        files(FILE_A, FILE_B));

    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());

    V1Assert.assertEquals("Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());
  }

  @Test
  public void testNonEmptyTableAppend() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().allManifests();
    Assert.assertEquals("Should have one existing manifest", 1, v2manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertNotEquals("Snapshots should have unique IDs",
        base.currentSnapshot().snapshotId(), pending.snapshotId());
    validateSnapshot(base.currentSnapshot(), pending, FILE_C, FILE_D);
  }

  @Test
  public void testNoMerge() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v3manifests = base.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 existing manifests", 2, v3manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Set<Long> ids = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      ids.add(snapshot.snapshotId());
    }
    ids.add(pending.snapshotId());
    Assert.assertEquals("Snapshots should have 3 unique IDs", 3, ids.size());

    validateSnapshot(base.currentSnapshot(), pending, FILE_C, FILE_D);
  }

  @Test
  public void testRefreshBeforeApply() {
    // load a new copy of the table that will not be refreshed by the commit
    Table stale = load();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    // commit from the stale table
    AppendFiles append = stale.newFastAppend()
        .appendFile(FILE_D);
    Snapshot pending = append.apply();

    // table should have been refreshed before applying the changes
    validateSnapshot(base.currentSnapshot(), pending, FILE_D);
  }

  @Test
  public void testRefreshBeforeCommit() {
    // commit from the stale table
    AppendFiles append = table.newFastAppend()
        .appendFile(FILE_D);
    Snapshot pending = append.apply();

    validateSnapshot(null, pending, FILE_D);

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    append.commit();

    TableMetadata committed = readMetadata();

    // apply was called before the conflicting commit, but the commit was still consistent
    validateSnapshot(base.currentSnapshot(), committed.currentSnapshot(), FILE_D);

    List<ManifestFile> committedManifests = Lists.newArrayList(committed.currentSnapshot().allManifests());
    committedManifests.removeAll(base.currentSnapshot().allManifests());
    Assert.assertEquals("Should reused manifest created by apply",
        pending.allManifests().get(0), committedManifests.get(0));
  }

  @Test
  public void testFailure() {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.allManifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AppendFiles append = table.newFastAppend().appendManifest(manifest);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.allManifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testRecoveryWithManifestList() {
    table.updateProperties().set(TableProperties.MANIFEST_LISTS_ENABLED, "true").commit();

    // inject 3 failures, the last try will succeed
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3);

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.allManifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    append.commit();

    TableMetadata metadata = readMetadata();

    validateSnapshot(null, metadata.currentSnapshot(), FILE_B);
    Assert.assertTrue("Should commit same new manifest", new File(newManifest.path()).exists());
    Assert.assertTrue("Should commit the same new manifest",
        metadata.currentSnapshot().allManifests().contains(newManifest));
  }

  @Test
  public void testRecoveryWithoutManifestList() {
    table.updateProperties().set(TableProperties.MANIFEST_LISTS_ENABLED, "false").commit();

    // inject 3 failures, the last try will succeed
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3);

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.allManifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    append.commit();

    TableMetadata metadata = readMetadata();

    validateSnapshot(null, metadata.currentSnapshot(), FILE_B);
    Assert.assertTrue("Should commit same new manifest", new File(newManifest.path()).exists());
    Assert.assertTrue("Should commit the same new manifest",
        metadata.currentSnapshot().allManifests().contains(newManifest));
  }

  @Test
  public void testAppendManifestWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newFastAppend()
        .appendManifest(manifest)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 committed manifest", 1, manifests.size());

    validateManifestEntries(manifests.get(0),
        ids(snapshot.snapshotId(), snapshot.snapshotId()),
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
  public void testAppendManifestFailureWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

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
        () -> table.newFastAppend()
            .appendManifest(manifestWithExistingFiles)
            .commit());

    ManifestFile manifestWithDeletedFiles = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(Status.DELETED, null, FILE_A));
    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest with deleted files",
        () -> table.newFastAppend()
            .appendManifest(manifestWithDeletedFiles)
            .commit());
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
