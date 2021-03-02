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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergFilesCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

@RunWith(Parameterized.class)
public class TestIcebergFilesCommitter extends TableTestBase {
  private static final Configuration CONF = new Configuration();

  private String tablePath;
  private File flinkManifestFolder;

  private final FileFormat format;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2},
        new Object[] {"orc", 1},
    };
  }

  public TestIcebergFilesCommitter(String format, int formatVersion) {
    super(formatVersion);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    flinkManifestFolder = temp.newFolder();

    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    // Construct the iceberg table.
    table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(DEFAULT_FILE_FORMAT, format.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
        .commit();
  }

  @Test
  public void testCommitTxnWithoutDataFiles() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      SimpleDataUtil.assertTableRows(table, Lists.newArrayList());
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // It's better to advance the max-committed-checkpoint-id in iceberg snapshot, so that the future flink job
      // failover won't fail.
      for (int i = 1; i <= 3; i++) {
        harness.snapshot(++checkpointId, ++timestamp);
        assertFlinkManifests(0);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(0);

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobId, checkpointId);
      }
    }
  }

  @Test
  public void testMaxContinuousEmptyCommits() throws Exception {
    table.updateProperties()
        .set(MAX_CONTINUOUS_EMPTY_COMMITS, "3")
        .commit();

    JobID jobId = new JobID();
    long checkpointId = 0;
    long timestamp = 0;
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);

      for (int i = 1; i <= 9; i++) {
        harness.snapshot(++checkpointId, ++timestamp);
        harness.notifyOfCompletedCheckpoint(checkpointId);

        assertSnapshotSize(i / 3);
      }
    }
  }

  private WriteResult of(DataFile dataFile) {
    return WriteResult.builder().addDataFiles(dataFile).build();
  }

  @Test
  public void testCommitTxn() throws Exception {
    // Test with 3 continues checkpoints:
    //   1. snapshotState for checkpoint#1
    //   2. notifyCheckpointComplete for checkpoint#1
    //   3. snapshotState for checkpoint#2
    //   4. notifyCheckpointComplete for checkpoint#2
    //   5. snapshotState for checkpoint#3
    //   6. notifyCheckpointComplete for checkpoint#3
    long timestamp = 0;

    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobID)) {
      harness.setup();
      harness.open();
      assertSnapshotSize(0);

      List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
      for (int i = 1; i <= 3; i++) {
        RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
        DataFile dataFile = writeDataFile("data-" + i, ImmutableList.of(rowData));
        harness.processElement(of(dataFile), ++timestamp);
        rows.add(rowData);

        harness.snapshot(i, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(i);
        assertFlinkManifests(0);

        SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(rows));
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobID, i);
      }
    }
  }

  @Test
  public void testOrderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#1;
    //   4. notifyCheckpointComplete for checkpoint#2;
    long timestamp = 0;

    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      harness.processElement(of(dataFile2), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1));
      assertMaxCommittedCheckpointId(jobId, firstCheckpointId);
      assertFlinkManifests(1);

      // 4. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testDisorderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that the two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#2;
    //   4. notifyCheckpointComplete for checkpoint#1;
    long timestamp = 0;

    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      harness.processElement(of(dataFile2), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);

      // 4. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testRecoveryFromValidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> expectedRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row));

      harness.processElement(of(dataFile1), ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row));
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, checkpointId);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      harness.processElement(of(dataFile), ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithoutCompletedNotification() throws Exception {
    // We've two steps in checkpoint: 1. snapshotState(ckp); 2. notifyCheckpointComplete(ckp). It's possible that we
    // flink job will restore from a checkpoint with only step#1 finished.
    long checkpointId = 0;
    long timestamp = 0;
    OperatorSubtaskState snapshot;
    List<RowData> expectedRows = Lists.newArrayList();
    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-1", ImmutableList.of(row));
      harness.processElement(of(dataFile), ++timestamp);

      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of());
      assertMaxCommittedCheckpointId(jobId, -1L);
      assertFlinkManifests(1);
    }

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg transaction.
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertMaxCommittedCheckpointId(jobId, checkpointId);

      harness.snapshot(++checkpointId, ++timestamp);
      // Did not write any new record, so it won't generate new manifest.
      assertFlinkManifests(0);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, checkpointId);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      harness.processElement(of(dataFile), ++timestamp);

      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);
    }

    // Redeploying flink job from external checkpoint.
    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg transaction.
      assertFlinkManifests(0);

      assertMaxCommittedCheckpointId(newJobId, -1);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertSnapshotSize(3);

      RowData row = SimpleDataUtil.createRowData(3, "foo");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-3", ImmutableList.of(row));
      harness.processElement(of(dataFile), ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newJobId, checkpointId);
    }
  }

  @Test
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    JobID oldJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(oldJobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(oldJobId, -1L);

      for (int i = 1; i <= 3; i++) {
        rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(of(dataFile), ++timestamp);
        harness.snapshot(++checkpointId, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(0);

        SimpleDataUtil.assertTableRows(table, tableRows);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(oldJobId, checkpointId);
      }
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 0;
    timestamp = 0;
    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(newJobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(oldJobId, 3);
      assertMaxCommittedCheckpointId(newJobId, -1);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);

      DataFile dataFile = writeDataFile("data-new-1", rows);
      harness.processElement(of(dataFile), ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, tableRows);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newJobId, checkpointId);
    }
  }

  @Test
  public void testMultipleJobsWriteSameTable() throws Exception {
    long timestamp = 0;
    List<RowData> tableRows = Lists.newArrayList();

    JobID[] jobs = new JobID[] {new JobID(), new JobID(), new JobID()};
    for (int i = 0; i < 20; i++) {
      int jobIndex = i % 3;
      int checkpointId = i / 3;
      JobID jobId = jobs[jobIndex];
      try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
        harness.setup();
        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobId, checkpointId == 0 ? -1 : checkpointId);

        List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(of(dataFile), ++timestamp);
        harness.snapshot(checkpointId + 1, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId + 1);
        assertFlinkManifests(0);
        SimpleDataUtil.assertTableRows(table, tableRows);
        assertSnapshotSize(i + 1);
        assertMaxCommittedCheckpointId(jobId, checkpointId + 1);
      }
    }
  }

  @Test
  public void testBoundedStream() throws Exception {
    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertFlinkManifests(0);
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, -1L);

      List<RowData> tableRows = Lists.newArrayList(SimpleDataUtil.createRowData(1, "word-1"));

      DataFile dataFile = writeDataFile("data-1", tableRows);
      harness.processElement(of(dataFile), 1);
      ((BoundedOneInput) harness.getOneInputOperator()).endInput();

      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, Long.MAX_VALUE);
    }
  }

  @Test
  public void testFlinkManifests() throws Exception {
    long timestamp = 0;
    final long checkpoint = 10;

    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      Assert.assertEquals("File name should have the expected pattern.",
          String.format("%s-%05d-%d-%d-%05d.avro", jobId, 0, 0, checkpoint, 1), manifestPath.getFileName().toString());

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles = FlinkManifestUtil.readDataFiles(createTestingManifestFile(manifestPath), table.io());
      Assert.assertEquals(1, dataFiles.size());
      TestHelpers.assertEquals(dataFile1, dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1));
      assertMaxCommittedCheckpointId(jobId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testDeleteFiles() throws Exception {
    Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createInsert(1, "aaa");
      DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(row1));
      harness.processElement(of(dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      Assert.assertEquals("File name should have the expected pattern.",
          String.format("%s-%05d-%d-%d-%05d.avro", jobId, 0, 0, checkpoint, 1), manifestPath.getFileName().toString());

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles = FlinkManifestUtil.readDataFiles(createTestingManifestFile(manifestPath), table.io());
      Assert.assertEquals(1, dataFiles.size());
      TestHelpers.assertEquals(dataFile1, dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1));
      assertMaxCommittedCheckpointId(jobId, checkpoint);
      assertFlinkManifests(0);

      // 4. process both data files and delete files.
      RowData row2 = SimpleDataUtil.createInsert(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(row2));

      RowData delete1 = SimpleDataUtil.createDelete(1, "aaa");
      DeleteFile deleteFile1 = writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete1));
      harness.processElement(WriteResult.builder()
              .addDataFiles(dataFile2)
              .addDeleteFiles(deleteFile1)
              .build(),
          ++timestamp);
      assertMaxCommittedCheckpointId(jobId, checkpoint);

      // 5. snapshotState for checkpoint#2
      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(2);

      // 6. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row2));
      assertMaxCommittedCheckpointId(jobId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testValidateDataFileExist() throws Exception {
    Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);
    long timestamp = 0;
    long checkpoint = 10;
    JobID jobId = new JobID();
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    RowData insert1 = SimpleDataUtil.createInsert(1, "aaa");
    DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(insert1));

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      // Txn#1: insert the row <1, 'aaa'>
      harness.processElement(WriteResult.builder()
              .addDataFiles(dataFile1)
              .build(),
          ++timestamp);
      harness.snapshot(checkpoint, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);

      // Txn#2: Overwrite the committed data-file-1
      RowData insert2 = SimpleDataUtil.createInsert(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(insert2));
      new TestTableLoader(tablePath)
          .loadTable()
          .newOverwrite()
          .addFile(dataFile2)
          .deleteFile(dataFile1)
          .commit();
    }

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      // Txn#3: position-delete the <1, 'aaa'> (NOT committed).
      DeleteFile deleteFile1 = writePosDeleteFile(appenderFactory,
          "pos-delete-file-1",
          ImmutableList.of(Pair.of(dataFile1.path(), 0L)));
      harness.processElement(WriteResult.builder()
              .addDeleteFiles(deleteFile1)
              .addReferencedDataFiles(dataFile1.path())
              .build(),
          ++timestamp);
      harness.snapshot(++checkpoint, ++timestamp);

      // Txn#3: validate will be failure when committing.
      final long currentCheckpointId = checkpoint;
      AssertHelpers.assertThrows("Validation should be failure because of non-exist data files.",
          ValidationException.class, "Cannot commit, missing data files",
          () -> {
            harness.notifyOfCompletedCheckpoint(currentCheckpointId);
            return null;
          });
    }
  }

  @Test
  public void testCommitTwoCheckpointsInSingleTxn() throws Exception {
    Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData insert1 = SimpleDataUtil.createInsert(1, "aaa");
      RowData insert2 = SimpleDataUtil.createInsert(2, "bbb");
      RowData delete3 = SimpleDataUtil.createDelete(3, "ccc");
      DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(insert1, insert2));
      DeleteFile deleteFile1 = writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete3));
      harness.processElement(WriteResult.builder()
              .addDataFiles(dataFile1)
              .addDeleteFiles(deleteFile1)
              .build(),
          ++timestamp);

      // The 1th snapshotState.
      harness.snapshot(checkpoint, ++timestamp);

      RowData insert4 = SimpleDataUtil.createInsert(4, "ddd");
      RowData delete2 = SimpleDataUtil.createDelete(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(insert4));
      DeleteFile deleteFile2 = writeEqDeleteFile(appenderFactory, "delete-file-2", ImmutableList.of(delete2));
      harness.processElement(WriteResult.builder()
              .addDataFiles(dataFile2)
              .addDeleteFiles(deleteFile2)
              .build(),
          ++timestamp);

      // The 2nd snapshotState.
      harness.snapshot(++checkpoint, ++timestamp);

      // Notify the 2nd snapshot to complete.
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(insert1, insert4));
      assertMaxCommittedCheckpointId(jobId, checkpoint);
      assertFlinkManifests(0);
      Assert.assertEquals("Should have committed 2 txn.", 2, ImmutableList.copyOf(table.snapshots()).size());
    }
  }

  private DeleteFile writeEqDeleteFile(FileAppenderFactory<RowData> appenderFactory,
                                       String filename, List<RowData> deletes) throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, FileFormat.PARQUET, tablePath, filename, appenderFactory, deletes);
  }

  private DeleteFile writePosDeleteFile(FileAppenderFactory<RowData> appenderFactory,
                                        String filename,
                                        List<Pair<CharSequence, Long>> positions) throws IOException {
    return SimpleDataUtil.writePosDeleteFile(table, FileFormat.PARQUET, tablePath, filename, appenderFactory,
        positions);
  }

  private FileAppenderFactory<RowData> createDeletableAppenderFactory() {
    int[] equalityFieldIds = new int[] {
        table.schema().findField("id").fieldId(),
        table.schema().findField("data").fieldId()
    };
    return new FlinkAppenderFactory(table.schema(),
        FlinkSchemaUtil.convert(table.schema()), table.properties(), table.spec(), table.sortOrder(), equalityFieldIds,
        table.schema(), null);
  }

  private ManifestFile createTestingManifestFile(Path manifestPath) {
    return new GenericManifestFile(manifestPath.toAbsolutePath().toString(), manifestPath.toFile().length(), 0,
        ManifestContent.DATA, 0, 0, 0L, 0, 0, 0, 0, 0, 0, null, null);
  }

  private List<Path> assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests = Files.list(flinkManifestFolder.toPath())
        .filter(p -> !p.toString().endsWith(".crc"))
        .collect(Collectors.toList());
    Assert.assertEquals(String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount, manifests.size());
    return manifests;
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(),
        table.spec(), table.sortOrder(), CONF, tablePath, format.addExtension(filename), rows);
  }

  private void assertMaxCommittedCheckpointId(JobID jobID, long expectedId) {
    table.refresh();
    long actualId = IcebergFilesCommitter.getMaxCommittedCheckpointId(table, jobID.toString());
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private OneInputStreamOperatorTestHarness<WriteResult, Void> createStreamSink(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(tablePath);
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<WriteResult, Void> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> param) {
      IcebergFilesCommitter committer = new IcebergFilesCommitter(new TestTableLoader(tablePath), false);
      committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) committer;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergFilesCommitter.class;
    }
  }
}
