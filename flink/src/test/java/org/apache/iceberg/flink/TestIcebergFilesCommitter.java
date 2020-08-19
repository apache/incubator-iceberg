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

package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergFilesCommitter {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String warehouse;
  private String tablePath;
  private Table table;

  private final FileFormat format;

  // TODO Add ORC/Parquet unit test once those readers and writers are ready.
  @Parameterized.Parameters(name = "format = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro"},
        new Object[] {"avro"}
    };
  }

  public TestIcebergFilesCommitter(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse + "/test";
    Assert.assertTrue("Should create the table directory correctly.", new File(tablePath).mkdir());

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), false);
  }

  @Test
  public void testCommitTxnWithoutDataFiles() throws Exception {
    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      SimpleDataUtil.assertTableRows(tablePath, Lists.newArrayList());
      assertSnapshotSize(0);
    }
  }

  @Test
  public void testCommitTxn() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = writeDataFile("data-1", rows);

      harness.processElement(dataFile1, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);
      DataFile dataFile2 = writeDataFile("data-2", rows);

      harness.processElement(dataFile2, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      rows.add(SimpleDataUtil.createRowData(3, "foo"));
      tableRows.addAll(rows);
      DataFile dataFile3 = writeDataFile("data-3", rows);

      harness.processElement(dataFile3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId - 1);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithoutNotifying() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = writeDataFile("data-1", rows);

      harness.processElement(dataFile1, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(tablePath, Lists.newArrayList());
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      rows.add(SimpleDataUtil.createRowData(2, "foo"));
      tableRows.addAll(rows);
      DataFile dataFile2 = writeDataFile("data-2", rows);
      harness.processElement(dataFile2, ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      rows.add(SimpleDataUtil.createRowData(3, "bar"));
      tableRows.addAll(rows);
      DataFile dataFile3 = writeDataFile("data-3", rows);

      harness.processElement(dataFile3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithNotifying() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = writeDataFile("data-1", rows);

      harness.processElement(dataFile1, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);
      DataFile dataFile2 = writeDataFile("data-2", rows);
      harness.processElement(dataFile2, ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);
    }
  }

  @Test
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    String oldFilesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(oldFilesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(oldFilesCommitterUid, -1L);

      for (int i = 1; i <= 3; i++) {
        rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(dataFile, ++timestamp);
        harness.snapshot(++checkpointId, ++timestamp);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        SimpleDataUtil.assertTableRows(tablePath, tableRows);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(oldFilesCommitterUid, checkpointId);
      }
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 0;
    timestamp = 0;
    String newFilesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(newFilesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(oldFilesCommitterUid, 3);
      assertMaxCommittedCheckpointId(newFilesCommitterUid, -1);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);

      DataFile dataFile = writeDataFile("data-new-1", rows);
      harness.processElement(dataFile, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newFilesCommitterUid, checkpointId);
    }
  }

  @Test
  public void testMultipleJobsWriteSameTable() throws Exception {
    long timestamp = 0;
    List<RowData> tableRows = Lists.newArrayList();

    for (int i = 0; i < 20; i++) {
      int jobId = i % 3;
      int checkpointId = i / 3;
      String jobUid = String.format("job-%d", jobId);
      try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink(jobUid)) {
        harness.setup();
        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobUid, checkpointId == 0 ? -1 : checkpointId);

        List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(dataFile, ++timestamp);
        harness.snapshot(checkpointId + 1, ++timestamp);

        harness.notifyOfCompletedCheckpoint(checkpointId + 1);
        SimpleDataUtil.assertTableRows(tablePath, tableRows);
        assertSnapshotSize(i + 1);
        assertMaxCommittedCheckpointId(jobUid, checkpointId + 1);
      }
    }
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), CONF, tablePath, format.addExtension(filename), rows);
  }

  private void assertMaxCommittedCheckpointId(String filesCommitterUid, long expectedId) {
    table.refresh();
    long actualId = IcebergFilesCommitter.getMaxCommittedCheckpointId(table, filesCommitterUid);
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private OneInputStreamOperatorTestHarness<DataFile, Object> createStreamSink(String filesCommitterUid)
      throws Exception {
    Map<String, String> options = ImmutableMap.of(
        "type", "iceberg",
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop",
        FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION, warehouse
    );

    IcebergFilesCommitter committer = new IcebergFilesCommitter(filesCommitterUid, "test", options, CONF);
    return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(committer), 1, 1, 0);
  }
}
