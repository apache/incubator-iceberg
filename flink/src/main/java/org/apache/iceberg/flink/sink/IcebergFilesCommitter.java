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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<DataFile, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, Byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The data files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // 'dataFilesPerCheckpoint'.
  private final List<DataFile> dataFilesOfCurrentCheckpoint = Lists.newArrayList();

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient Table table;
  private transient FlinkManifest.FlinkManifestFactory flinkManifestFactory;
  private transient long maxCommittedCheckpointId;

  // There're two cases that we restore from flink checkpoints: the first case is restoring from snapshot created by the
  // same flink job; another case is restoring from snapshot created by another different job. For the second case, we
  // need to maintain the old flink job's id in flink state backend to find the max-committed-checkpoint-id when
  // traversing iceberg table's snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;
  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, Byte[]>> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<SortedMap<Long, Byte[]>> checkpointsState;

  IcebergFilesCommitter(TableLoader tableLoader, boolean replacePartitions) {
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    this.flinkManifestFactory = FlinkManifest.createFactory(table, flinkJobId, subTaskId, attemptId);
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    if (context.isRestored()) {
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new flink job even if
      // it's restored from a snapshot created by another different flink job, so it's safe to assign the max committed
      // checkpoint id from restored flink job to the current flink job.
      this.maxCommittedCheckpointId = getMaxCommittedCheckpointId(table, restoredFlinkJobId);

      NavigableMap<Long, Byte[]> uncommittedDataFiles = Maps
          .newTreeMap(checkpointsState.get().iterator().next())
          .tailMap(maxCommittedCheckpointId, false);
      if (!uncommittedDataFiles.isEmpty()) {
        // Committed all uncommitted data files from the old flink job to iceberg table.
        long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
        commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, maxUncommittedCheckpointId);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", table, checkpointId);

    // Update the checkpoint state.
    dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId));

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    // Clear the local buffer for current checkpoint.
    dataFilesOfCurrentCheckpoint.clear();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    // It's possible that we have the following events:
    //   1. snapshotState(ckpId);
    //   2. snapshotState(ckpId+1);
    //   3. notifyCheckpointComplete(ckpId+1);
    //   4. notifyCheckpointComplete(ckpId);
    // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all the files,
    // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
    if (checkpointId > maxCommittedCheckpointId) {
      commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, checkpointId);
      this.maxCommittedCheckpointId = checkpointId;
    }
  }

  private void commitUpToCheckpoint(NavigableMap<Long, Byte[]> manifestsMap,
                                    String newFlinkJobId,
                                    long checkpointId) throws IOException {
    NavigableMap<Long, Byte[]> pendingManifestMap = manifestsMap.headMap(checkpointId, true);

    List<ManifestFile> manifestFiles = Lists.newArrayList();
    for (Byte[] manifestData : pendingManifestMap.values()) {
      ManifestFile manifestFile = ManifestFiles.decode(ArrayUtils.toPrimitive(manifestData));
      manifestFiles.add(manifestFile);
    }

    List<DataFile> pendingDataFiles = Lists.newArrayList();
    for (ManifestFile manifestFile : manifestFiles) {
      pendingDataFiles.addAll(FlinkManifest.read(manifestFile, table.io()));
    }

    if (replacePartitions) {
      replacePartitions(pendingDataFiles, newFlinkJobId, checkpointId);
    } else {
      append(pendingDataFiles, newFlinkJobId, checkpointId);
    }

    // Delete the committed manifests and clear the committed data files from dataFilesPerCheckpoint.
    for (ManifestFile manifestFile : manifestFiles) {
      table.io().deleteFile(manifestFile.path());
    }
    pendingManifestMap.clear();
  }

  private void replacePartitions(List<DataFile> dataFiles, String newFlinkJobId, long checkpointId) {
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (DataFile file : dataFiles) {
      numFiles += 1;
      dynamicOverwrite.addFile(file);
    }

    commitOperation(dynamicOverwrite, numFiles, "dynamic partition overwrite", newFlinkJobId, checkpointId);
  }

  private void append(List<DataFile> dataFiles, String newFlinkJobId, long checkpointId) {
    AppendFiles appendFiles = table.newAppend();

    int numFiles = 0;
    for (DataFile file : dataFiles) {
      numFiles += 1;
      appendFiles.appendFile(file);
    }

    commitOperation(appendFiles, numFiles, "append", newFlinkJobId, checkpointId);
  }

  private void commitOperation(SnapshotUpdate<?> operation, int numFiles, String description,
                               String newFlinkJobId, long checkpointId) {
    LOG.info("Committing {} with {} files to table {}", description, numFiles, table);
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void processElement(StreamRecord<DataFile> element) {
    this.dataFilesOfCurrentCheckpoint.add(element.getValue());
  }

  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = Long.MAX_VALUE;
    dataFilesPerCheckpoint.put(currentCheckpointId, writeToManifest(currentCheckpointId));
    dataFilesOfCurrentCheckpoint.clear();

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, currentCheckpointId);
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized bytes.
   */
  private Byte[] writeToManifest(long checkpointId) throws IOException {
    FlinkManifest flinkManifest = flinkManifestFactory.create(checkpointId);
    ManifestFile manifestFile = flinkManifest.write(dataFilesOfCurrentCheckpoint);
    return ArrayUtils.toObject(ManifestFiles.encode(manifestFile));
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static ListStateDescriptor<SortedMap<Long, Byte[]>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, Byte[]> sortedMapTypeInfo = new SortedMapTypeInfo<>(
        BasicTypeInfo.LONG_TYPE_INFO, BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, longComparator
    );
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }

  static long getMaxCommittedCheckpointId(Table table, String flinkJobId) {
    Snapshot snapshot = table.currentSnapshot();
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)) {
        String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          lastCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }
}
