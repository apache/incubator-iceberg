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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<WriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String CURRENT_WATERMARK = "flink.current-watermark";
  static final String STORE_WATERMARK = "flink.store-watermark";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();
  private final Map<Long, Long> watermarkPerCheckpoint = Maps.newHashMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // 'dataFilesPerCheckpoint'.
  private final List<WriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient Table table;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient long maxCommittedCheckpointId;
  private transient long currentWatermark;
  private transient boolean storeWatermark;

  // There're two cases that we restore from flink checkpoints: the first case is restoring from snapshot created by the
  // same flink job; another case is restoring from snapshot created by another different job. For the second case, we
  // need to maintain the old flink job's id in flink state backend to find the max-committed-checkpoint-id when
  // traversing iceberg table's snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;
  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<SortedMap<Long, byte[]>> checkpointsState;
  private static final ListStateDescriptor<Map<Long, Long>> WATERMARK_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-watermark", new MapTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));
  private transient ListState<Map<Long, Long>> watermarkState;

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
    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    Map<String, String> properties = this.table.properties();
    this.currentWatermark = PropertyUtil.propertyAsLong(properties, CURRENT_WATERMARK, -1L);
    this.storeWatermark = PropertyUtil.propertyAsBoolean(properties, STORE_WATERMARK, false);

    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    this.watermarkState = context.getOperatorStateStore().getListState(WATERMARK_DESCRIPTOR);

    if (context.isRestored()) {
      watermarkPerCheckpoint.putAll(watermarkState.get().iterator().next());
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new flink job even if
      // it's restored from a snapshot created by another different flink job, so it's safe to assign the max committed
      // checkpoint id from restored flink job to the current flink job.
      this.maxCommittedCheckpointId = getMaxCommittedCheckpointId(table, restoredFlinkJobId);

      NavigableMap<Long, byte[]> uncommittedDataFiles = Maps
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

    watermarkPerCheckpoint.put(checkpointId, currentWatermark);
    watermarkState.clear();
    watermarkState.add(watermarkPerCheckpoint);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    // Clear the local buffer for current checkpoint.
    writeResultsOfCurrentCkpt.clear();
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

  private void commitUpToCheckpoint(NavigableMap<Long, byte[]> deltaManifestsMap,
                                    String newFlinkJobId,
                                    long checkpointId) throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);

    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests = SimpleVersionedSerialization
          .readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, e.getValue());
      pendingResults.put(e.getKey(), FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io()));
      manifests.addAll(deltaManifests.manifests());
    }

    if (replacePartitions) {
      replacePartitions(pendingResults, newFlinkJobId, checkpointId);
    } else {
      commitDeltaTxn(pendingResults, newFlinkJobId, checkpointId);
    }

    pendingMap.clear();

    // Delete the committed manifests.
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details = MoreObjects.toStringHelper(this)
            .add("flinkJobId", newFlinkJobId)
            .add("checkpointId", checkpointId)
            .add("manifestPath", manifest.path())
            .toString();
        LOG.warn("The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details, e);
      }
    }
  }

  private void replacePartitions(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId,
                                 long checkpointId) {
    // Partition overwrite does not support delete files.
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();
    Preconditions.checkState(deleteFilesNum == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

      numFiles += result.dataFiles().length;
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(dynamicOverwrite, numFiles, 0, "dynamic partition overwrite", newFlinkJobId, checkpointId);
  }

  private void commitDeltaTxn(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId, long checkpointId) {
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();

    if (deleteFilesNum == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend();

      int numFiles = 0;
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

        numFiles += result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }

      commitOperation(appendFiles, numFiles, 0, "append", newFlinkJobId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential transaction txn1 and
        // txn2, the equality-delete files of txn2 are required to be applied to data files from txn1. Committing the
        // merged one will lead to the incorrect delete semantic.
        WriteResult result = e.getValue();
        RowDelta rowDelta = table.newRowDelta()
            .validateDataFilesExist(ImmutableList.copyOf(result.referencedDataFiles()))
            .validateDeletedFiles();

        int numDataFiles = result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

        int numDeleteFiles = result.deleteFiles().length;
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        commitOperation(rowDelta, numDataFiles, numDeleteFiles, "rowDelta", newFlinkJobId, e.getKey());
      }
    }
  }

  private void commitOperation(SnapshotUpdate<?> operation, int numDataFiles, int numDeleteFiles, String description,
                               String newFlinkJobId, long checkpointId) {
    LOG.info("Committing {} with {} data files and {} delete files to table {}", description, numDataFiles,
        numDeleteFiles, table);
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.

    Long watermarkForCheckpoint = watermarkPerCheckpoint.get(checkpointId);
    if (storeWatermark && watermarkForCheckpoint != null && watermarkForCheckpoint > 0) {
      table.updateProperties().set(CURRENT_WATERMARK, String.valueOf(watermarkForCheckpoint)).commit();
      LOG.info("Update {} to {}", CURRENT_WATERMARK, watermarkForCheckpoint);
    }

    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void processElement(StreamRecord<WriteResult> element) {
    this.writeResultsOfCurrentCkpt.add(element.getValue());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    if (mark.getTimestamp() != Watermark.MAX_WATERMARK.getTimestamp()) {
      currentWatermark = mark.getTimestamp();
    }
  }

  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = Long.MAX_VALUE;
    dataFilesPerCheckpoint.put(currentCheckpointId, writeToManifest(currentCheckpointId));
    writeResultsOfCurrentCkpt.clear();

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, currentCheckpointId);
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultsOfCurrentCkpt).build();
    DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(result,
        () -> manifestOutputFileFactory.create(checkpointId), table.spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo = new SortedMapTypeInfo<>(
        BasicTypeInfo.LONG_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, longComparator
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
