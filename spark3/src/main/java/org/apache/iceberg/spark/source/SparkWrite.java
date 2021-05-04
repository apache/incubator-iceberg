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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.IsolationLevel.SERIALIZABLE;
import static org.apache.iceberg.IsolationLevel.SNAPSHOT;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

class SparkWrite {
  private static final Logger LOG = LoggerFactory.getLogger(SparkWrite.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final boolean partitionedFanoutEnabled;

  SparkWrite(SparkSession spark, Table table, LogicalWriteInfo writeInfo,
             String applicationId, String wapId,
             Schema writeSchema, StructType dsSchema) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.queryId = writeInfo.queryId();
    this.format = getFileFormat(table.properties(), writeInfo.options());
    this.applicationId = applicationId;
    this.wapId = wapId;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = Maps.newHashMap();

    writeInfo.options().forEach((key, value) -> {
      if (key.startsWith(SnapshotSummary.EXTRA_METADATA_PREFIX)) {
        extraSnapshotMetadata.put(key.substring(SnapshotSummary.EXTRA_METADATA_PREFIX.length()), value);
      }
    });

    long tableTargetFileSize = PropertyUtil.propertyAsLong(
        table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = writeInfo.options().getLong(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, tableTargetFileSize);

    boolean tablePartitionedFanoutEnabled = PropertyUtil.propertyAsBoolean(
        table.properties(), SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT);
    this.partitionedFanoutEnabled = writeInfo.options()
        .getBoolean(SparkWriteOptions.FANOUT_ENABLED, tablePartitionedFanoutEnabled);
  }

  BatchWrite asBatchAppend() {
    return new BatchAppend();
  }

  BatchWrite asDynamicOverwrite() {
    return new DynamicOverwrite();
  }

  BatchWrite asOverwriteByFilter(Expression overwriteExpr) {
    return new OverwriteByFilter(overwriteExpr);
  }

  BatchWrite asCopyOnWriteMergeWrite(SparkMergeScan scan, IsolationLevel isolationLevel) {
    return new CopyOnWriteMergeWrite(scan, isolationLevel);
  }

  BatchWrite asRewrite(String fileSetID) {
    return new RewriteFiles(fileSetID);
  }

  StreamingWrite asStreamingAppend() {
    return new StreamingAppend();
  }

  StreamingWrite asStreamingOverwrite() {
    return new StreamingOverwrite();
  }

  private FileFormat getFileFormat(Map<String, String> tableProperties, Map<String, String> options) {
    Optional<String> formatOption = Optional.ofNullable(options.get(SparkWriteOptions.WRITE_FORMAT));
    String formatString = formatOption
        .orElseGet(() -> tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(table.properties().getOrDefault(
        TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  // the writer factory works for both batch and streaming
  private WriterFactory createWriterFactory() {
    // broadcast the table metadata as the writer factory will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    return new WriterFactory(tableBroadcast, format, targetFileSize, writeSchema, dsSchema, partitionedFanoutEnabled);
  }

  private void commitOperation(SnapshotUpdate<?> operation, String description) {
    LOG.info("Committing {} to table {}", description, table);
    if (applicationId != null) {
      operation.set("spark.app.id", applicationId);
    }

    if (!extraSnapshotMetadata.isEmpty()) {
      extraSnapshotMetadata.forEach(operation::set);
    }

    if (isWapTable() && wapId != null) {
      // write-audit-publish is enabled for this table and job
      // stage the changes without changing the current snapshot
      operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
      operation.stageOnly();
    }

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  private void abort(WriterCommitMessage[] messages) {
    Map<String, String> props = table.properties();
    Tasks.foreach(files(messages))
        .retry(PropertyUtil.propertyAsInt(props, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(props, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(props, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(props, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .throwFailureWhenFinished()
        .run(file -> {
          table.io().deleteFile(file.path().toString());
        });
  }

  private Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return Iterables.concat(Iterables.transform(Arrays.asList(messages), message -> message != null ?
          ImmutableList.copyOf(((TaskCommit) message).files()) :
          ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  private abstract class BaseBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return createWriterFactory();
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      SparkWrite.this.abort(messages);
    }

    @Override
    public String toString() {
      return String.format("IcebergBatchWrite(table=%s, format=%s)", table, format);
    }
  }

  private class BatchAppend extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
      AppendFiles append = table.newAppend();

      int numFiles = 0;
      for (DataFile file : files(messages)) {
        numFiles += 1;
        append.appendFile(file);
      }

      commitOperation(append, String.format("append with %d new data files", numFiles));
    }
  }

  private class DynamicOverwrite extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
      ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

      int numFiles = 0;
      for (DataFile file : files(messages)) {
        numFiles += 1;
        dynamicOverwrite.addFile(file);
      }

      commitOperation(dynamicOverwrite, String.format("dynamic partition overwrite with %d new data files", numFiles));
    }
  }

  private class OverwriteByFilter extends BaseBatchWrite {
    private final Expression overwriteExpr;

    private OverwriteByFilter(Expression overwriteExpr) {
      this.overwriteExpr = overwriteExpr;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();
      overwriteFiles.overwriteByRowFilter(overwriteExpr);

      int numFiles = 0;
      for (DataFile file : files(messages)) {
        numFiles += 1;
        overwriteFiles.addFile(file);
      }

      String commitMsg = String.format("overwrite by filter %s with %d new data files", overwriteExpr, numFiles);
      commitOperation(overwriteFiles, commitMsg);
    }
  }

  private class CopyOnWriteMergeWrite extends BaseBatchWrite {
    private final SparkMergeScan scan;
    private final IsolationLevel isolationLevel;

    private CopyOnWriteMergeWrite(SparkMergeScan scan, IsolationLevel isolationLevel) {
      this.scan = scan;
      this.isolationLevel = isolationLevel;
    }

    private List<DataFile> overwrittenFiles() {
      return scan.files().stream().map(FileScanTask::file).collect(Collectors.toList());
    }

    private Expression conflictDetectionFilter() {
      // the list of filter expressions may be empty but is never null
      List<Expression> scanFilterExpressions = scan.filterExpressions();

      Expression filter = Expressions.alwaysTrue();

      for (Expression expr : scanFilterExpressions) {
        filter = Expressions.and(filter, expr);
      }

      return filter;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();

      List<DataFile> overwrittenFiles = overwrittenFiles();
      int numOverwrittenFiles = overwrittenFiles.size();
      for (DataFile overwrittenFile : overwrittenFiles) {
        overwriteFiles.deleteFile(overwrittenFile);
      }

      int numAddedFiles = 0;
      for (DataFile file : files(messages)) {
        numAddedFiles += 1;
        overwriteFiles.addFile(file);
      }

      if (isolationLevel == SERIALIZABLE) {
        commitWithSerializableIsolation(overwriteFiles, numOverwrittenFiles, numAddedFiles);
      } else if (isolationLevel == SNAPSHOT) {
        commitWithSnapshotIsolation(overwriteFiles, numOverwrittenFiles, numAddedFiles);
      } else {
        throw new IllegalArgumentException("Unsupported isolation level: " + isolationLevel);
      }
    }

    private void commitWithSerializableIsolation(OverwriteFiles overwriteFiles,
                                                 int numOverwrittenFiles,
                                                 int numAddedFiles) {
      Long scanSnapshotId = scan.snapshotId();
      if (scanSnapshotId != null) {
        overwriteFiles.validateFromSnapshot(scanSnapshotId);
      }

      Expression conflictDetectionFilter = conflictDetectionFilter();
      overwriteFiles.validateNoConflictingAppends(conflictDetectionFilter);

      String commitMsg = String.format(
          "overwrite of %d data files with %d new data files, scanSnapshotId: %d, conflictDetectionFilter: %s",
          numOverwrittenFiles, numAddedFiles, scanSnapshotId, conflictDetectionFilter);
      commitOperation(overwriteFiles, commitMsg);
    }

    private void commitWithSnapshotIsolation(OverwriteFiles overwriteFiles,
                                             int numOverwrittenFiles,
                                             int numAddedFiles) {
      String commitMsg = String.format(
          "overwrite of %d data files with %d new data files",
          numOverwrittenFiles, numAddedFiles);
      commitOperation(overwriteFiles, commitMsg);
    }
  }

  private class RewriteFiles extends BaseBatchWrite {
    private final String fileSetID;

    private RewriteFiles(String fileSetID) {
      this.fileSetID = fileSetID;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();

      Set<DataFile> newDataFiles = Sets.newHashSetWithExpectedSize(messages.length);
      for (DataFile file : files(messages)) {
        newDataFiles.add(file);
      }

      coordinator.stageRewrite(table, fileSetID, Collections.unmodifiableSet(newDataFiles));
    }
  }

  private abstract class BaseStreamingWrite implements StreamingWrite {
    private static final String QUERY_ID_PROPERTY = "spark.sql.streaming.queryId";
    private static final String EPOCH_ID_PROPERTY = "spark.sql.streaming.epochId";

    protected abstract String mode();

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
      return createWriterFactory();
    }

    @Override
    public final void commit(long epochId, WriterCommitMessage[] messages) {
      LOG.info("Committing epoch {} for query {} in {} mode", epochId, queryId, mode());

      table.refresh();

      Long lastCommittedEpochId = findLastCommittedEpochId();
      if (lastCommittedEpochId != null && epochId <= lastCommittedEpochId) {
        LOG.info("Skipping epoch {} for query {} as it was already committed", epochId, queryId);
        return;
      }

      doCommit(epochId, messages);
    }

    protected abstract void doCommit(long epochId, WriterCommitMessage[] messages);

    protected <T> void commit(SnapshotUpdate<T> snapshotUpdate, long epochId, String description) {
      snapshotUpdate.set(QUERY_ID_PROPERTY, queryId);
      snapshotUpdate.set(EPOCH_ID_PROPERTY, Long.toString(epochId));
      commitOperation(snapshotUpdate, description);
    }

    private Long findLastCommittedEpochId() {
      Snapshot snapshot = table.currentSnapshot();
      Long lastCommittedEpochId = null;
      while (snapshot != null) {
        Map<String, String> summary = snapshot.summary();
        String snapshotQueryId = summary.get(QUERY_ID_PROPERTY);
        if (queryId.equals(snapshotQueryId)) {
          lastCommittedEpochId = Long.valueOf(summary.get(EPOCH_ID_PROPERTY));
          break;
        }
        Long parentSnapshotId = snapshot.parentId();
        snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
      }
      return lastCommittedEpochId;
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
      SparkWrite.this.abort(messages);
    }

    @Override
    public String toString() {
      return String.format("IcebergStreamingWrite(table=%s, format=%s)", table, format);
    }
  }

  private class StreamingAppend extends BaseStreamingWrite {
    @Override
    protected String mode() {
      return "append";
    }

    @Override
    protected void doCommit(long epochId, WriterCommitMessage[] messages) {
      AppendFiles append = table.newFastAppend();
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        append.appendFile(file);
        numFiles++;
      }
      commit(append, epochId, String.format("streaming append with %d new data files", numFiles));
    }
  }

  private class StreamingOverwrite extends BaseStreamingWrite {
    @Override
    protected String mode() {
      return "complete";
    }

    @Override
    public void doCommit(long epochId, WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();
      overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        overwriteFiles.addFile(file);
        numFiles++;
      }
      commit(overwriteFiles, epochId, String.format("streaming complete overwrite with %d new data files", numFiles));
    }
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] taskFiles;

    TaskCommit(DataFile[] taskFiles) {
      this.taskFiles = taskFiles;
    }

    DataFile[] files() {
      return taskFiles;
    }
  }

  private static class WriterFactory implements DataWriterFactory, StreamingDataWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final FileFormat format;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final boolean partitionedFanoutEnabled;

    protected WriterFactory(Broadcast<Table> tableBroadcast, FileFormat format, long targetFileSize,
                            Schema writeSchema, StructType dsSchema, boolean partitionedFanoutEnabled) {
      this.tableBroadcast = tableBroadcast;
      this.format = format;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.partitionedFanoutEnabled = partitionedFanoutEnabled;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return createWriter(partitionId, taskId, 0);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      Table table = tableBroadcast.value();

      OutputFileFactory fileFactory = new OutputFileFactory(table, format, partitionId, taskId);
      SparkAppenderFactory appenderFactory = SparkAppenderFactory.builderFor(table, writeSchema, dsSchema).build();

      PartitionSpec spec = table.spec();
      FileIO io = table.io();

      if (spec.isUnpartitioned()) {
        return new Unpartitioned3Writer(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      } else if (partitionedFanoutEnabled) {
        return new PartitionedFanout3Writer(
            spec, format, appenderFactory, fileFactory, io, targetFileSize, writeSchema, dsSchema);
      } else {
        return new Partitioned3Writer(
            spec, format, appenderFactory, fileFactory, io, targetFileSize, writeSchema, dsSchema);
      }
    }
  }

  private static class Unpartitioned3Writer extends UnpartitionedWriter<InternalRow>
      implements DataWriter<InternalRow> {
    Unpartitioned3Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                         OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      this.close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class Partitioned3Writer extends SparkPartitionedWriter implements DataWriter<InternalRow> {
    Partitioned3Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                       OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                       Schema schema, StructType sparkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      this.close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class PartitionedFanout3Writer extends SparkPartitionedFanoutWriter
      implements DataWriter<InternalRow> {
    PartitionedFanout3Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                             OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                             Schema schema, StructType sparkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      this.close();

      return new TaskCommit(dataFiles());
    }
  }
}
