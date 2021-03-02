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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final FileFormat format;
  private final boolean replacePartitions;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final boolean partitionedFanoutEnabled;

  Writer(SparkSession spark, Table table, DataSourceOptions options, boolean replacePartitions,
         String applicationId, Schema writeSchema, StructType dsSchema) {
    this(spark, table, options, replacePartitions, applicationId, null, writeSchema, dsSchema);
  }

  Writer(SparkSession spark, Table table, DataSourceOptions options, boolean replacePartitions,
         String applicationId, String wapId, Schema writeSchema, StructType dsSchema) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.format = getFileFormat(table.properties(), options);
    this.replacePartitions = replacePartitions;
    this.applicationId = applicationId;
    this.wapId = wapId;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = Maps.newHashMap();

    options.asMap().forEach((key, value) -> {
      if (key.startsWith(SnapshotSummary.EXTRA_METADATA_PREFIX)) {
        extraSnapshotMetadata.put(key.substring(SnapshotSummary.EXTRA_METADATA_PREFIX.length()), value);
      }
    });

    long tableTargetFileSize = PropertyUtil.propertyAsLong(
        table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = options.getLong(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, tableTargetFileSize);

    boolean tablePartitionedFanoutEnabled = PropertyUtil.propertyAsBoolean(
        table.properties(), SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT);
    this.partitionedFanoutEnabled = options.getBoolean(SparkWriteOptions.FANOUT_ENABLED, tablePartitionedFanoutEnabled);
  }

  private FileFormat getFileFormat(Map<String, String> tableProperties, DataSourceOptions options) {
    Optional<String> formatOption = options.get(SparkWriteOptions.WRITE_FORMAT);
    String formatString = formatOption
        .orElseGet(() -> tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(table.properties().getOrDefault(
        TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
<<<<<<< HEAD
    // broadcast the table metadata as the writer factory will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    return new WriterFactory(tableBroadcast, format, targetFileSize, writeSchema, dsSchema, partitionedFanoutEnabled);
=======
    return new WriterFactory(
        table.spec(), table.sortOrder(), format, table.locationProvider(), table.properties(),
        io, encryptionManager, targetFileSize, writeSchema, dsSchema, partitionedFanoutEnabled);
>>>>>>> Address review comments
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (replacePartitions) {
      replacePartitions(messages);
    } else {
      append(messages);
    }
  }

  protected void commitOperation(SnapshotUpdate<?> operation, int numFiles, String description) {
    LOG.info("Committing {} with {} files to table {}", description, numFiles, table);
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

  private void append(WriterCommitMessage[] messages) {
    AppendFiles append = table.newAppend();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "append");
  }

  private void replacePartitions(WriterCommitMessage[] messages) {
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      dynamicOverwrite.addFile(file);
    }

    commitOperation(dynamicOverwrite, numFiles, "dynamic partition overwrite");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
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

  protected Table table() {
    return table;
  }

  protected Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return Iterables.concat(Iterables.transform(Arrays.asList(messages), message -> message != null ?
          ImmutableList.copyOf(((TaskCommit) message).files()) :
          ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }

  private static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] taskFiles;

    TaskCommit(DataFile[] files) {
      this.taskFiles = files;
    }

    DataFile[] files() {
      return this.taskFiles;
    }
  }

  static class WriterFactory implements DataWriterFactory<InternalRow> {
<<<<<<< HEAD
    private final Broadcast<Table> tableBroadcast;
=======
    private final PartitionSpec spec;
    private final SortOrder sortOrder;
>>>>>>> Address review comments
    private final FileFormat format;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final boolean partitionedFanoutEnabled;

<<<<<<< HEAD
    WriterFactory(Broadcast<Table> tableBroadcast, FileFormat format, long targetFileSize,
                  Schema writeSchema, StructType dsSchema, boolean partitionedFanoutEnabled) {
      this.tableBroadcast = tableBroadcast;
=======
    WriterFactory(PartitionSpec spec, SortOrder sortOrder, FileFormat format, LocationProvider locations,
                  Map<String, String> properties, Broadcast<FileIO> io,
                  Broadcast<EncryptionManager> encryptionManager, long targetFileSize,
                  Schema writeSchema, StructType dsSchema, boolean partitionedFanoutEnabled) {
      this.spec = spec;
      this.sortOrder = sortOrder;
>>>>>>> Address review comments
      this.format = format;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.partitionedFanoutEnabled = partitionedFanoutEnabled;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
<<<<<<< HEAD
      Table table = tableBroadcast.value();

      OutputFileFactory fileFactory = new OutputFileFactory(table, format, partitionId, taskId);
      SparkAppenderFactory appenderFactory = SparkAppenderFactory.builderFor(table, writeSchema, dsSchema).build();

      PartitionSpec spec = table.spec();
      FileIO io = table.io();
=======
      OutputFileFactory fileFactory = new OutputFileFactory(
          spec, format, locations, io.value(), encryptionManager.value(), partitionId, taskId);
<<<<<<< HEAD
      SparkAppenderFactory appenderFactory = SparkAppenderFactory.builderFor(properties, writeSchema, dsSchema)
          .partitionSpec(spec).build();
>>>>>>> Create SparkAppenderFactory builder, avoid argument explosion
=======
      SparkAppenderFactory appenderFactory = new SparkAppenderFactory(properties,
          writeSchema, dsSchema, spec, sortOrder);
>>>>>>> Address review comments

      if (spec.isUnpartitioned()) {
        return new Unpartitioned24Writer(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      } else if (partitionedFanoutEnabled) {
        return new PartitionedFanout24Writer(spec, format, appenderFactory, fileFactory, io, targetFileSize,
            writeSchema, dsSchema);
      } else {
        return new Partitioned24Writer(spec, format, appenderFactory, fileFactory, io, targetFileSize,
            writeSchema, dsSchema);
      }
    }
  }

  private static class Unpartitioned24Writer extends UnpartitionedWriter<InternalRow>
      implements DataWriter<InternalRow> {
    Unpartitioned24Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                          OutputFileFactory fileFactory, FileIO fileIo, long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class Partitioned24Writer extends SparkPartitionedWriter implements DataWriter<InternalRow> {

    Partitioned24Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                        OutputFileFactory fileFactory, FileIO fileIo, long targetFileSize,
                        Schema schema, StructType sparkSchema) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class PartitionedFanout24Writer extends SparkPartitionedFanoutWriter
      implements DataWriter<InternalRow> {

    PartitionedFanout24Writer(PartitionSpec spec, FileFormat format,
                              SparkAppenderFactory appenderFactory,
                              OutputFileFactory fileFactory, FileIO fileIo, long targetFileSize,
                              Schema schema, StructType sparkSchema) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize, schema,
          sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }
}
