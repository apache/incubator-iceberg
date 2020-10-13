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
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkBatchScan implements Scan, Batch, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchScan.class);

  private final Table table;
  private final boolean caseSensitive;
  private final boolean localityPreferred;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final boolean batchReadsEnabled;
  private final int batchSize;

  // lazy variables
  private StructType readSchema = null;
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkBatchScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption, boolean caseSensitive,
                 Schema expectedSchema, List<Expression> filters, CaseInsensitiveStringMap options) {
    this.table = table;
    this.io = io;
    this.encryptionManager = encryption;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters;
    this.snapshotId = Spark3Util.propertyAsLong(options, "snapshot-id", null);
    this.asOfTimestamp = Spark3Util.propertyAsLong(options, "as-of-timestamp", null);

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = Spark3Util.propertyAsLong(options, "start-snapshot-id", null);
    this.endSnapshotId = Spark3Util.propertyAsLong(options, "end-snapshot-id", null);
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either snapshot-id or " +
                "as-of-timestamp is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }

    // look for split behavior overrides in options
    this.splitSize = Spark3Util.propertyAsLong(options, "split-size", null);
    this.splitLookback = Spark3Util.propertyAsInt(options, "lookback", null);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, "file-open-cost", null);
    this.localityPreferred = Spark3Util.isLocalityEnabled(io.value(), table.location(), options);
    this.batchReadsEnabled = Spark3Util.isVectorizationEnabled(table.properties(), options);
    this.batchSize = Spark3Util.batchSize(table.properties(), options);
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);

    List<CombinedScanTask> scanTasks = tasks();
    InputPartition[] readTasks = new InputPartition[scanTasks.size()];
    for (int i = 0; i < scanTasks.size(); i++) {
      readTasks[i] = new ReadTask(
          scanTasks.get(i), tableSchemaString, expectedSchemaString, nameMappingString, io, encryptionManager,
          caseSensitive, localityPreferred);
    }

    return readTasks;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    boolean allParquetFileScanTasks =
        tasks().stream()
            .allMatch(combinedScanTask -> !combinedScanTask.isDataTask() && combinedScanTask.files()
                .stream()
                .allMatch(fileScanTask -> fileScanTask.file().format().equals(
                    FileFormat.PARQUET)));

    boolean allOrcFileScanTasks =
        tasks().stream()
            .allMatch(combinedScanTask -> !combinedScanTask.isDataTask() && combinedScanTask.files()
                .stream()
                .allMatch(fileScanTask -> fileScanTask.file().format().equals(
                    FileFormat.ORC)));

    boolean atLeastOneColumn = expectedSchema.columns().size() > 0;

    boolean onlyPrimitives = expectedSchema.columns().stream().allMatch(c -> c.type().isPrimitiveType());

    boolean hasNoDeleteFiles = tasks().stream().noneMatch(TableScanUtil::hasDeletes);

    boolean readUsingBatch = batchReadsEnabled && hasNoDeleteFiles && (allOrcFileScanTasks ||
        (allParquetFileScanTasks && atLeastOneColumn && onlyPrimitives));

    return new ReaderFactory(readUsingBatch ? batchSize : 0);
  }

  @Override
  public Statistics estimateStatistics() {
    // its a fresh table, no data
    if (table.currentSnapshot() == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && (filterExpressions == null || filterExpressions.isEmpty())) {
      LOG.debug("using table metadata to estimate table statistics");
      long totalRecords = PropertyUtil.propertyAsLong(table.currentSnapshot().summary(),
          SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      Schema projectedSchema = expectedSchema != null ? expectedSchema : table.schema();
      return new Stats(
          SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(projectedSchema), totalRecords),
          totalRecords);
    }

    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchScan that = (SparkBatchScan) o;
    return table.name().equals(that.table.name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions.toString().equals(that.filterExpressions.toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table.name(), readSchema(), filterExpressions.toString(), snapshotId, startSnapshotId, endSnapshotId,
        asOfTimestamp);
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(expectedSchema);

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
      }

      if (splitSize != null) {
        scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
      }

      if (splitLookback != null) {
        scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
      }

      if (splitOpenFileCost != null) {
        scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
      }

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      }  catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public String description() {
    String filters = filterExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, filters);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table, expectedSchema.asStruct(), filterExpressions, caseSensitive);
  }

  private static class ReaderFactory implements PartitionReaderFactory {
    private final int batchSize;

    private ReaderFactory(int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new RowReader((ReadTask) partition);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new BatchReader((ReadTask) partition, batchSize);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return batchSize > 1;
    }
  }

  private static class RowReader extends RowDataReader implements PartitionReader<InternalRow> {
    RowReader(ReadTask task) {
      super(task.task, task.tableSchema(), task.expectedSchema(), task.nameMappingString, task.io(), task.encryption(),
          task.isCaseSensitive());
    }
  }

  private static class BatchReader extends BatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, int batchSize) {
      super(task.task, task.expectedSchema(), task.nameMappingString, task.io(), task.encryption(),
          task.isCaseSensitive(), batchSize);
    }
  }

  private static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final String nameMappingString;
    private final Broadcast<FileIO> io;
    private final Broadcast<EncryptionManager> encryptionManager;
    private final boolean caseSensitive;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;
    private transient NameMapping nameMapping = null;
    private transient String[] preferredLocations = null;

    ReadTask(CombinedScanTask task, String tableSchemaString, String expectedSchemaString, String nameMappingString,
             Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager, boolean caseSensitive,
             boolean localityPreferred) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.nameMappingString = nameMappingString;
      this.io = io;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      if (localityPreferred) {
        this.preferredLocations = Util.blockLocations(io.value(), task);
      } else {
        this.preferredLocations = HadoopInputFile.NO_LOCATION_PREFERENCE;
      }
    }

    @Override
    public String[] preferredLocations() {
      return preferredLocations;
    }

    public Collection<FileScanTask> files() {
      return task.files();
    }

    public FileIO io() {
      return io.value();
    }

    public EncryptionManager encryption() {
      return encryptionManager.value();
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    private Schema tableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }

    private NameMapping nameMapping() {
      if (nameMapping == null) {
        this.nameMapping = NameMappingParser.fromJson(nameMappingString);
      }
      return nameMapping;
    }
  }
}
