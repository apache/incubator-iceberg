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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkBatchScan implements Scan, Batch, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchScan.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final boolean caseSensitive;
  private final boolean localityPreferred;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final int batchSize;
  private final CaseInsensitiveStringMap options;

  // lazy variables
  private StructType readSchema = null;

  SparkBatchScan(SparkSession spark, Table table, boolean caseSensitive, Schema expectedSchema,
                 List<Expression> filters, CaseInsensitiveStringMap options) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
    this.batchSize = Spark3Util.batchSize(table.properties(), options);
    this.options = options;
  }

  protected Table table() {
    return table;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected List<Expression> filterExpressions() {
    return filterExpressions;
  }

  protected abstract List<CombinedScanTask> tasks();

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkMicroBatchStream(
        sparkContext, table, caseSensitive, expectedSchema, options, checkpointLocation);
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  protected abstract Schema snapshotSchema();

  @Override
  public InputPartition[] planInputPartitions() {
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));

    List<CombinedScanTask> scanTasks = tasks();
    InputPartition[] readTasks = new InputPartition[scanTasks.size()];
    for (int i = 0; i < scanTasks.size(); i++) {
      readTasks[i] = new ReadTask(
          scanTasks.get(i), tableBroadcast, expectedSchemaString,
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

    boolean batchReadsEnabled = batchReadsEnabled(allParquetFileScanTasks, allOrcFileScanTasks);

    boolean readUsingBatch = batchReadsEnabled && hasNoDeleteFiles && (allOrcFileScanTasks ||
        (allParquetFileScanTasks && atLeastOneColumn && onlyPrimitives));

    return new ReaderFactory(readUsingBatch ? batchSize : 0);
  }

  private boolean batchReadsEnabled(boolean isParquetOnly, boolean isOrcOnly) {
    Map<String, String> properties = table.properties();
    RuntimeConfig sessionConf = SparkSession.active().conf();
    if (isParquetOnly) {
      return Spark3Util.isVectorizationEnabled(FileFormat.PARQUET, properties, sessionConf, options);
    } else if (isOrcOnly) {
      return Spark3Util.isVectorizationEnabled(FileFormat.ORC, properties, sessionConf, options);
    } else {
      return false;
    }
  }

  @Override
  public Statistics estimateStatistics() {
    // its a fresh table, no data
    if (table.currentSnapshot() == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpressions.isEmpty()) {
      LOG.debug("using table metadata to estimate table statistics");
      long totalRecords = PropertyUtil.propertyAsLong(table.currentSnapshot().summary(),
          SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      Schema projectedSchema = expectedSchema != null ? expectedSchema : snapshotSchema();
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
  public String description() {
    String filters = filterExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, filters);
  }

  static class ReaderFactory implements PartitionReaderFactory {
    private final int batchSize;

    ReaderFactory(int batchSize) {
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
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive());
    }
  }

  private static class BatchReader extends BatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, int batchSize) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive(), batchSize);
    }
  }

  static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final Broadcast<Table> tableBroadcast;
    private final String expectedSchemaString;
    private final boolean caseSensitive;

    private transient Schema expectedSchema = null;
    private transient String[] preferredLocations = null;

    ReadTask(CombinedScanTask task, Broadcast<Table> tableBroadcast, String expectedSchemaString,
             boolean caseSensitive, boolean localityPreferred) {
      this.task = task;
      this.tableBroadcast = tableBroadcast;
      this.expectedSchemaString = expectedSchemaString;
      this.caseSensitive = caseSensitive;
      if (localityPreferred) {
        Table table = tableBroadcast.value();
        this.preferredLocations = Util.blockLocations(table.io(), task);
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

    public Table table() {
      return tableBroadcast.value();
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }
}
