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

package org.apache.iceberg.actions;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ScanTasks;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanScanAction extends BaseAction<CloseableIterable<CombinedScanTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(PlanScanAction.class);

  public enum PlanMode {
    LOCAL,
    DISTRIBUTED
  }

  public static final String ICEBERG_PLAN_MODE = "plan_mode";
  public static final String ICEBERG_TEST_PLAN_MODE = "test_plan_mode";

  public static PlanMode parsePlanMode(String mode) {
    try {
      return PlanMode.valueOf(mode.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(String.format("Cannot use planning mode %s, Available modes are: %s", mode,
          Arrays.toString(PlanMode.values())));
    }
  }

  private final Table table;
  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final TableOperations ops;
  private final Schema schema;

  private TableScanContext context;
  private Snapshot lazySnapshot;

  public PlanScanAction(SparkSession spark, Table table) {
    this.table = table;
    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.schema = table.schema();
    this.ops = ((HasTableOperations) table).operations();
    this.context = new TableScanContext();
  }

  public PlanScanAction withContext(TableScanContext newContext) {
    this.context = newContext;
    return this;
  }

  @Override
  protected Table table() {
    return table;
  }

  @Override
  public CloseableIterable<CombinedScanTask> execute() {
    LOG.info("Preparing distributed planning of scan for {} snapshot {} created at {} with filter {}",
        table, snapshot().snapshotId(), TableScanUtil.formatTimestampMillis(snapshot().timestampMillis()),
        context.rowFilter());
    long start = System.currentTimeMillis();
    CloseableIterable<CombinedScanTask> result = planTasks();
    long elapsed = System.currentTimeMillis() - start;
    LOG.info("Planning complete. Took {} ms", elapsed);
    return result;
  }

  protected CloseableIterable<CombinedScanTask> planTasks() {
    Map<String, String> options = context.options();
    long splitSize;
    if (options.containsKey(TableProperties.SPLIT_SIZE)) {
      splitSize = Long.parseLong(options.get(TableProperties.SPLIT_SIZE));
    } else {
      splitSize = ops.current().propertyAsLong(TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    }
    int lookback;
    if (options.containsKey(TableProperties.SPLIT_LOOKBACK)) {
      lookback = Integer.parseInt(options.get(TableProperties.SPLIT_LOOKBACK));
    } else {
      lookback = ops.current().propertyAsInt(
          TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
    }
    long openFileCost;
    if (options.containsKey(TableProperties.SPLIT_OPEN_FILE_COST)) {
      openFileCost = Long.parseLong(options.get(TableProperties.SPLIT_OPEN_FILE_COST));
    } else {
      openFileCost = ops.current().propertyAsLong(
          TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    }

    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, splitSize);
    return TableScanUtil.planTasks(splitFiles, splitSize, lookback, openFileCost);
  }

  private Snapshot snapshot() {
    if (lazySnapshot == null) {
      lazySnapshot = context.snapshotId() != null ?
          ops.current().snapshot(context.snapshotId()) :
          ops.current().currentSnapshot();
    }
    return lazySnapshot;
  }

  public CloseableIterable<FileScanTask> planFiles() {
    // Create a dataframe of all DataFile entries
    String dataFilesMetadataTable = metadataTableName(MetadataTableType.ENTRIES);
    Dataset<Row> manifestEntries =
        spark.read()
            .format("iceberg")
            .option("snapshot-id", snapshot().snapshotId())
            .load(dataFilesMetadataTable);

    // Todo pushdown filters to ManifestEntriesTable
    // Read entries which are not deleted and are datafiles and not delete files
    Dataset<Row> dataFileEntries = manifestEntries
        .filter(manifestEntries.col("data_file").getField(DataFile.CONTENT.name()).equalTo(0)) // Only DataFiles
        .filter(manifestEntries.col("status").notEqual(2)); // No Deleted Files

    dataFileEntries = handleIncrementalScan(dataFileEntries);

    // Build up evaluators and filters for Metrics and Partition values
    Expression scanFilter = context.rowFilter();
    boolean isCaseSensitive = context.caseSensitive();

    // Build cache of partition evaluators
    Broadcast<Map<Integer, Evaluator>> broadcastPartitionEvaluators = buildPartitionEvaluators();

    // Build metric evaluators
    Broadcast<InclusiveMetricsEvaluator> broadcastMetricsEvaluator = sparkContext.broadcast(
        new InclusiveMetricsEvaluator(schema, scanFilter, isCaseSensitive));

    // Cache residual information and Partition spec information
    Types.StructType partitionStruct = DataFile.getType(table().spec().partitionType());
    StructType dataFileSchema = (StructType) dataFileEntries.schema().apply("data_file").dataType();

    // Evaluate all files based on their partition info and collect the rows back locally
    Dataset<Row> scanTaskDataset = dataFileEntries
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>) it -> {
              SparkDataFile container = new SparkDataFile(partitionStruct, dataFileSchema);
              return Streams.stream(it)
                  .filter(row -> {
                    Row dataFile = row.getAs("data_file");
                    SparkDataFile file = container.wrap(dataFile);
                    return broadcastPartitionEvaluators.getValue().get(file.specId()).eval(file.partition()) &&
                        broadcastMetricsEvaluator.getValue().eval(file);
                  }).iterator();
            }, RowEncoder.apply(dataFileEntries.schema()));

    LoadingCache<Integer, SpecCacheEntry> specCache = buildSpecCache();

    // Build delete index locally
    DeleteFileIndex deleteFileIndex = buildDeleteFileIndex();

    SparkDataFile container = new SparkDataFile(partitionStruct, dataFileSchema);
    List<FileScanTask> tasks = scanTaskDataset.collectAsList().stream().map(row -> {
      Row dataFile = row.getAs("data_file");
      SparkDataFile file = container.wrap(dataFile);
      DeleteFile[] deletes =
          deleteFileIndex.forDataFile(row.getAs("sequence_number"), file);
      SpecCacheEntry cached = specCache.get(file.specId());
      return (FileScanTask) ScanTasks
          .createBaseFileScanTask(file.copy(), deletes, cached.schemaString, cached.specString, cached.residuals);
    }).collect(Collectors.toList());

    return CloseableIterable.withNoopClose(tasks);
  }

  private Dataset<Row> handleIncrementalScan(Dataset<Row> dataFileEntries) {
    if (context.fromSnapshotId() != null) {
      LOG.debug("Planning incremental scan from {} to {}", context.fromSnapshotId(), context.toSnapshotId());
      List<Snapshot> snapshots = SnapshotUtil.snapshotsWithin(table, context.fromSnapshotId(), context.toSnapshotId());
      List<Long> validSnapshotIds = snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toList());
      return dataFileEntries
          .filter(dataFileEntries.col("snapshot_id").isin(validSnapshotIds.toArray()))
          .filter(dataFileEntries.col("status").equalTo(1)); // Added files only
    } else {
      return dataFileEntries;
    }
  }

  private LoadingCache<Integer, SpecCacheEntry> buildSpecCache() {
    return Caffeine.newBuilder().build((CacheLoader<Integer, SpecCacheEntry> & Serializable) specId -> {
      PartitionSpec spec = table().specs().get(specId);
      Expression filter = context.ignoreResiduals() ? Expressions.alwaysTrue() : context.rowFilter();
      return new SpecCacheEntry(SchemaParser.toJson(spec.schema()), PartitionSpecParser.toJson(spec),
          ResidualEvaluator.of(spec, filter, context.caseSensitive()));
    });
  }

  private Broadcast<Map<Integer, Evaluator>> buildPartitionEvaluators() {
    ImmutableMap.Builder<Integer, Evaluator> evalMapBuilder = ImmutableMap.builder();
    boolean caseSensitive = context.caseSensitive();
    Expression filter = context.rowFilter();
    table.specs().entrySet().forEach(entry ->
        evalMapBuilder.put(entry.getKey(),
            new Evaluator(entry.getValue().partitionType(),
                Projections.inclusive(entry.getValue(), caseSensitive).project(filter))));

    Map<Integer, Evaluator> partitionEvaluatorsById = evalMapBuilder.build();
    return  sparkContext.broadcast(partitionEvaluatorsById);
  }


  private DeleteFileIndex buildDeleteFileIndex() {
    // Build delete index locally
    List<ManifestFile> deleteManifests = snapshot().deleteManifests();
    DeleteFileIndex.Builder deleteFileIndexBuilder = DeleteFileIndex.builderFor(table.io(), deleteManifests);
    deleteFileIndexBuilder.caseSensitive(context.caseSensitive());
    deleteFileIndexBuilder.specsById(table.specs());
    deleteFileIndexBuilder.filterData(context.rowFilter());
    return deleteFileIndexBuilder.build();
  }

  private static class SpecCacheEntry implements Serializable {
    private final String schemaString;
    private final String specString;
    private final ResidualEvaluator residuals;

    SpecCacheEntry(String schemaString, String specString, ResidualEvaluator residuals) {
      this.schemaString = schemaString;
      this.specString = specString;
      this.residuals = residuals;
    }
  }
}
