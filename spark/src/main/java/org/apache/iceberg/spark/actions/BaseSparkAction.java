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

package org.apache.iceberg.spark.actions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.actions.ManifestFileBean;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.JobGroupUtils;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;

abstract class BaseSparkAction<ThisT, R> implements Action<ThisT, R> {

  private static final AtomicInteger JOB_COUNTER = new AtomicInteger();

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Map<String, String> options = Maps.newHashMap();

  protected BaseSparkAction(SparkSession spark) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
  }

  protected SparkSession spark() {
    return spark;
  }

  protected JavaSparkContext sparkContext() {
    return sparkContext;
  }

  protected abstract ThisT self();

  @Override
  public ThisT option(String name, String value) {
    options.put(name, value);
    return self();
  }

  @Override
  public ThisT options(Map<String, String> newOptions) {
    options.putAll(newOptions);
    return self();
  }

  protected Map<String, String> options() {
    return options;
  }

  protected <T> T withJobGroupInfo(JobGroupInfo info, Supplier<T> supplier) {
    SparkContext context = spark().sparkContext();
    JobGroupInfo previousInfo = JobGroupUtils.getJobGroupInfo(context);
    try {
      JobGroupUtils.setJobGroupInfo(context, info);
      return supplier.get();
    } finally {
      JobGroupUtils.setJobGroupInfo(context, previousInfo);
    }
  }

  protected JobGroupInfo newJobGroupInfo(String groupId, String desc) {
    return new JobGroupInfo(groupId + "-" + JOB_COUNTER.incrementAndGet(), desc, false);
  }

  /**
   * Returns all the path locations of all Manifest Lists for a given list of snapshots
   * @param snapshots snapshots
   * @return the paths of the Manifest Lists
   */
  private List<String> getManifestListPaths(Iterable<Snapshot> snapshots) {
    List<String> manifestLists = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestLists.add(manifestListLocation);
      }
    }
    return manifestLists;
  }

  /**
   * Returns all Metadata file paths which may not be in the current metadata. Specifically
   * this includes "version-hint" files as well as entries in metadata.previousFiles.
   * @param ops TableOperations for the table we will be getting paths from
   * @return a list of paths to metadata files
   */
  private List<String> getOtherMetadataFilePaths(TableOperations ops) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.add(ops.metadataFileLocation("version-hint.text"));

    TableMetadata metadata = ops.current();
    otherMetadataFiles.add(metadata.metadataFileLocation());
    for (TableMetadata.MetadataLogEntry previousMetadataFile : metadata.previousFiles()) {
      otherMetadataFiles.add(previousMetadataFile.file());
    }
    return otherMetadataFiles;
  }

  protected Table newStaticTable(TableMetadata metadata, FileIO io) {
    String metadataFileLocation = metadata.metadataFileLocation();
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  protected Dataset<Row> buildValidDataFileDF(Table table) {
    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
    Broadcast<FileIO> ioBroadcast = context.broadcast(SparkUtil.serializableFileIO(table));
    return loadAllManifestFileBean(table).filter((FilterFunction<ManifestFileBean>) manifest ->
        manifest.content() == ManifestContent.DATA)
        .flatMap(new ReadManifest(ioBroadcast), Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildValidDeleteFileDF(Table table) {
    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
    Broadcast<FileIO> ioBroadcast = context.broadcast(SparkUtil.serializableFileIO(table));
    return loadAllManifestFileBean(table).filter((FilterFunction<ManifestFileBean>) manifest ->
        manifest.content() == ManifestContent.DELETES)
      .flatMap(new ReadManifest(ioBroadcast), Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestFileDF(Table table) {
    return loadMetadataTable(table, ALL_MANIFESTS).selectExpr("path as file_path");
  }

  protected Dataset<Row> buildManifestListDF(Table table) {
    List<String> manifestLists = getManifestListPaths(table.snapshots());
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildOtherMetadataFileDF(TableOperations ops) {
    List<String> otherMetadataFiles = getOtherMetadataFilePaths(ops);
    return spark.createDataset(otherMetadataFiles, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildValidMetadataFileDF(Table table, TableOperations ops) {
    Dataset<Row> manifestDF = buildManifestFileDF(table);
    Dataset<Row> manifestListDF = buildManifestListDF(table);
    Dataset<Row> otherMetadataFileDF = buildOtherMetadataFileDF(ops);

    return manifestDF.union(otherMetadataFileDF).union(manifestListDF);
  }

  // Attempt to use Spark3 Catalog resolution if available on the path
  private static final DynMethods.UnboundMethod LOAD_CATALOG = DynMethods.builder("loadCatalogMetadataTable")
      .hiddenImpl("org.apache.iceberg.spark.Spark3Util", SparkSession.class, String.class, MetadataTableType.class)
      .orNoop()
      .build();

  private Dataset<ManifestFileBean> loadAllManifestFileBean(Table table) {
    return loadMetadataTable(table, ALL_MANIFESTS)
        .selectExpr("path", "length", "partition_spec_id as partitionSpecId", "content",
          "added_snapshot_id as addedSnapshotId")
        .dropDuplicates("path")
        .repartition(spark.sessionState().conf().numShufflePartitions()) // avoid adaptive execution combining tasks
        .as(Encoders.bean(ManifestFileBean.class));
  }

  private Dataset<Row> loadCatalogMetadataTable(String tableName, MetadataTableType type) {
    Preconditions.checkArgument(!LOAD_CATALOG.isNoop(), "Cannot find Spark3Util class but Spark3 is in use");
    return LOAD_CATALOG.asStatic().invoke(spark, tableName, type);
  }

  protected Dataset<Row> loadMetadataTable(Table table, MetadataTableType type) {
    String tableName = table.name();
    String tableLocation = table.location();

    DataFrameReader dataFrameReader = spark.read().format("iceberg");
    if (tableName.contains("/")) {
      // Hadoop Table or Metadata location passed, load without a catalog
      return dataFrameReader.load(tableName + "#" + type);
    }

    // Try DSV2 catalog based name based resolution
    if (spark.version().startsWith("3")) {
      Dataset<Row> catalogMetadataTable = loadCatalogMetadataTable(tableName, type);
      if (catalogMetadataTable != null) {
        return catalogMetadataTable;
      }
    }

    // Catalog based resolution failed, our catalog may be a non-DatasourceV2 Catalog
    if (tableName.startsWith("hadoop.")) {
      // Try loading by location as Hadoop table without Catalog
      return dataFrameReader.load(tableLocation + "#" + type);
    } else if (tableName.startsWith("hive")) {
      // Try loading by name as a Hive table without Catalog
      return dataFrameReader.load(tableName.replaceFirst("hive\\.", "") + "." + type);
    } else {
      throw new IllegalArgumentException(String.format(
          "Cannot find the metadata table for %s of type %s", tableName, type));
    }
  }

  private static class ReadManifest implements FlatMapFunction<ManifestFileBean, String> {
    private final Broadcast<FileIO> io;

    ReadManifest(Broadcast<FileIO> io) {
      this.io = io;
    }

    @Override
    public Iterator<String> call(ManifestFileBean manifest) {
      switch (manifest.content()) {
        case DATA:
          return new ClosingIterator<>(ManifestFiles.readPaths(manifest, io.getValue()).iterator());
        case DELETES:
          return new ClosingIterator<>(ManifestFiles.readDeleteFiles(manifest, io.getValue()).iterator());
      }
      throw new UnsupportedOperationException("Cannot read unknown manifest type: " + manifest.content());
    }
  }
}
