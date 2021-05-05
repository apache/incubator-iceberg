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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.BaseRemoveOrphanFilesActionResult;
import org.apache.iceberg.actions.RemoveOrphanFiles;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

/**
 * An action that removes orphan metadata and data files by listing a given location and comparing
 * the actual files in that location with data and metadata files referenced by all valid snapshots.
 * The location must be accessible for listing via the Hadoop {@link FileSystem}.
 * <p>
 * By default, this action cleans up the table location returned by {@link Table#location()} and
 * removes unreachable files that are older than 3 days using {@link Table#io()}. The behavior can be modified
 * by passing a custom location to {@link #location} and a custom timestamp to {@link #olderThan(long)}.
 * For example, someone might point this action to the data folder to clean up only orphan data files.
 * In addition, there is a way to configure an alternative delete method via {@link #deleteWith(Consumer)}.
 * <p>
 * <em>Note:</em> It is dangerous to call this action with a short retention interval as it might corrupt
 * the state of the table if another operation is writing at the same time.
 */
public class BaseRemoveOrphanFilesSparkAction
    extends BaseSparkAction<RemoveOrphanFiles, RemoveOrphanFiles.Result> implements RemoveOrphanFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRemoveOrphanFilesSparkAction.class);
  private static final UserDefinedFunction filenameUDF = functions.udf((String path) -> {
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex == -1) {
      return path;
    } else {
      return path.substring(lastIndex + 1);
    }
  }, DataTypes.StringType);

  private final SerializableConfiguration hadoopConf;
  private final int partitionDiscoveryParallelism;
  private final Table table;
  private final TableOperations ops;

  private String location = null;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc = new Consumer<String>() {
    @Override
    public void accept(String file) {
      table.io().deleteFile(file);
    }
  };

  public BaseRemoveOrphanFilesSparkAction(SparkSession spark, Table table) {
    super(spark);

    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    this.partitionDiscoveryParallelism = spark.sessionState().conf().parallelPartitionDiscoveryParallelism();
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
    this.location = table.location();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot remove orphan files: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected RemoveOrphanFiles self() {
    return this;
  }

  @Override
  public BaseRemoveOrphanFilesSparkAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public BaseRemoveOrphanFilesSparkAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  @Override
  public BaseRemoveOrphanFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public RemoveOrphanFiles.Result execute() {
    JobGroupInfo info = newJobGroupInfo("REMOVE-ORPHAN-FILES", "REMOVE-ORPHAN-FILES");
    return withJobGroupInfo(info, this::doExecute);
  }

  private RemoveOrphanFiles.Result doExecute() {
    Dataset<Row> validDataFileDF = buildValidDataFileDF(table);
    Dataset<Row> validDeleteFileDF = buildValidDeleteFileDF(table);
    Dataset<Row> validMetadataFileDF = buildValidMetadataFileDF(table, ops);
    Dataset<Row> validFileDF = validDataFileDF.union(validDeleteFileDF).union(validMetadataFileDF);
    Dataset<Row> actualFileDF = buildActualFileDF();

    Column actualFileName = filenameUDF.apply(actualFileDF.col("file_path"));
    Column validFileName = filenameUDF.apply(validFileDF.col("file_path"));
    Column nameEqual = actualFileName.equalTo(validFileName);
    Column actualContains = actualFileDF.col("file_path").contains(validFileDF.col("file_path"));
    Column joinCond = nameEqual.and(actualContains);
    List<String> orphanFiles = actualFileDF.join(validFileDF, joinCond, "leftanti")
        .as(Encoders.STRING())
        .collectAsList();

    Tasks.foreach(orphanFiles)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc))
        .run(deleteFunc::accept);

    return new BaseRemoveOrphanFilesActionResult(orphanFiles);
  }

  private Dataset<Row> buildActualFileDF() {
    List<String> subDirs = Lists.newArrayList();
    List<String> matchingFiles = Lists.newArrayList();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

    // list at most 3 levels and only dirs that have less than 10 direct sub dirs on the driver
    listDirRecursively(location, predicate, hadoopConf.value(), 3, 10, subDirs, matchingFiles);

    JavaRDD<String> matchingFileRDD = sparkContext().parallelize(matchingFiles, 1);

    if (subDirs.isEmpty()) {
      return spark().createDataset(matchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
    }

    int parallelism = Math.min(subDirs.size(), partitionDiscoveryParallelism);
    JavaRDD<String> subDirRDD = sparkContext().parallelize(subDirs, parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext().broadcast(hadoopConf);
    JavaRDD<String> matchingLeafFileRDD = subDirRDD.mapPartitions(listDirsRecursively(conf, olderThanTimestamp));

    JavaRDD<String> completeMatchingFileRDD = matchingFileRDD.union(matchingLeafFileRDD);
    return spark().createDataset(completeMatchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
  }

  private static void listDirRecursively(
      String dir, Predicate<FileStatus> predicate, Configuration conf, int maxDepth,
      int maxDirectSubDirs, List<String> remainingSubDirs, List<String> matchingFiles) {

    // stop listing whenever we reach the max depth
    if (maxDepth <= 0) {
      remainingSubDirs.add(dir);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);

      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
        if (file.isDirectory()) {
          subDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          matchingFiles.add(file.getPath().toString());
        }
      }

      // stop listing if the number of direct sub dirs is bigger than maxDirectSubDirs
      if (subDirs.size() > maxDirectSubDirs) {
        remainingSubDirs.addAll(subDirs);
        return;
      }

      for (String subDir : subDirs) {
        listDirRecursively(subDir, predicate, conf, maxDepth - 1, maxDirectSubDirs, remainingSubDirs, matchingFiles);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static FlatMapFunction<Iterator<String>, String> listDirsRecursively(
      Broadcast<SerializableConfiguration> conf,
      long olderThanTimestamp) {

    return dirs -> {
      List<String> subDirs = Lists.newArrayList();
      List<String> files = Lists.newArrayList();

      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

      int maxDepth = 2000;
      int maxDirectSubDirs = Integer.MAX_VALUE;

      dirs.forEachRemaining(dir -> {
        listDirRecursively(dir, predicate, conf.value().value(), maxDepth, maxDirectSubDirs, subDirs, files);
      });

      if (!subDirs.isEmpty()) {
        throw new RuntimeException("Could not list subdirectories, reached maximum subdirectory depth: " + maxDepth);
      }

      return files.iterator();
    };
  }
}
