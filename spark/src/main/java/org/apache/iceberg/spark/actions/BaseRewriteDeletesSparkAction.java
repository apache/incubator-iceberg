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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDeleteActionResult;
import org.apache.iceberg.actions.RewriteDeletes;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.EqualityDeleteRewriter;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRewriteDeletesSparkAction extends BaseSparkAction<RewriteDeletes, RewriteDeletes.Result>
    implements RewriteDeletes {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDeletesSparkAction.class);
  private final Table table;
  private final JavaSparkContext sparkContext;
  private FileIO fileIO;
  private final EncryptionManager encryptionManager;
  private final boolean caseSensitive;
  private final PartitionSpec spec;
  private final long targetSizeInBytes;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private boolean rewriteEqualityDelete;

  public BaseRewriteDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.fileIO = fileIO();
    this.encryptionManager = table.encryption();
    this.caseSensitive = false;
    this.spec = table.spec();

    long splitSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    long targetFileSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetSizeInBytes = Math.min(splitSize, targetFileSize);

    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    this.splitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  protected FileIO fileIO() {
    if (this.fileIO == null) {
      this.fileIO = SparkUtil.serializableFileIO(table);
    }
    return this.fileIO;
  }

  @Override
  public Result execute() {
    CloseableIterable<FileScanTask> fileScanTasks = null;
    try {
      fileScanTasks = table.newScan()
          .caseSensitive(caseSensitive)
          .ignoreResiduals()
          .planFiles();
    } finally {
      try {
        if (fileScanTasks != null) {
          fileScanTasks.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to close task iterable", ioe);
      }
    }

    if (!rewriteEqualityDelete) {
      LOG.warn("Only supports rewrite equality deletes currently");
      return new RewriteDeleteActionResult(Collections.emptyList(), Collections.emptyList());
    }

    CloseableIterable<FileScanTask> tasksWithEqDelete = CloseableIterable.filter(fileScanTasks, scan ->
        scan.deletes().stream().anyMatch(delete -> delete.content().equals(FileContent.EQUALITY_DELETES))
    );

    Set<DeleteFile> eqDeletes = Sets.newHashSet();
    tasksWithEqDelete.forEach(task -> {
      eqDeletes.addAll(task.deletes().stream()
          .filter(deleteFile -> deleteFile.content().equals(FileContent.EQUALITY_DELETES))
          .collect(Collectors.toList()));
    });

    Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks =
        TableScanUtil.groupTasksByPartition(spec, tasksWithEqDelete.iterator());

    // Split and combine tasks under each partition
    List<Pair<StructLike, CombinedScanTask>> combinedScanTasks = groupedTasks.entrySet().stream()
        .map(entry -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(entry.getValue()), targetSizeInBytes);
          return Pair.of(entry.getKey().get(),
              TableScanUtil.planTasks(splitTasks, targetSizeInBytes, splitLookback, splitOpenFileCost));
        })
        .flatMap(pair -> StreamSupport.stream(CloseableIterable
            .transform(pair.second(), task -> Pair.of(pair.first(), task)).spliterator(), false)
        )
        .collect(Collectors.toList());

    if (!combinedScanTasks.isEmpty()) {
      JavaRDD<Pair<StructLike, CombinedScanTask>> taskRDD = sparkContext.parallelize(combinedScanTasks,
          combinedScanTasks.size());
      Broadcast<FileIO> io = sparkContext.broadcast(fileIO());
      Broadcast<EncryptionManager> encryption = sparkContext.broadcast(encryptionManager());

      EqualityDeleteRewriter deleteRewriter = new EqualityDeleteRewriter(table, caseSensitive, io, encryption);
      List<DeleteFile> posDeletes = deleteRewriter.toPosDeletes(taskRDD);

      if (!eqDeletes.isEmpty()) {
        rewriteEqualityDeletes(Lists.newArrayList(eqDeletes), posDeletes);
        return new RewriteDeleteActionResult(Lists.newArrayList(eqDeletes), posDeletes);
      }
    }

    return new RewriteDeleteActionResult(Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public RewriteDeletes rewriteEqualityDeletes() {
    this.rewriteEqualityDelete = true;
    return this;
  }

  @Override
  protected RewriteDeletes self() {
    return this;
  }

  protected EncryptionManager encryptionManager() {
    return encryptionManager;
  }

  private void rewriteEqualityDeletes(List<DeleteFile> eqDeletes, List<DeleteFile> posDeletes) {
    Preconditions.checkArgument(eqDeletes.stream().allMatch(f -> f.content().equals(FileContent.EQUALITY_DELETES)),
        "The deletes to be converted should be equality deletes");
    Preconditions.checkArgument(posDeletes.stream().allMatch(f -> f.content().equals(FileContent.POSITION_DELETES)),
        "The converted deletes should be position deletes");
    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.rewriteFiles(ImmutableSet.of(), Sets.newHashSet(eqDeletes), ImmutableSet.of(),
              Sets.newHashSet(posDeletes));
      rewriteFiles.commit();
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(posDeletes, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
      throw e;
    }
  }
}
