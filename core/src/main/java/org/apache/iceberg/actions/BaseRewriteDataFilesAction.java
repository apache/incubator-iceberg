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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseRewriteDataFilesAction<ThisT>
    extends BaseSnapshotUpdateAction<ThisT, RewriteDataFilesActionResult> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDataFilesAction.class);

  private final Table table;
  private final FileIO fileIO;
  private final EncryptionManager encryptionManager;
  private boolean caseSensitive;
  private PartitionSpec spec;
  private Expression filter;
  private long targetSizeInBytes;
  private int splitLookback;
  private long splitOpenFileCost;

  protected BaseRewriteDataFilesAction(Table table) {
    this.table = table;
    this.spec = table.spec();
    this.filter = Expressions.alwaysTrue();
    this.caseSensitive = false;

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

    this.fileIO = fileIO();
    this.encryptionManager = table.encryption();
  }

  @Override
  protected Table table() {
    return table;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected EncryptionManager encryptionManager() {
    return encryptionManager;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  /**
   * Is it case sensitive
   *
   * @param newCaseSensitive caseSensitive
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  /**
   * Pass a PartitionSpec id to specify which PartitionSpec should be used in DataFile rewrite
   *
   * @param specId PartitionSpec id to rewrite
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> outputSpecId(int specId) {
    Preconditions.checkArgument(table.specs().containsKey(specId), "Invalid spec id %d", specId);
    this.spec = table.specs().get(specId);
    return this;
  }

  /**
   * Specify the target rewrite data file size in bytes
   *
   * @param targetSize size in bytes of rewrite data file
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> targetSizeInBytes(long targetSize) {
    Preconditions.checkArgument(targetSize > 0L, "Invalid target rewrite data file size in bytes %d",
        targetSize);
    this.targetSizeInBytes = targetSize;
    return this;
  }

  /**
   * Specify the number of "bins" considered when trying to pack the next file split into a task. Increasing this
   * usually makes tasks a bit more even by considering more ways to pack file regions into a single task with extra
   * planning cost.
   * <p>
   * This configuration can reorder the incoming file regions, to preserve order for lower/upper bounds in file
   * metadata, user can use a lookback of 1.
   *
   * @param lookback number of "bins" considered when trying to pack the next file split into a task.
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> splitLookback(int lookback) {
    Preconditions.checkArgument(lookback > 0L, "Invalid split lookback %d", lookback);
    this.splitLookback = lookback;
    return this;
  }

  /**
   * Specify the minimum file size to count to pack into one "bin". If the read file size is smaller than this specified
   * threshold, Iceberg will use this value to do count.
   * <p>
   * this configuration controls the number of files to compact for each task, small value would lead to a high
   * compaction, the default value is 4MB.
   *
   * @param openFileCost minimum file size to count to pack into one "bin".
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> splitOpenFileCost(long openFileCost) {
    Preconditions.checkArgument(openFileCost > 0L, "Invalid split openFileCost %d", openFileCost);
    this.splitOpenFileCost = openFileCost;
    return this;
  }

  /**
   * Pass a row Expression to filter DataFiles to be rewritten. Note that all files that may contain data matching the
   * filter may be rewritten.
   *
   * @param expr Expression to filter out DataFiles
   * @return this for method chaining
   */
  public BaseRewriteDataFilesAction<ThisT> filter(Expression expr) {
    this.filter = Expressions.and(filter, expr);
    return this;
  }

  @Override
  public RewriteDataFilesActionResult execute() {
    CloseableIterable<FileScanTask> fileScanTasks = null;
    try {
      fileScanTasks = table.newScan()
          .caseSensitive(caseSensitive)
          .ignoreResiduals()
          .filter(filter)
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

    Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks = groupTasksByPartition(fileScanTasks.iterator());
    Map<StructLikeWrapper, Collection<FileScanTask>> filteredGroupedTasks = groupedTasks.entrySet().stream()
        .filter(kv -> kv.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Nothing to rewrite if there's only one DataFile in each partition.
    if (filteredGroupedTasks.isEmpty()) {
      return RewriteDataFilesActionResult.empty();
    }
    // Split and combine tasks under each partition
    List<CombinedScanTask> combinedScanTasks = filteredGroupedTasks.values().stream()
        .map(scanTasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(scanTasks), targetSizeInBytes);
          return TableScanUtil.planTasks(splitTasks, targetSizeInBytes, splitLookback, splitOpenFileCost);
        })
        .flatMap(Streams::stream)
        .filter(task -> task.files().size() > 1 || isPartialFileScan(task))
        .collect(Collectors.toList());

    if (combinedScanTasks.isEmpty()) {
      return RewriteDataFilesActionResult.empty();
    }

    List<DataFile> addedDataFiles = rewriteDataForTasks(combinedScanTasks);
    List<DataFile> currentDataFiles = combinedScanTasks.stream()
        .flatMap(tasks -> tasks.files().stream().map(FileScanTask::file))
        .collect(Collectors.toList());
    replaceDataFiles(currentDataFiles, addedDataFiles);

    return new RewriteDataFilesActionResult(currentDataFiles, addedDataFiles);
  }


  private Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(
      CloseableIterator<FileScanTask> tasksIter) {
    ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition = Multimaps.newListMultimap(
        Maps.newHashMap(), Lists::newArrayList);
    try (CloseableIterator<FileScanTask> iterator = tasksIter) {
      iterator.forEachRemaining(task -> {
        StructLikeWrapper structLike = StructLikeWrapper.forType(spec.partitionType()).set(task.file().partition());
        tasksGroupedByPartition.put(structLike, task);
      });
    } catch (IOException e) {
      LOG.warn("Failed to close task iterator", e);
    }
    return tasksGroupedByPartition.asMap();
  }

  @VisibleForTesting
  void replaceDataFiles(Iterable<DataFile> deletedDataFiles, Iterable<DataFile> addedDataFiles) {
    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.rewriteFiles(Sets.newHashSet(deletedDataFiles), Sets.newHashSet(addedDataFiles));
      commit(rewriteFiles);
    } catch (CommitStateUnknownException e) {
      LOG.warn("Failed to check commit status: skipping data file deletion.", e);
      throw e;
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(addedDataFiles, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
      throw e;
    }
  }

  private boolean isPartialFileScan(CombinedScanTask task) {
    if (task.files().size() == 1) {
      FileScanTask fileScanTask = task.files().iterator().next();
      return fileScanTask.file().fileSizeInBytes() != fileScanTask.length();
    } else {
      return false;
    }
  }

  protected abstract FileIO fileIO();

  protected abstract List<DataFile> rewriteDataForTasks(List<CombinedScanTask> combinedScanTask);
}
