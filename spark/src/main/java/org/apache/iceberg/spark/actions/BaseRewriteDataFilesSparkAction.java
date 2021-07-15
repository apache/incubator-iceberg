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
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDataFilesFileGroupInfo;
import org.apache.iceberg.actions.BaseRewriteDataFilesResult;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.actions.SortStrategy;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRewriteDataFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDataFiles, RewriteDataFiles.Result> implements RewriteDataFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDataFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS = ImmutableSet.of(
      MAX_CONCURRENT_FILE_GROUP_REWRITES,
      MAX_FILE_GROUP_SIZE_BYTES,
      PARTIAL_PROGRESS_ENABLED,
      PARTIAL_PROGRESS_MAX_COMMITS,
      TARGET_FILE_SIZE_BYTES
  );

  private final Table table;

  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private boolean partialProgressEnabled;
  private RewriteStrategy strategy;

  protected BaseRewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.strategy = binPackStrategy();
  }

  protected Table table() {
    return table;
  }

  /**
   * The framework specific {@link BinPackStrategy}
   */
  protected abstract BinPackStrategy binPackStrategy();

  /**
   * The framework specific {@link SortStrategy}
   */
  protected abstract SortStrategy sortStrategy();

  @Override
  public RewriteDataFiles binPack() {
    this.strategy = binPackStrategy();
    return this;
  }

  @Override
  public RewriteDataFiles sort(SortOrder sortOrder) {
    this.strategy = sortStrategy().sortOrder(sortOrder);
    return this;
  }

  @Override
  public RewriteDataFiles sort() {
    this.strategy = sortStrategy();
    return this;
  }

  @Override
  public RewriteDataFiles filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public RewriteDataFiles.Result execute() {
    validateAndInitOptions();
    strategy = strategy.options(options());

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);
    Stream<RewriteFileGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return new BaseRewriteDataFilesResult(Collections.emptyList());
    }

    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(ctx, groupStream);
    } else {
      return doExecute(ctx, groupStream);
    }
  }

  private Map<StructLike, List<List<FileScanTask>>> planFileGroups() {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .filter(filter)
        .ignoreResiduals()
        .planFiles();

    try {
      Map<StructLike, List<FileScanTask>> filesByPartition = Streams.stream(fileScanTasks)
          .collect(Collectors.groupingBy(task -> {
            if (task.file().specId() == table.spec().specId()) {
              return task.file().partition();
            } else {
              return EmptyStruct.instance();
            }
          }));

      Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = Maps.newHashMap();

      filesByPartition.forEach((partition, tasks) -> {
        Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(tasks);
        Iterable<List<FileScanTask>> groupedTasks = strategy.planFileGroups(filtered);
        List<List<FileScanTask>> fileGroups = ImmutableList.copyOf(groupedTasks);
        if (fileGroups.size() > 0) {
          fileGroupsByPartition.put(partition, fileGroups);
        }
      });

      return fileGroupsByPartition;
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  @VisibleForTesting
  RewriteFileGroup rewriteFiles(RewriteExecutionContext ctx, RewriteFileGroup fileGroup) {
    String desc = jobDesc(fileGroup, ctx);
    Set<DataFile> addedFiles = withJobGroupInfo(
        newJobGroupInfo("REWRITE-DATA-FILES", desc),
        () -> strategy.rewriteFiles(fileGroup.fileScans()));

    fileGroup.setOutputFiles(addedFiles);
    LOG.info("Rewrite Files Ready to be Committed - {}", desc);
    return fileGroup;
  }

  private ExecutorService rewriteService() {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(
            maxConcurrentFileGroupRewrites,
            new ThreadFactoryBuilder()
                .setNameFormat("Rewrite-Service-%d")
                .build()));
  }

  @VisibleForTesting
  RewriteDataFilesCommitManager commitManager() {
    return new RewriteDataFilesCommitManager(table);
  }

  private Result doExecute(RewriteExecutionContext ctx, Stream<RewriteFileGroup> groupStream) {
    ExecutorService rewriteService = rewriteService();
    RewriteDataFilesCommitManager commitManager = commitManager();

    ConcurrentLinkedQueue<RewriteFileGroup> rewrittenGroups = Queues.newConcurrentLinkedQueue();

    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder = Tasks.foreach(groupStream)
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .onFailure((fileGroup, exception) -> {
          LOG.warn("Failure during rewrite process for group {}", fileGroup.info(), exception);
        });

    try {
      rewriteTaskBuilder.run(fileGroup -> {
        rewrittenGroups.add(rewriteFiles(ctx, fileGroup));
      });
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error("Cannot complete rewrite, {} is not enabled and one of the file set groups failed to " +
          "be rewritten. This error occurred during the writing of new files, not during the commit process. This " +
          "indicates something is wrong that doesn't involve conflicts with other Iceberg operations. Enabling " +
          "{} may help in this case but the root cause should be investigated. Cleaning up {} groups which finished " +
          "being written.", PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED, rewrittenGroups.size(), e);

      Tasks.foreach(rewrittenGroups)
          .suppressFailureWhenFinished()
          .run(group -> commitManager.abortFileGroup(group));
      throw e;
    } finally {
      rewriteService.shutdown();
    }

    try {
      commitManager.commitOrClean(Sets.newHashSet(rewrittenGroups));
    } catch (ValidationException | CommitFailedException e) {
      String errorMessage = String.format(
          "Cannot commit rewrite because of a ValidationException or CommitFailedException. This usually means that " +
              "this rewrite has conflicted with another concurrent Iceberg operation. To reduce the likelihood of " +
              "conflicts, set %s which will break up the rewrite into multiple smaller commits controlled by %s. " +
              "Separate smaller rewrite commits can succeed independently while any commits that conflict with " +
              "another Iceberg operation will be ignored. This mode will create additional snapshots in the table " +
              "history, one for each commit.",
          PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
      throw new RuntimeException(errorMessage, e);
    }

    List<FileGroupRewriteResult> rewriteResults = rewrittenGroups.stream()
        .map(RewriteFileGroup::asResult)
        .collect(Collectors.toList());
    return new BaseRewriteDataFilesResult(rewriteResults);
  }

  private Result doExecuteWithPartialProgress(RewriteExecutionContext ctx, Stream<RewriteFileGroup> groupStream) {
    ExecutorService rewriteService = rewriteService();

    // Start Commit Service
    int groupsPerCommit = IntMath.divide(ctx.totalGroupCount(), maxCommits, RoundingMode.CEILING);
    RewriteDataFilesCommitManager.CommitService commitService = commitManager().service(groupsPerCommit);
    commitService.start();

    // Start rewrite tasks
    Tasks.foreach(groupStream)
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure((fileGroup, exception) -> LOG.error("Failure during rewrite group {}", fileGroup.info(), exception))
        .run(fileGroup -> commitService.offer(rewriteFiles(ctx, fileGroup)));
    rewriteService.shutdown();

    // Stop Commit service
    commitService.close();
    List<RewriteFileGroup> commitResults = commitService.results();
    if (commitResults.size() == 0) {
      LOG.error("{} is true but no rewrite commits succeeded. Check the logs to determine why the individual " +
          "commits failed. If this is persistent it may help to increase {} which will break the rewrite operation " +
          "into smaller commits.", PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
    }

    List<FileGroupRewriteResult> rewriteResults = commitResults.stream()
        .map(RewriteFileGroup::asResult)
        .collect(Collectors.toList());
    return new BaseRewriteDataFilesResult(rewriteResults);
  }

  private Stream<RewriteFileGroup> toGroupStream(RewriteExecutionContext ctx,
                                                 Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {

    // Todo Add intelligence to the order in which we do rewrites instead of just using partition order
    return fileGroupsByPartition.entrySet().stream()
        .flatMap(e -> {
          StructLike partition = e.getKey();
          List<List<FileScanTask>> fileGroups = e.getValue();
          return fileGroups.stream().map(tasks -> {
            int globalIndex = ctx.currentGlobalIndex();
            int partitionIndex = ctx.currentPartitionIndex(partition);
            FileGroupInfo info = new BaseRewriteDataFilesFileGroupInfo(globalIndex, partitionIndex, partition);
            return new RewriteFileGroup(info, tasks);
          });
        });
  }

  private void validateAndInitOptions() {
    Set<String> validOptions = Sets.newHashSet(strategy.validOptions());
    validOptions.addAll(VALID_OPTIONS);

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by the action or the strategy %s",
        invalidKeys, strategy.name());

    maxConcurrentFileGroupRewrites = PropertyUtil.propertyAsInt(options(),
        MAX_CONCURRENT_FILE_GROUP_REWRITES,
        MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT);

    maxCommits = PropertyUtil.propertyAsInt(options(),
        PARTIAL_PROGRESS_MAX_COMMITS,
        PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    partialProgressEnabled = PropertyUtil.propertyAsBoolean(options(),
        PARTIAL_PROGRESS_ENABLED,
        PARTIAL_PROGRESS_ENABLED_DEFAULT);

    Preconditions.checkArgument(maxConcurrentFileGroupRewrites >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_CONCURRENT_FILE_GROUP_REWRITES, maxConcurrentFileGroupRewrites);

    Preconditions.checkArgument(!partialProgressEnabled || partialProgressEnabled && maxCommits > 0,
        "Cannot set %s to %s, the value must be positive when %s is true",
        PARTIAL_PROGRESS_MAX_COMMITS, maxCommits, PARTIAL_PROGRESS_ENABLED);
  }

  private String jobDesc(RewriteFileGroup group, RewriteExecutionContext ctx) {
    StructLike partition = group.info().partition();
    if (partition.size() > 0) {
      return String.format("Rewriting %d files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.rewrittenFiles().size(),
          strategy.name(), group.info().globalIndex(),
          ctx.totalGroupCount(), partition, group.info().partitionIndex(), ctx.groupsInPartition(partition),
          table.name());
    } else {
      return String.format("Rewriting %d files (%s, file group %d/%d) in %s",
          group.rewrittenFiles().size(),
          strategy.name(), group.info().globalIndex(), ctx.totalGroupCount(),
          table.name());
    }
  }

  @VisibleForTesting
  static class RewriteExecutionContext {
    private final Map<StructLike, Integer> numGroupsByPartition;
    private final int totalGroupCount;
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    RewriteExecutionContext(Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {
      this.numGroupsByPartition = fileGroupsByPartition.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      this.totalGroupCount = numGroupsByPartition.values().stream()
          .reduce(Integer::sum)
          .orElse(0);
      this.partitionIndexMap = Maps.newConcurrentMap();
      this.groupIndex = new AtomicInteger(1);
    }

    public int currentGlobalIndex() {
      return groupIndex.getAndIncrement();
    }

    public int currentPartitionIndex(StructLike partition) {
      return partitionIndexMap.merge(partition, 1, Integer::sum);
    }

    public int groupsInPartition(StructLike partition) {
      return numGroupsByPartition.get(partition);
    }

    public int totalGroupCount() {
      return totalGroupCount;
    }
  }

  static class EmptyStruct implements StructLike {

    private static EmptyStruct instance;

    static EmptyStruct instance() {
      if (instance == null) {
        instance = new EmptyStruct();
      }
      return instance;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return null;
    }

    @Override
    public <T> void set(int pos, T value) {

    }
  }
}
