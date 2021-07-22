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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public abstract class TestNewRewriteDataFilesAction extends SparkTestBase {

  protected abstract ActionsProvider actions();
  protected abstract Set<String> cacheContents(Table table);

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  private RewriteDataFiles basicRewrite(Table table) {
    // Always compact regardless of input files
    table.refresh();
    return actions().rewriteDataFiles(table).option(BinPackStrategy.MIN_INPUT_FILES, "1");
  }

  @Test
  public void testEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    basicRewrite(table).execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testBinPackUnpartitionedTable() {
    Table table = createTable(4);
    shouldHaveFiles(table, 4);
    List<Object[]> expectedRecords = currentData();

    Result result = basicRewrite(table).execute();
    Assert.assertEquals("Action should rewrite 4 data files", 4, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    shouldHaveFiles(table, 1);
    List<Object[]> actual = currentData();

    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  public void testBinPackPartitionedTable() {
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();

    Result result = basicRewrite(table).execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFilesCount());

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();

    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackWithFilter() {
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();

    Result result = basicRewrite(table)
        .filter(Expressions.equal("c1", 1))
        .filter(Expressions.startsWith("c2", "foo"))
        .execute();

    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    shouldHaveFiles(table, 7);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteLargeTableHasResiduals() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "100");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // all records belong to the same partition
    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i % 4)));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);

    List<Object[]> expectedRecords = currentData();

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan()
        .ignoreResiduals()
        .filter(Expressions.equal("c3", "0"))
        .planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }

    shouldHaveFiles(table, 2);

    Result result = basicRewrite(table)
        .filter(Expressions.equal("c3", "0"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    List<Object[]> actualRecords = currentData();

    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackSplitLargeFile() {
    Table table = createTable(1);
    shouldHaveFiles(table, 1);

    List<Object[]> expectedRecords = currentData();
    long targetSize = testDataSize(table) / 2;

    Result result = basicRewrite(table)
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(targetSize))
        .execute();

    Assert.assertEquals("Action should delete 1 data files", 1, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 2 data files", 2, result.addedDataFilesCount());

    shouldHaveFiles(table, 2);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackCombineMixedFiles() {
    // One file too big
    Table table = createTable(1);
    shouldHaveFiles(table, 1);

    // Two files too small
    writeRecords(1, 100);
    writeRecords(1, 100);
    shouldHaveFiles(table, 3);

    List<Object[]> expectedRecords = currentData();

    int targetSize = averageFileSize(table);

    Result result = basicRewrite(table)
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(targetSize))
        .option(BinPackStrategy.MAX_FILE_SIZE_BYTES, Integer.toString(targetSize + 100))
        .option(BinPackStrategy.MIN_FILE_SIZE_BYTES, Integer.toString(targetSize - 100))
        .execute();

    Assert.assertEquals("Action should delete 3 data files", 3, result.rewrittenDataFilesCount());
    // Should Split the big files into 3 pieces, one of which should be combined with the two smaller files
    Assert.assertEquals("Action should add 3 data files", 3, result.addedDataFilesCount());

    shouldHaveFiles(table, 3);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testPartialProgressEnabled() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "10")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);

    table.refresh();

    shouldHaveSnapshots(table, 11);
    shouldHaveACleanCache(table);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);
  }

  @Test
  public void testMultipleGroups() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .option(BinPackStrategy.MIN_INPUT_FILES, "1")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testPartialProgressMaxCommits() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 4);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100));

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    AssertHelpers.assertThrows("Should fail entire rewrite if part fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testSingleCommitWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100));

    BaseRewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    // Fail to commit
    doThrow(new RuntimeException("Commit Failure"))
        .when(util)
        .commitFileGroups(any());

    doReturn(util)
        .when(spyRewrite)
        .commitManager();

    AssertHelpers.assertThrows("Should fail entire rewrite if commit fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    AssertHelpers.assertThrows("Should fail entire rewrite if part fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.rewriteResults().size(), 7);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.rewriteResults().size(), 7);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelPartialProgressWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            basicRewrite(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    // First and Third commits work, second does not
    doCallRealMethod()
        .doThrow(new RuntimeException("Commit Failed"))
        .doCallRealMethod()
        .when(util)
        .commitFileGroups(any());

    doReturn(util)
        .when(spyRewrite)
        .commitManager();

    RewriteDataFiles.Result result = spyRewrite.execute();

    // Commit 1: 4/4 + Commit 2 failed 0/4 + Commit 3: 2/2 == 6 out of 10 total groups comitted
    Assert.assertEquals("Should have 6 fileGroups", 6, result.rewriteResults().size());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // Only 2 new commits because we broke one
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testInvalidOptions() {
    Table table = createTable(20);

    AssertHelpers.assertThrows("No negative values for partial progress max commits",
        IllegalArgumentException.class,
        () -> basicRewrite(table)
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "-5")
            .execute());

    AssertHelpers.assertThrows("No negative values for max concurrent groups",
        IllegalArgumentException.class,
        () -> basicRewrite(table)
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "-5")
            .execute());

    AssertHelpers.assertThrows("No unknown options allowed",
        IllegalArgumentException.class,
        () -> basicRewrite(table)
            .option("foobarity", "-5")
            .execute());
  }

  @Test
  public void testCommitStateUnknownException() {
    Table table = createTable(20);
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction action = (BaseRewriteDataFilesSparkAction) basicRewrite(table);
    BaseRewriteDataFilesSparkAction spyAction = spy(action);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    doAnswer(invocationOnMock -> {
      invocationOnMock.callRealMethod();
      throw new CommitStateUnknownException(new RuntimeException("Unknown State"));
    }).when(util).commitFileGroups(any());

    doReturn(util)
        .when(spyAction)
        .commitManager();

    AssertHelpers.assertThrows("Should propagate CommitStateUnknown Exception",
        CommitStateUnknownException.class, () -> spyAction.execute());

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2); // Commit actually Succeeded
  }

  protected List<Object[]> currentData() {
    return rowsToJava(spark.read().format("iceberg").load(tableLocation)
        .sort("c1", "c2", "c3")
        .collectAsList());
  }

  protected long testDataSize(Table table) {
    return Streams.stream(table.newScan().planFiles()).mapToLong(FileScanTask::length).sum();
  }

  protected void shouldHaveFiles(Table table, int numExpected) {
    table.refresh();
    int numFiles = Iterables.size(table.newScan().planFiles());
    Assert.assertEquals("Did not have the expected number of files", numExpected, numFiles);
  }

  protected void shouldHaveSnapshots(Table table, int expectedSnapshots) {
    table.refresh();
    int actualSnapshots = Iterables.size(table.snapshots());
    Assert.assertEquals("Table did not have the expected number of snapshots",
        expectedSnapshots, actualSnapshots);
  }

  protected void shouldHaveNoOrphans(Table table) {
    Assert.assertEquals("Should not have found any orphan files", ImmutableList.of(),
        actions().deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis())
            .execute()
            .orphanFileLocations());
  }

  protected void shouldHaveACleanCache(Table table) {
    Assert.assertEquals("Should not have any entries in cache", ImmutableSet.of(),
        cacheContents(table));
  }

  protected <T> void shouldHaveLastCommitSorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>>
        overlappingFiles = getOverlappingFiles(table, column);

    Assert.assertEquals("Found overlapping files", Collections.emptyList(), overlappingFiles);
  }

  protected <T> void shouldHaveLastCommitUnsorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>>
        overlappingFiles = getOverlappingFiles(table, column);

    Assert.assertNotEquals("Found overlapping files", Collections.emptyList(), overlappingFiles);
  }

  private <T> List<Pair<Pair<T, T>, Pair<T, T>>> getOverlappingFiles(Table table, String column) {
    table.refresh();
    NestedField field = table.schema().caseInsensitiveFindField(column);
    int columnId = field.fieldId();
    Class<T> javaClass = (Class<T>) field.type().typeId().javaClass();
    List<Pair<T, T>> columnBounds =
        Streams.stream(table.currentSnapshot().addedFiles())
            .map(file -> Pair.of(
                javaClass.cast(Conversions.fromByteBuffer(field.type(), file.lowerBounds().get(columnId))),
                javaClass.cast(Conversions.fromByteBuffer(field.type(), file.upperBounds().get(columnId)))))
            .collect(Collectors.toList());

    Comparator<T> comparator = Comparators.forType(field.type().asPrimitiveType());

    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = columnBounds.stream()
        .flatMap(left -> columnBounds.stream().map(right -> Pair.of(left, right)))
        .filter(filePair -> {
          Pair<T, T> left = filePair.first();
          T leftLower = left.first();
          T leftUpper = left.second();
          Pair<T, T> right = filePair.second();
          T rightLower = right.first();
          T rightUpper = right.second();
          boolean boundsOverlap =
              comparator.compare(leftUpper, rightLower) > 0 && comparator.compare(leftUpper, rightUpper) < 0 ||
                  comparator.compare(leftLower, rightLower) > 0 && comparator.compare(leftLower, rightUpper) < 0;

          return left != right && boundsOverlap;
        })
        .collect(Collectors.toList());
    return overlappingFiles;
  }

  /**
   * Create a table with a certain number of files, returns the size of a file
   * @param files number of files to create
   * @return size of a file
   */
  protected Table createTable(int files) {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    table.updateProperties().set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1024").commit();
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    writeRecords(files, 2000);

    return table;
  }

  protected Table createTablePartitioned(int partitions, int files) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    writeRecords(files, 2000, partitions);
    return table;
  }

  protected int averageFileSize(Table table) {
    table.refresh();
    return (int) Streams.stream(table.newScan().planFiles()).mapToLong(FileScanTask::length).average().getAsDouble();
  }

  private void writeRecords(int files, int numRecords) {
    writeRecords(files, numRecords, 0);
  }

  private void writeRecords(int files, int numRecords, int partitions) {
    List<ThreeColumnRecord> records = Lists.newArrayList();
    List<Integer> data = IntStream.range(0, numRecords).boxed().collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    if (partitions > 0) {
      data.forEach(i -> records.add(new ThreeColumnRecord(i % partitions, "foo" + i, "bar" + i)));
    } else {
      data.forEach(i -> records.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(files);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .sortWithinPartitions("c1", "c2")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  class GroupInfoMatcher implements ArgumentMatcher<RewriteFileGroup> {
    private final Set<Integer> groupIDs;

    GroupInfoMatcher(Integer... globalIndex) {
      this.groupIDs = ImmutableSet.copyOf(globalIndex);
    }

    @Override
    public boolean matches(RewriteFileGroup argument) {
      return groupIDs.contains(argument.info().globalIndex());
    }
  }
}
