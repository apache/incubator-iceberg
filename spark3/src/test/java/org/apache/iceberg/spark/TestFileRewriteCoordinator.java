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

package org.apache.iceberg.spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFileRewriteCoordinator extends SparkCatalogTestBase {

  public TestFileRewriteCoordinator(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testBinPackRewrite() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    Dataset<Row> df = newDF(1000);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should produce 4 snapshots", 4, Iterables.size(table.snapshots()));

    Dataset<Row> fileDF = spark.read().format("iceberg").load(tableName(tableIdent.name() + ".files"));
    List<Long> fileSizes = fileDF.select("file_size_in_bytes").as(Encoders.LONG()).collectAsList();
    long avgFileSize = fileSizes.stream().mapToLong(i -> i).sum() / fileSizes.size();

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      String fileSetID = UUID.randomUUID().toString();

      FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();
      taskSetManager.stageTasks(table, fileSetID, Lists.newArrayList(fileScanTasks));

      // read and pack original 4 files into 2 splits
      Dataset<Row> scanDF = spark.read().format("iceberg")
          .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, fileSetID)
          .option(SparkReadOptions.SPLIT_SIZE, Long.toString(avgFileSize * 2))
          .option(SparkReadOptions.FILE_OPEN_COST, "0")
          .load(tableName);

      // write the packed data into new files where each split becomes a new file
      scanDF.writeTo(tableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();

      // commit the rewrite
      FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();
      rewriteCoordinator.commitRewrite(table, fileSetID);
    }

    table.refresh();

    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("Deleted files count must match", "4", summary.get("deleted-data-files"));
    Assert.assertEquals("Added files count must match", "2", summary.get("added-data-files"));

    Object rowCount = scalarSql("SELECT count(*) FROM %s", tableName);
    Assert.assertEquals("Row count must match", 4000L, rowCount);
  }

  @Test
  public void testSortRewrite() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    Dataset<Row> df = newDF(1000);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should produce 4 snapshots", 4, Iterables.size(table.snapshots()));

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      String fileSetID = UUID.randomUUID().toString();

      FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();
      taskSetManager.stageTasks(table, fileSetID, Lists.newArrayList(fileScanTasks));

      // read original 4 files as 4 splits
      Dataset<Row> scanDF = spark.read().format("iceberg")
          .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, fileSetID)
          .option(SparkReadOptions.SPLIT_SIZE, "134217728")
          .option(SparkReadOptions.FILE_OPEN_COST, "134217728")
          .load(tableName);

      // make sure we disable AQE and set the number of shuffle partitions as the target num files
      ImmutableMap<String, String> sqlConf = ImmutableMap.of(
          "spark.sql.shuffle.partitions", "2",
          "spark.sql.adaptive.enabled", "false"
      );

      withSQLConf(sqlConf, () -> {
        try {
          // write new files with sorted records
          scanDF.sort("id").writeTo(tableName)
              .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
              .append();
        } catch (NoSuchTableException e) {
          throw new RuntimeException("Could not replace files", e);
        }
      });

      // commit the rewrite
      FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();
      rewriteCoordinator.commitRewrite(table, fileSetID);
    }

    table.refresh();

    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("Deleted files count must match", "4", summary.get("deleted-data-files"));
    Assert.assertEquals("Added files count must match", "2", summary.get("added-data-files"));

    Object rowCount = scalarSql("SELECT count(*) FROM %s", tableName);
    Assert.assertEquals("Row count must match", 4000L, rowCount);
  }

  @Test
  public void testCommitMultipleRewrites() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    Dataset<Row> df = newDF(1000);

    // add first two files
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);

    String firstFileSetID = UUID.randomUUID().toString();
    long firstFileSetSnapshotId = table.currentSnapshot().snapshotId();

    FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      // stage first 2 files for compaction
      taskSetManager.stageTasks(table, firstFileSetID, Lists.newArrayList(tasks));
    }

    // add two more files
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    table.refresh();

    String secondFileSetID = UUID.randomUUID().toString();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().dataAfter(firstFileSetSnapshotId).planFiles()) {
      // stage 2 more files for compaction
      taskSetManager.stageTasks(table, secondFileSetID, Lists.newArrayList(tasks));
    }

    ImmutableSet<String> fileSetIDs = ImmutableSet.of(firstFileSetID, secondFileSetID);

    for (String fileSetID : fileSetIDs) {
      // read and pack 2 files into 1 split
      Dataset<Row> scanDF = spark.read().format("iceberg")
          .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, fileSetID)
          .option(SparkReadOptions.SPLIT_SIZE, Long.MAX_VALUE)
          .load(tableName);

      // write the combined data as one file
      scanDF.writeTo(tableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();
    }

    // commit both rewrites at the same time
    FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();
    rewriteCoordinator.commitRewrite(table, fileSetIDs);

    table.refresh();

    Assert.assertEquals("Should produce 5 snapshots", 5, Iterables.size(table.snapshots()));

    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("Deleted files count must match", "4", summary.get("deleted-data-files"));
    Assert.assertEquals("Added files count must match", "2", summary.get("added-data-files"));

    Object rowCount = scalarSql("SELECT count(*) FROM %s", tableName);
    Assert.assertEquals("Row count must match", 4000L, rowCount);
  }

  private Dataset<Row> newDF(int numRecords) {
    List<SimpleRecord> data = Lists.newArrayListWithExpectedSize(numRecords);
    for (int index = 0; index < numRecords; index++) {
      data.add(new SimpleRecord(index, Integer.toString(index)));
    }
    return spark.createDataFrame(data, SimpleRecord.class);
  }
}
