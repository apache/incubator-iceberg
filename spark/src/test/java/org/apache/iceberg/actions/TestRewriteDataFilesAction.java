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

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestRewriteDataFilesAction extends SparkTestBase {

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

  @Test
  public void testRewriteDataFilesEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    Actions actions = Actions.forTable(table);

    actions.rewriteDataFiles().execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testRewriteDataFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions.rewriteDataFiles().execute();
    Assert.assertEquals("Action should rewrite 4 data files", 4, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 1 data files before rewrite", 1, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions.rewriteDataFiles().execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithFilter() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .filter(Expressions.equal("c1", 1))
        .filter(Expressions.startsWith("c2", "AA"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 7 data files before rewrite", 7, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
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

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan()
        .ignoreResiduals()
        .filter(Expressions.equal("c3", "0"))
        .planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files before rewrite", 2, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .filter(Expressions.equal("c3", "0"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testRewriteDataFilesForLargeFile() throws AnalysisException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    IntStream.range(0, 2000).forEach(i -> records1.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(1);
    writeDF(df);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    DataFile maxSizeFile = Collections.max(dataFiles, Comparator.comparingLong(DataFile::fileSizeInBytes));
    Assert.assertEquals("Should have 3 files before rewrite", 3, dataFiles.size());

    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("origin");
    long originalNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> originalRecords = sql("SELECT * from origin sort by c2");

    Actions actions = Actions.forTable(table);

    long targetSizeInBytes = maxSizeFile.fileSizeInBytes() - 10;
    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .targetSizeInBytes(targetSizeInBytes)
        .splitOpenFileCost(1)
        .execute();

    Assert.assertEquals("Action should delete 4 data files", 4, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 2 data files", 2, result.addedDataFiles().size());

    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("postRewrite");
    long postRewriteNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> rewrittenRecords = sql("SELECT * from postRewrite sort by c2");

    Assert.assertEquals(originalNumRecords, postRewriteNumRecords);
    assertEquals("Rows should be unchanged", originalRecords, rewrittenRecords);
  }

  @Test
  public void testRewriteDeletesInUnpartitionedTable() throws Exception {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TABLES.create(SCHEMA, spec, ImmutableMap.of(), tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());
    upgradeToFormatV2(table);

    List<ThreeColumnRecord> txn1 = Lists.newArrayList(
        new ThreeColumnRecord(0, "AAA", "AAA-0"),
        new ThreeColumnRecord(1, "BBB", "BBB-1"),
        new ThreeColumnRecord(2, "CCC", "CCC-2"),
        new ThreeColumnRecord(3, "DDD", "DDD-3")
    );
    writeRecords(txn1);

    List<ThreeColumnRecord> txn2 = Lists.newArrayList(
        new ThreeColumnRecord(4, "EEE", "EEE-4"),
        new ThreeColumnRecord(5, "FFF", "FFF-5")
    );
    writeRecords(txn2);

    // Commit the txn to delete few rows.
    Schema deleteRowSchema = table.schema().select("c2");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> deletions = Lists.newArrayList(
        dataDelete.copy("c2", "BBB"),
        dataDelete.copy("c2", "CCC"),
        dataDelete.copy("c2", "DDD"),
        dataDelete.copy("c2", "FFF")
    );
    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(table, newOutputFile(), deletions, deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    // Read original records before action.
    long originalNumRecords = 2;
    List<Object[]> originalRecords = Lists.newArrayList(
        new Object[] {0, "AAA", "AAA-0"},
        new Object[] {4, "EEE", "EEE-4"}
    );
    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("preRewrite");
    assertEquals("Rows should be as expected", originalRecords, sql("SELECT * from preRewrite sort by c2"));

    // Execute the rewrite files action.
    table.refresh();
    RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();

    Assert.assertEquals("Action should delete 4 data files", 4, result.deletedDataFiles().size());
    Assert.assertEquals("Action should delete 1 delete files", 1, result.deletedDeleteFiles().size());
    Assert.assertEquals("Action should add 1 data files", 1, result.addedDataFiles().size());
    Assert.assertEquals("Action should add 0 delete files", 0, result.addedDeleteFiles().size());

    // Read rewritten records after action.
    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("postRewrite");
    long postRewriteNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> rewrittenRecords = sql("SELECT * from postRewrite sort by c2");

    Assert.assertEquals(originalNumRecords, postRewriteNumRecords);
    assertEquals("Rows should be unchanged", originalRecords, rewrittenRecords);
  }

  @Test
  public void testRewriteDeletesInPartitionedTable() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Table table = TABLES.create(SCHEMA, spec, ImmutableMap.of(), tableLocation);
    upgradeToFormatV2(table);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    // Commit the txn to delete few rows.
    Schema deleteRowSchema = table.schema().select("c1", "c2", "c3");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> deletions = Lists.newArrayList(
        dataDelete.copy("c1", 1, "c2", "AAAAAAAAAA", "c3", "CCCC"),
        dataDelete.copy("c1", 1, "c2", "BBBBBBBBBB", "c3", "DDDD"),
        dataDelete.copy("c1", 2, "c2", "AAAAAAAAAA", "c3", "GGGG"),
        dataDelete.copy("c1", 2, "c2", "BBBBBBBBBB", "c3", "HHHH")
    );
    DeleteFile eqDeletes1 = FileHelpers.writeDeleteFile(table, newOutputFile(),
        TestHelpers.Row.of(1, "AA"), deletions.subList(0, 1), deleteRowSchema);
    DeleteFile eqDeletes2 = FileHelpers.writeDeleteFile(table, newOutputFile(),
        TestHelpers.Row.of(1, "BB"), deletions.subList(1, 2), deleteRowSchema);
    DeleteFile eqDeletes3 = FileHelpers.writeDeleteFile(table, newOutputFile(),
        TestHelpers.Row.of(2, "AA"), deletions.subList(2, 3), deleteRowSchema);
    DeleteFile eqDeletes4 = FileHelpers.writeDeleteFile(table, newOutputFile(),
        TestHelpers.Row.of(2, "BB"), deletions.subList(3, 4), deleteRowSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes1)
        .addDeletes(eqDeletes2)
        .addDeletes(eqDeletes3)
        .addDeletes(eqDeletes4)
        .commit();

    // Read original records before action.
    long originalNumRecords = 4;
    List<Object[]> originalRecords = Lists.newArrayList(
        new Object[] {1, "AAAAAAAAAA", "AAAA"},
        new Object[] {1, "BBBBBBBBBB", "BBBB"},
        new Object[] {2, "AAAAAAAAAA", "EEEE"},
        new Object[] {2, "BBBBBBBBBB", "FFFF"}
    );
    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("preRewrite");
    assertEquals("Rows should be as expected", originalRecords, sql("SELECT * from preRewrite sort by c3"));

    // Execute the rewrite files action.
    table.refresh();
    RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();

    Assert.assertEquals("Action should delete 8 data files", 8, result.deletedDataFiles().size());
    Assert.assertEquals("Action should delete 4 delete files", 4, result.deletedDeleteFiles().size());
    Assert.assertEquals("Action should add 4 data files", 4, result.addedDataFiles().size());
    Assert.assertEquals("Action should add 0 delete files", 0, result.addedDeleteFiles().size());

    // Read rewritten records after action.
    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("postRewrite");
    long postRewriteNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> rewrittenRecords = sql("SELECT * from postRewrite sort by c3");

    Assert.assertEquals(originalNumRecords, postRewriteNumRecords);
    assertEquals("Rows should be unchanged", originalRecords, rewrittenRecords);
  }

  private static Table upgradeToFormatV2(Table table) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));
    return table;
  }

  private static final AtomicInteger FILE_COUNT = new AtomicInteger(1);

  private OutputFile newOutputFile() {
    return HadoopOutputFile.fromLocation(String.format("%s/%s.%s", tableLocation,
        FILE_COUNT.incrementAndGet(), FileFormat.PARQUET), new Configuration());
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  @Test
  public void testRewriteToOutputPartitionSpec() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    table.updateSpec().addField(Expressions.truncate("c2", 2)).commit();

    Assert.assertEquals("Should have 2 partitions specs", 2, table.specs().size());

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Dataset<Row> beforeResultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> beforeActualFilteredRecords = beforeResultDF.sort("c1", "c2", "c3")
        .filter("c1 = 1 AND c2 = 'BBBBBBBBBB'")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records2, beforeActualFilteredRecords);

    Actions actions = Actions.forTable(table);
    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .outputSpecId(0)
        .execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 2 data file", 2, result.addedDataFiles().size());

    Assert.assertTrue(result.deletedDataFiles().stream().allMatch(df -> df.specId() == 1));
    Assert.assertTrue(result.addedDataFiles().stream().allMatch(df -> df.specId() == 0));

    table.refresh();

    CloseableIterable<FileScanTask> tasks2 = table.newScan().planFiles();
    List<DataFile> dataFiles2 = Lists.newArrayList(CloseableIterable.transform(tasks2, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files after rewrite", 2, dataFiles2.size());

    // Should still have all the same data
    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);

    List<ThreeColumnRecord> actualFilteredRecords = resultDF.sort("c1", "c2", "c3")
        .filter("c1 = 1 AND c2 = 'BBBBBBBBBB'")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records2, actualFilteredRecords);

    List<ThreeColumnRecord> records5 = Lists.newArrayList(
        new ThreeColumnRecord(3, "CCCCCCCCCC", "FFFF"),
        new ThreeColumnRecord(3, "CCCCCCCCCC", "HHHH")
    );
    writeRecords(records5);
    expectedRecords.addAll(records5);
    actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }
}
