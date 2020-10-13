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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDataFileIndexStatsFilters {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()),
      Types.NestedField.required(3, "category", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private List<Record> records = null;
  private DataFile dataFile = null;
  private DataFile dataFileWithoutNulls = null;
  private DataFile dataFileOnlyNulls = null;

  @Before
  public void createTableAndData() throws IOException {
    File location = temp.newFolder();
    this.table = TestTables.create(location, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);

    this.records = Lists.newArrayList();

    Record record = GenericRecord.create(table.schema());
    records.add(record.copy("id", 1, "data", "a", "category", "odd"));
    records.add(record.copy("id", 2, "data", "b", "category", "even"));
    records.add(record.copy("id", 3, "data", "c", "category", "odd"));
    records.add(record.copy("id", 4, "data", "d", "category", "even"));
    records.add(record.copy("id", 5, "data", "e", "category", "odd"));
    records.add(record.copy("id", 6, "data", "f", "category", "even"));
    records.add(record.copy("id", 7, "data", "g", "category", "odd"));
    records.add(record.copy("id", 8, "data", null, "category", "even"));

    this.dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    this.dataFileWithoutNulls = FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()),
        records.stream().filter(rec -> rec.getField("data") != null).collect(Collectors.toList()));
    this.dataFileOnlyNulls = FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()),
        records.stream().filter(rec -> rec.getField("data") == null).collect(Collectors.toList()));
  }

  @After
  public void dropTable() {
    TestTables.clearTables();
  }

  @Test
  public void testPositionDeletePlanningPath() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFile.path(), 0L));
    deletes.add(Pair.of(dataFile.path(), 1L));

    Pair<DeleteFile, Set<CharSequence>> posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes);
    table.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have one delete file, file_path matches", 1, task.deletes().size());
  }

  @Test
  public void testPositionDeletePlanningPathFilter() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of("some-other-file.parquet", 0L));
    deletes.add(Pair.of("some-other-file.parquet", 1L));

    Pair<DeleteFile, Set<CharSequence>> posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes);
    table.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should not have delete file, filtered by file_path stats", 0, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStats() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "d"));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have one delete file, data contains a matching value", 1, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStatsFilter() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = table.schema().select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "x"));
    deletes.add(delete.copy("data", "y"));
    deletes.add(delete.copy("data", "z"));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should not have delete file, filtered by data column stats", 0, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStatsNullValueWithAllNullDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", null));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have delete file, data contains a null value", 1, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStatsNoNullValuesWithAllNullDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFileWithoutNulls) // note that there are no nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", null));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have no delete files, data contains no null values", 0, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStatsAllNullValuesWithNoNullDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFileOnlyNulls) // note that there are only nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "d"));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have no delete files, data contains no null values", 0, task.deletes().size());
  }

  @Test
  public void testEqualityDeletePlanningStatsSomeNullValuesWithSomeNullDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFile) // note that there are some nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    // the data and delete ranges do not overlap, but both contain null
    deletes.add(delete.copy("data", null));
    deletes.add(delete.copy("data", "x"));

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), deletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assert.assertEquals("Should produce one task", 1, tasks.size());
    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have one delete file, data and deletes have null values", 1, task.deletes().size());
  }
}
