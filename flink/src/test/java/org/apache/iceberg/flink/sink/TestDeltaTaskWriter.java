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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.createDelete;
import static org.apache.iceberg.flink.SimpleDataUtil.createInsert;
import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.apache.iceberg.flink.SimpleDataUtil.createUpdateAfter;
import static org.apache.iceberg.flink.SimpleDataUtil.createUpdateBefore;

@RunWith(Parameterized.class)
public class TestDeltaTaskWriter extends TableTestBase {
  private static final int FORMAT_V2 = 2;

  private final FileFormat format;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro"},
        {"parquet"}
    };
  }

  public TestDeltaTaskWriter(String fileFormat) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
  }

  private void initTable(boolean partitioned) {
    if (partitioned) {
      this.table = create(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("data").build());
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    }

    table.updateProperties()
        .defaultFormat(format)
        .commit();
  }

  private int idFieldId() {
    return table.schema().findField("id").fieldId();
  }

  private int dataFieldId() {
    return table.schema().findField("data").fieldId();
  }

  private void testCdcEvents(boolean partitioned) throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    // Start the 1th transaction.
    TaskWriter<RowData> writer = taskWriterFactory.create();

    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "bbb"));
    writer.write(createInsert(3, "ccc"));

    // Update <2, 'bbb'> to <2, 'ddd'>
    writer.write(createUpdateBefore(2, "bbb")); // 1 pos-delete and 1 eq-delete.
    writer.write(createUpdateAfter(2, "ddd"));

    // Update <1, 'aaa'> to <1, 'eee'>
    writer.write(createUpdateBefore(1, "aaa")); // 1 pos-delete and 1 eq-delete.
    writer.write(createUpdateAfter(1, "eee"));

    // Insert <4, 'fff'>
    writer.write(createInsert(4, "fff"));
    // Insert <5, 'ggg'>
    writer.write(createInsert(5, "ggg"));

    // Delete <3, 'ccc'>
    writer.write(createDelete(3, "ccc")); // 1 pos-delete and 1 eq-delete.

    WriteResult result = writer.complete();
    Assert.assertEquals(partitioned ? 7 : 1, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 6 : 2, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records.", expectedRowSet(
        createRecord(1, "eee"),
        createRecord(2, "ddd"),
        createRecord(4, "fff"),
        createRecord(5, "ggg")
    ), actualRowSet("*"));

    // Start the 2nd transaction.
    writer = taskWriterFactory.create();

    // Update <2, 'ddd'> to <6, 'hhh'> - (Update both key and value)
    writer.write(createUpdateBefore(2, "ddd")); // 1 eq-delete
    writer.write(createUpdateAfter(6, "hhh"));

    // Update <5, 'ggg'> to <5, 'iii'>
    writer.write(createUpdateBefore(5, "ggg")); // 1 eq-delete
    writer.write(createUpdateAfter(5, "iii"));

    // Delete <4, 'fff'>
    writer.write(createDelete(4, "fff")); // 1 eq-delete.

    result = writer.complete();
    Assert.assertEquals(partitioned ? 2 : 1, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", expectedRowSet(
        createRecord(1, "eee"),
        createRecord(5, "iii"),
        createRecord(6, "hhh")
    ), actualRowSet("*"));
  }

  @Test
  public void testUnpartitioned() throws IOException {
    initTable(false);
    testCdcEvents(false);
  }

  @Test
  public void testPartitioned() throws IOException {
    initTable(true);
    testCdcEvents(true);
  }

  private void testWritePureEqDeletes(boolean partitioned) throws IOException {
    initTable(partitioned);
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createDelete(1, "aaa"));
    writer.write(createDelete(2, "bbb"));
    writer.write(createDelete(3, "ccc"));

    WriteResult result = writer.complete();
    Assert.assertEquals(0, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have no record", expectedRowSet(), actualRowSet("*"));
  }

  @Test
  public void testUnpartitionedPureEqDeletes() throws IOException {
    testWritePureEqDeletes(false);
  }

  @Test
  public void testPartitionedPureEqDeletes() throws IOException {
    testWritePureEqDeletes(true);
  }

  private void testAbort(boolean partitioned) throws IOException {
    initTable(partitioned);
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createUpdateBefore(1, "aaa"));
    writer.write(createUpdateAfter(1, "bbb"));

    writer.write(createUpdateBefore(2, "aaa"));
    writer.write(createUpdateAfter(2, "bbb"));

    // Assert the current data/delete file count.
    List<Path> files = Files.walk(Paths.get(tableDir.getPath(), "data"))
        .filter(p -> p.toFile().isFile())
        .filter(p -> !p.toString().endsWith(".crc"))
        .collect(Collectors.toList());
    Assert.assertEquals("Should have expected file count, but files are: " + files, partitioned ? 4 : 2, files.size());

    writer.abort();
    for (Path file : files) {
      Assert.assertFalse(Files.exists(file));
    }
  }

  @Test
  public void testUnpartitionedAbort() throws IOException {
    testAbort(false);
  }

  @Test
  public void testPartitionedAbort() throws IOException {
    testAbort(true);
  }

  @Test
  public void testPartitionedTableWithDataAsKey() throws IOException {
    initTable(true);
    List<Integer> equalityFieldIds = Lists.newArrayList(dataFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    // Start the 1th transaction.
    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "aaa"));
    writer.write(createInsert(3, "bbb"));
    writer.write(createInsert(4, "ccc"));

    WriteResult result = writer.complete();
    Assert.assertEquals(3, result.dataFiles().length);
    Assert.assertEquals(1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", expectedRowSet(
        createRecord(2, "aaa"),
        createRecord(3, "bbb"),
        createRecord(4, "ccc")
    ), actualRowSet("*"));

    // Start the 2nd transaction.
    writer = taskWriterFactory.create();
    writer.write(createInsert(5, "aaa"));
    writer.write(createInsert(6, "bbb"));
    writer.write(createDelete(7, "ccc")); // 1 eq-delete.

    result = writer.complete();
    Assert.assertEquals(2, result.dataFiles().length);
    Assert.assertEquals(1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", expectedRowSet(
        createRecord(2, "aaa"),
        createRecord(5, "aaa"),
        createRecord(3, "bbb"),
        createRecord(6, "bbb")
    ), actualRowSet("*"));
  }

  @Test
  public void testPartitionedTableWithDataAndIdAsKey() throws IOException {
    initTable(true);
    List<Integer> equalityFieldIds = Lists.newArrayList(dataFieldId(), idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "aaa"));

    writer.write(createDelete(2, "aaa")); // 1 pos-delete and 1 eq-delete.

    WriteResult result = writer.complete();
    Assert.assertEquals(1, result.dataFiles().length);
    Assert.assertEquals(2, result.deleteFiles().length);
    Assert.assertEquals(Sets.newHashSet(FileContent.EQUALITY_DELETES, FileContent.POSITION_DELETES),
        Sets.newHashSet(result.deleteFiles()[0].content(), result.deleteFiles()[1].content()));
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", expectedRowSet(
        createRecord(1, "aaa")
    ), actualRowSet("*"));
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
  }

  private StructLikeSet expectedRowSet(Record... records) {
    return SimpleDataUtil.expectedRowSet(table, records);
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    return SimpleDataUtil.actualRowSet(table, columns);
  }

  private TaskWriterFactory<RowData> createTaskWriterFactory(List<Integer> equalityFieldIds) {
    return new RowDataTaskWriterFactory(table, FlinkSchemaUtil.convert(table.schema()),
        table.locationProvider(), table.io(), table.encryption(),
        128 * 1024 * 1024, format, table.properties(), equalityFieldIds);
  }
}
