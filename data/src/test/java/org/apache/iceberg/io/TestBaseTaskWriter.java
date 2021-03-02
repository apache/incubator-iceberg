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

package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBaseTaskWriter extends TableTestBase {
  private static final int FORMAT_V2 = 2;

  private final FileFormat format;
  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);

  private OutputFileFactory fileFactory = null;
  private FileAppenderFactory<Record> appenderFactory = null;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro"},
        {"parquet"}
    };
  }

  public TestBaseTaskWriter(String fileFormat) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = new OutputFileFactory(table.spec(), format, table.locationProvider(), table.io(),
        table.encryption(), 1, 1);

    int firstFieldId = table.schema().findField("id").fieldId();
    int secondFieldId = table.schema().findField("data").fieldId();
    this.appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), table.sortOrder(),
        new int[] {firstFieldId, secondFieldId}, table.schema(), null);

    table.updateProperties()
        .defaultFormat(format)
        .commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @Test
  public void testWriteZeroRecord() throws IOException {
    try (TestTaskWriter writer = createTaskWriter(128 * 1024 * 1024)) {
      writer.close();

      WriteResult result = writer.complete();
      Assert.assertEquals(0, result.dataFiles().length);
      Assert.assertEquals(0, result.deleteFiles().length);

      writer.close();
      result = writer.complete();
      Assert.assertEquals(0, result.dataFiles().length);
      Assert.assertEquals(0, result.deleteFiles().length);
    }
  }

  @Test
  public void testAbort() throws IOException {
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 2000; i++) {
      records.add(createRecord(i, "aaa"));
    }

    List<Path> files;
    try (TestTaskWriter taskWriter = createTaskWriter(4)) {
      for (Record record : records) {
        taskWriter.write(record);
        taskWriter.delete(record);
      }

      // Close the current opened files.
      taskWriter.close();

      // Assert the current data file count.
      files = Files.list(Paths.get(tableDir.getPath(), "data"))
          .filter(p -> !p.toString().endsWith(".crc"))
          .collect(Collectors.toList());
      Assert.assertEquals("Should have 4 files but the files are: " + files, 4, files.size());

      // Abort to clean all delete files and data files.
      taskWriter.abort();
    }

    for (Path path : files) {
      Assert.assertFalse(Files.exists(path));
    }
  }

  @Test
  public void testRollIfExceedTargetFileSize() throws IOException {
    List<Record> records = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      records.add(createRecord(i, "aaa"));
      records.add(createRecord(i, "bbb"));
      records.add(createRecord(i, "ccc"));
      records.add(createRecord(i, "ddd"));
    }

    WriteResult result;
    try (TaskWriter<Record> taskWriter = createTaskWriter(4)) {
      for (Record record : records) {
        taskWriter.write(record);
      }

      result = taskWriter.complete();
      Assert.assertEquals(8, result.dataFiles().length);
      Assert.assertEquals(0, result.deleteFiles().length);
    }

    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    rowDelta.commit();

    List<Record> expected = Lists.newArrayList();
    try (TestTaskWriter taskWriter = createTaskWriter(3)) {
      for (Record record : records) {
        // ex: UPSERT <0, 'aaa'> to <0, 'AAA'>
        taskWriter.delete(record);

        int id = record.get(0, Integer.class);
        String data = record.get(1, String.class);
        Record newRecord = createRecord(id, data.toUpperCase());
        expected.add(newRecord);
        taskWriter.write(newRecord);
      }

      result = taskWriter.complete();
      Assert.assertEquals(8, result.dataFiles().length);
      Assert.assertEquals(8, result.deleteFiles().length);
    }

    rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.commit();

    Assert.assertEquals("Should have expected records", expectedRowSet(expected), actualRowSet("*"));
  }

  private StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  private TestTaskWriter createTaskWriter(long targetFileSize) {
    return new TestTaskWriter(table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
  }

  private static class TestTaskWriter extends BaseTaskWriter<Record> {

    private RollingFileWriter dataWriter;
    private RollingEqDeleteWriter deleteWriter;

    private TestTaskWriter(PartitionSpec spec, FileFormat format,
                           FileAppenderFactory<Record> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io,
                           long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.dataWriter = new RollingFileWriter(null);
      this.deleteWriter = new RollingEqDeleteWriter(null);
    }

    @Override
    public void write(Record row) throws IOException {
      dataWriter.write(row);
    }

    void delete(Record row) throws IOException {
      deleteWriter.write(row);
    }

    @Override
    public void close() throws IOException {
      if (dataWriter != null) {
        dataWriter.close();
      }
      if (deleteWriter != null) {
        deleteWriter.close();
      }
    }
  }
}
