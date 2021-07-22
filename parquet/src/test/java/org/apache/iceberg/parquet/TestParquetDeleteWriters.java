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

package org.apache.iceberg.parquet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetDeleteWriters {
  private static final Schema SCHEMA = new Schema(
      NestedField.required(1, "id", Types.LongType.get()),
      NestedField.optional(2, "data", Types.StringType.get()));

  private List<Record> records;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createDeleteRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(record.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(record.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(record.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(record.copy(ImmutableMap.of("id", 5L, "data", "e")));

    this.records = builder.build();
  }

  @Test
  public void testEqualityDeleteWriter() throws IOException {
    File deleteFile = temp.newFile();

    OutputFile out = Files.localOutput(deleteFile);
    EqualityDeleteWriter<Record> deleteWriter = Parquet.writeDeletes(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .rowSchema(SCHEMA)
        .spec(PartitionSpec.unpartitioned())
        .equalityFieldIds(1)
        .buildEqualityWriter();

    try (EqualityDeleteWriter<Record> writer = deleteWriter) {
      writer.deleteAll(records);
    }

    DeleteFile metadata = deleteWriter.toDeleteFile();
    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, metadata.format());
    Assert.assertEquals("Should be equality deletes", FileContent.EQUALITY_DELETES, metadata.content());
    Assert.assertEquals("Record count should be correct", records.size(), metadata.recordCount());
    Assert.assertEquals("Partition should be empty", 0, metadata.partition().size());
    Assert.assertNull("Key metadata should be null", metadata.keyMetadata());

    List<Record> deletedRecords;
    try (CloseableIterable<Record> reader = Parquet.read(out.toInputFile())
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
        .build()) {
      deletedRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Deleted records should match expected", records, deletedRecords);
  }

  @Test
  public void testPositionDeleteWriter() throws IOException {
    File deleteFile = temp.newFile();

    Schema deleteSchema = new Schema(
        MetadataColumns.DELETE_FILE_PATH,
        MetadataColumns.DELETE_FILE_POS,
        NestedField.optional(MetadataColumns.DELETE_FILE_ROW_FIELD_ID, "row", SCHEMA.asStruct()));

    String deletePath = "s3://bucket/path/file.parquet";
    GenericRecord posDelete = GenericRecord.create(deleteSchema);
    List<Record> expectedDeleteRecords = Lists.newArrayList();

    OutputFile out = Files.localOutput(deleteFile);
    PositionDeleteWriter<Record> deleteWriter = Parquet.writeDeletes(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .rowSchema(SCHEMA)
        .spec(PartitionSpec.unpartitioned())
        .buildPositionWriter();

    try (PositionDeleteWriter<Record> writer = deleteWriter) {
      for (int i = 0; i < records.size(); i += 1) {
        int pos = i * 3 + 2;
        writer.delete(deletePath, pos, records.get(i));
        expectedDeleteRecords.add(posDelete.copy(ImmutableMap.of(
            "file_path", deletePath,
            "pos", (long) pos,
            "row", records.get(i))));
      }
    }

    DeleteFile metadata = deleteWriter.toDeleteFile();
    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, metadata.format());
    Assert.assertEquals("Should be position deletes", FileContent.POSITION_DELETES, metadata.content());
    Assert.assertEquals("Record count should be correct", records.size(), metadata.recordCount());
    Assert.assertEquals("Partition should be empty", 0, metadata.partition().size());
    Assert.assertNull("Key metadata should be null", metadata.keyMetadata());

    List<Record> deletedRecords;
    try (CloseableIterable<Record> reader = Parquet.read(out.toInputFile())
        .project(deleteSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
        .build()) {
      deletedRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Deleted records should match expected", expectedDeleteRecords, deletedRecords);
  }

  @Test
  public void testPositionDeleteWriterWithEmptyRow() throws IOException {
    File deleteFile = temp.newFile();

    Schema deleteSchema = new Schema(
        MetadataColumns.DELETE_FILE_PATH,
        MetadataColumns.DELETE_FILE_POS);

    String deletePath = "s3://bucket/path/file.parquet";
    GenericRecord posDelete = GenericRecord.create(deleteSchema);
    List<Record> expectedDeleteRecords = Lists.newArrayList();

    OutputFile out = Files.localOutput(deleteFile);
    PositionDeleteWriter<Void> deleteWriter = Parquet.writeDeletes(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .spec(PartitionSpec.unpartitioned())
        .buildPositionWriter();

    try (PositionDeleteWriter<Void> writer = deleteWriter) {
      for (int i = 0; i < records.size(); i += 1) {
        int pos = i * 3 + 2;
        writer.delete(deletePath, pos, null);
        expectedDeleteRecords.add(posDelete.copy(ImmutableMap.of(
            "file_path", deletePath,
            "pos", (long) pos)));
      }
    }

    DeleteFile metadata = deleteWriter.toDeleteFile();
    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, metadata.format());
    Assert.assertEquals("Should be position deletes", FileContent.POSITION_DELETES, metadata.content());
    Assert.assertEquals("Record count should be correct", records.size(), metadata.recordCount());
    Assert.assertEquals("Partition should be empty", 0, metadata.partition().size());
    Assert.assertNull("Key metadata should be null", metadata.keyMetadata());

    List<Record> deletedRecords;
    try (CloseableIterable<Record> reader = Parquet.read(out.toInputFile())
        .project(deleteSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
        .build()) {
      deletedRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Deleted records should match expected", expectedDeleteRecords, deletedRecords);
  }
}
