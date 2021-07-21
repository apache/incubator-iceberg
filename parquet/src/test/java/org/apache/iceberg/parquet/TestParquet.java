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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.write;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.writeRecords;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestParquet {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testRowGroupSizeConfigurable() throws IOException {
    // Without an explicit writer function
    File parquetFile = generateFileWithTwoRowGroups(null).first();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      Assert.assertEquals(2, reader.getRowGroups().size());
    }
  }

  @Test
  public void testRowGroupSizeConfigurableWithWriter() throws IOException {
    File parquetFile = generateFileWithTwoRowGroups(ParquetAvroWriter::buildWriter).first();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      Assert.assertEquals(2, reader.getRowGroups().size());
    }
  }

  @Test
  public void testNumberOfBytesWritten() throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", IntegerType.get())
    );

    // this value was specifically derived to reproduce iss1980
    // record count grow factor is 10000 (hardcoded)
    // total 10 checkSize method calls
    // for the 10th time (the last call of the checkSize method) nextCheckRecordCount == 100100
    // 100099 + 1 >= 100100
    int recordCount = 100099;
    File file = createTempFile(temp);

    List<GenericData.Record> records = new ArrayList<>(recordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= recordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    long actualSize = write(file, schema, Collections.emptyMap(), ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[]{}));

    long expectedSize = ParquetIO.file(localInput(file)).getLength();
    Assert.assertEquals(expectedSize, actualSize);
  }

  private Pair<File, Long> generateFileWithTwoRowGroups(Function<MessageType, ParquetValueWriter<?>> createWriterFunc)
      throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", IntegerType.get())
    );

    int minimumRowGroupRecordCount = 100;
    int desiredRecordCount = minimumRowGroupRecordCount + 1;

    List<GenericData.Record> records = new ArrayList<>(desiredRecordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= desiredRecordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    // Force multiple row groups by making the byte size very small
    // Note there'a also minimumRowGroupRecordCount which cannot be configured so we have to write
    // at least that many records for a new row group to occur
    File file = createTempFile(temp);
    long size = write(file,
        schema,
        ImmutableMap.of(
            PARQUET_ROW_GROUP_SIZE_BYTES,
            Integer.toString(minimumRowGroupRecordCount * Integer.BYTES)),
        createWriterFunc,
        records.toArray(new GenericData.Record[]{}));
    return Pair.of(file, size);
  }

  @Test
  public void testBloomFilterWriteRead() throws IOException {
    File parquetFile = generateFileWithBloomFilter();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      BloomFilterReader bloomFilterDataReader = reader.getBloomFilterDataReader(rowGroup);

      ColumnChunkMetaData intColumn = rowGroup.getColumns().get(0);
      BloomFilter intBloomFilter = bloomFilterDataReader.readBloomFilter(intColumn);
      Assert.assertTrue(intBloomFilter.findHash(intBloomFilter.hash(30)));
      Assert.assertTrue(intBloomFilter.findHash(intBloomFilter.hash(40)));
      Assert.assertTrue(intBloomFilter.findHash(intBloomFilter.hash(79)));

      ColumnChunkMetaData longColumn = rowGroup.getColumns().get(1);
      BloomFilter longBloomFilter = bloomFilterDataReader.readBloomFilter(longColumn);
      Assert.assertTrue(longBloomFilter.findHash(longBloomFilter.hash(130L)));
      Assert.assertTrue(longBloomFilter.findHash(longBloomFilter.hash(140L)));
      Assert.assertTrue(longBloomFilter.findHash(longBloomFilter.hash(179L)));

      ColumnChunkMetaData floatColumn = rowGroup.getColumns().get(2);
      BloomFilter floatBloomFilter = bloomFilterDataReader.readBloomFilter(floatColumn);
      Assert.assertTrue(floatBloomFilter.findHash(floatBloomFilter.hash(1030F)));
      Assert.assertTrue(floatBloomFilter.findHash(floatBloomFilter.hash(1040F)));
      Assert.assertTrue(floatBloomFilter.findHash(floatBloomFilter.hash(1079F)));

      ColumnChunkMetaData doubleColumn = rowGroup.getColumns().get(3);
      BloomFilter doubleBloomFilter = bloomFilterDataReader.readBloomFilter(doubleColumn);
      Assert.assertTrue(doubleBloomFilter.findHash(doubleBloomFilter.hash(10030D)));
      Assert.assertTrue(doubleBloomFilter.findHash(doubleBloomFilter.hash(10040D)));
      Assert.assertTrue(doubleBloomFilter.findHash(doubleBloomFilter.hash(10079D)));

      ColumnChunkMetaData binaryColumn = rowGroup.getColumns().get(4);
      BloomFilter binaryBloomFilter = bloomFilterDataReader.readBloomFilter(binaryColumn);
      Assert.assertTrue(binaryBloomFilter.findHash(binaryBloomFilter.hash(Binary.fromString("BINARY_30"))));
      Assert.assertTrue(binaryBloomFilter.findHash(binaryBloomFilter.hash(Binary.fromString("BINARY_40"))));
      Assert.assertTrue(binaryBloomFilter.findHash(binaryBloomFilter.hash(Binary.fromString("BINARY_79"))));

      ColumnChunkMetaData noBloomColumn = rowGroup.getColumns().get(5);
      Assert.assertFalse(ParquetUtil.hasNonBloomFilterPages(intColumn));
      Assert.assertFalse(ParquetUtil.hasNonBloomFilterPages(longColumn));
      Assert.assertFalse(ParquetUtil.hasNonBloomFilterPages(floatColumn));
      Assert.assertFalse(ParquetUtil.hasNonBloomFilterPages(doubleColumn));
      Assert.assertFalse(ParquetUtil.hasNonBloomFilterPages(binaryColumn));
      Assert.assertTrue(ParquetUtil.hasNonBloomFilterPages(noBloomColumn));
    }
  }

  private File generateFileWithBloomFilter() throws IOException {
    Schema schema = new Schema(
        required(0, "id", IntegerType.get()),
        required(1, "id_long", LongType.get()),
        required(2, "id_float", FloatType.get()),
        required(3, "id_double", DoubleType.get()),
        required(4, "id_binary", StringType.get()),
        required(5, "id_no_bloom", IntegerType.get())
    );
    int recordCount = 50;
    List<GenericData.Record> records = new ArrayList<>(recordCount);
    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "bloom_filter_test"));
    for (int i = 0; i < recordCount; i++) {
      builder.set("id", 30 + i); // min=30, max=79
      builder.set("id_long", 100L + 30 + i); // min=130L, max=179L
      builder.set("id_float", 1000F + 30 + i); // min=1030F, max=10079F
      builder.set("id_double", 10000D + 30 + i); // min=10030D, max=1079D
      builder.set("id_binary", "BINARY_" + (30 + i)); // min=BINARY_30, max=BINARY_79
      builder.set("id_no_bloom", 30 + i); // min=30, max=79
      records.add(builder.build());
    }

    ImmutableMap<String, String> config = ImmutableMap.of(
        PARQUET_BLOOM_FILTER_ENABLED, "true",
        PARQUET_BLOOM_FILTER_ENABLED + "#id_no_bloom", "false");
    return writeRecords(temp, schema, config, records.toArray(new GenericData.Record[] {}));
  }
}
