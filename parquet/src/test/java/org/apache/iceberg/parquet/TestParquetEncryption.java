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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.NativeFileEncryptParams;
import org.apache.iceberg.encryption.NativeFileEncryptParamsImpl;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;

// Modelled after TestParquet.java - writing with Iceberg Parquet API, reading with Parquet API
public class TestParquetEncryption {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testEncryption() throws IOException {
    String columnName = "intCol";
    Schema schema = new Schema(
        optional(1, columnName, IntegerType.get())
    );

    int minimumRowGroupRecordCount = 100;
    int desiredRecordCount = minimumRowGroupRecordCount - 1;

    List<GenericData.Record> records = new ArrayList<>(desiredRecordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= desiredRecordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    int numberOfKeys = 2;
    ByteBuffer[] deks = new ByteBuffer[numberOfKeys];
    String[] dekIds = new String[numberOfKeys];
    Map<String, ByteBuffer> fileKeys = new HashMap<>();
    Random rand = new Random();

    for (int i = 0; i < numberOfKeys; i++) {
      byte[] key = new byte[16];
      rand.nextBytes(key);
      deks[i] = ByteBuffer.wrap(key);
      dekIds[i] = "k" + i;
      fileKeys.put(dekIds[i], deks[i]);
    }

    String aadPrefix = "abcd";

    Map<String, String> columnKeyIds = new HashMap<>();
    columnKeyIds.put(columnName, dekIds[0]);

    NativeFileEncryptParams encryptionParams = NativeFileEncryptParamsImpl.create(fileKeys)
        .fileKeyId(dekIds[1])
        .columnKeyIds(columnKeyIds)
        .aadPrefix(ByteBuffer.wrap(aadPrefix.getBytes(StandardCharsets.UTF_8)))
        .build();

    File file = createTempFile(temp);

    FileAppender<GenericData.Record> writer = Parquet.write(localOutput(file))
        .schema(schema)
        .encryption(encryptionParams)
        .build();

    try (Closeable toClose = writer) {
      writer.addAll(Lists.newArrayList(records.toArray(new GenericData.Record[]{})));
    }

    FileDekRetriever keyRetriever = new FileDekRetriever(fileKeys);
    FileDecryptionProperties decryptionProperties = FileDecryptionProperties.builder()
        .withKeyRetriever(keyRetriever)
        .withAADPrefix(aadPrefix.getBytes(StandardCharsets.UTF_8))
        .build();

    String exceptionText = "";
    try {
      ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)));
    } catch (ParquetCryptoRuntimeException e) {
      exceptionText = e.toString();
    }
    String expected = "org.apache.parquet.crypto.ParquetCryptoRuntimeException: " +
        "Trying to read file with encrypted footer. No keys available";
    Assert.assertEquals(expected, exceptionText);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)),
        ParquetReadOptions.builder()
            .withDecryption(decryptionProperties)
            .build())) {
      MessageType parquetSchema = reader.getFileMetaData().getSchema();
      Assert.assertTrue(parquetSchema.containsPath(new String[]{"intCol"}));
    }
  }
}
