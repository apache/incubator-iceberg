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

package org.apache.iceberg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestDataFileSerialization {

  private static final Schema DATE_SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC = PartitionSpec
      .builderFor(DATE_SCHEMA)
      .identity("date")
      .build();

  private static final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();

  static {
    VALUE_COUNTS.put(1, 5L);
    VALUE_COUNTS.put(2, 3L);
    NULL_VALUE_COUNTS.put(1, 0L);
    NULL_VALUE_COUNTS.put(2, 2L);
    LOWER_BOUNDS.put(1, longToBuffer(0L));
    UPPER_BOUNDS.put(1, longToBuffer(4L));
  }

  private static final DataFile DATA_FILE = DataFiles
      .builder(PARTITION_SPEC)
      .withPath("/path/to/data-1.parquet")
      .withFileSizeInBytes(1234)
      .withPartitionPath("date=2018-06-08")
      .withMetrics(new Metrics(5L, null, VALUE_COUNTS, NULL_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS))
      .withSplitOffsets(ImmutableList.of(4L))
      .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(34))
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testDataFileKryoSerialization() throws Exception {
    File data = temp.newFile();
    Assert.assertTrue(data.delete());
    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    try (Output out = new Output(new FileOutputStream(data))) {
      kryo.writeClassAndObject(out, DATA_FILE);
      kryo.writeClassAndObject(out, DATA_FILE.copy());
    }

    try (Input in = new Input(new FileInputStream(data))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = kryo.readClassAndObject(in);
        Assert.assertTrue("Should be a DataFile", obj instanceof DataFile);
        checkDataFile(DATA_FILE, (DataFile) obj);
      }
    }
  }

  @Test
  public void testDataFileJavaSerialization() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(DATA_FILE);
      out.writeObject(DATA_FILE.copy());
    }

    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = in.readObject();
        Assert.assertTrue("Should be a DataFile", obj instanceof DataFile);
        checkDataFile(DATA_FILE, (DataFile) obj);
      }
    }
  }

  private void checkDataFile(DataFile expected, DataFile actual) {
    Assert.assertEquals("Should match the serialized status",
        expected.status(), actual.status());
    Assert.assertEquals("Should match the serialized snapshot id",
        expected.snapshotId(), actual.snapshotId());
    Assert.assertEquals("Should match the serialized sequence number",
        expected.sequenceNumber(), actual.sequenceNumber());
    Assert.assertEquals("Should match the serialized record path",
        expected.path(), actual.path());
    Assert.assertEquals("Should match the serialized record format",
        expected.format(), actual.format());
    Assert.assertEquals("Should match the serialized record partition",
        expected.partition().get(0, Object.class), actual.partition().get(0, Object.class));
    Assert.assertEquals("Should match the serialized record count",
        expected.recordCount(), actual.recordCount());
    Assert.assertEquals("Should match the serialized record size",
        expected.fileSizeInBytes(), actual.fileSizeInBytes());
    Assert.assertEquals("Should match the serialized record value counts",
        expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals("Should match the serialized record null value counts",
        expected.nullValueCounts(), actual.nullValueCounts());
    Assert.assertEquals("Should match the serialized record lower bounds",
        expected.lowerBounds(), actual.lowerBounds());
    Assert.assertEquals("Should match the serialized record upper bounds",
        expected.upperBounds(), actual.upperBounds());
    Assert.assertEquals("Should match the serialized record key metadata",
        expected.keyMetadata(), actual.keyMetadata());
    Assert.assertEquals("Should match the serialized record offsets",
        expected.splitOffsets(), actual.splitOffsets());
    Assert.assertEquals("Should match the serialized key metadata",
        expected.keyMetadata(), actual.keyMetadata());
  }

  @Test
  public void testParquetWriterSplitOffsets() throws IOException {
    Iterable<InternalRow> records = RandomData.generateSpark(DATE_SCHEMA, 1, 33L);
    File parquetFile = new File(
        temp.getRoot(),
        FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
    FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(DATE_SCHEMA)
            .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(DATE_SCHEMA, msgType))
            .build();
    try {
      writer.addAll(records);
    } finally {
      writer.close();
    }

    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();
    File dataFile = temp.newFile();
    try (Output out = new Output(new FileOutputStream(dataFile))) {
      kryo.writeClassAndObject(out, writer.splitOffsets());
    }
    try (Input in = new Input(new FileInputStream(dataFile))) {
      kryo.readClassAndObject(in);
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
