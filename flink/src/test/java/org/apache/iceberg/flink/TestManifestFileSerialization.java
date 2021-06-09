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

package org.apache.iceberg.flink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestManifestFileSerialization {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()),
      required(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec SPEC = PartitionSpec
      .builderFor(SCHEMA)
      .identity("double")
      .build();

  private static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-1.parquet")
      .withFileSizeInBytes(0)
      .withPartition(org.apache.iceberg.TestHelpers.Row.of(1D))
      .withPartitionPath("double=1")
      .withMetrics(new Metrics(5L,
          null, // no column sizes
          ImmutableMap.of(1, 5L, 2, 3L), // value count
          ImmutableMap.of(1, 0L, 2, 2L), // null count
          ImmutableMap.of(), // nan count
          ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
      ))
      .build();

  private static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-2.parquet")
      .withFileSizeInBytes(0)
      .withPartition(org.apache.iceberg.TestHelpers.Row.of(Double.NaN))
      .withPartitionPath("double=NaN")
      .withMetrics(new Metrics(1L,
          null, // no column sizes
          ImmutableMap.of(1, 1L, 4, 1L), // value count
          ImmutableMap.of(1, 0L, 2, 0L), // null count
          ImmutableMap.of(4, 1L), // nan count
          ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(1L)) // upper bounds
      ))
      .build();

  private static final FileIO FILE_IO = new HadoopFileIO(new Configuration());

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();


  @Test
  public void testKryoSerialization() throws IOException {
    KryoSerializer<ManifestFile> kryo = new KryoSerializer<>(ManifestFile.class, new ExecutionConfig());

    DataOutputSerializer outputView = new DataOutputSerializer(1024);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    kryo.serialize(manifest, outputView);
    kryo.serialize(manifest.copy(), outputView);
    kryo.serialize(GenericManifestFile.copyOf(manifest).build(), outputView);

    DataInputDeserializer inputView = new DataInputDeserializer(outputView.getCopyOfBuffer());
    ManifestFile m1 = kryo.deserialize(inputView);
    ManifestFile m2 = kryo.deserialize(inputView);
    ManifestFile m3 = kryo.deserialize(inputView);

    TestHelpers.assertEquals(manifest, m1);
    TestHelpers.assertEquals(manifest, m2);
    TestHelpers.assertEquals(manifest, m3);
  }

  @Test
  public void testJavaSerialization() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(manifest);
      out.writeObject(manifest.copy());
      out.writeObject(GenericManifestFile.copyOf(manifest).build());
    }

    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (int i = 0; i < 3; i += 1) {
        Object obj = in.readObject();
        Assert.assertTrue("Should be a ManifestFile", obj instanceof ManifestFile);
        TestHelpers.assertEquals(manifest, (ManifestFile) obj);
      }
    }
  }

  private ManifestFile writeManifest(DataFile... files) throws IOException {
    File manifestFile = temp.newFile("input.m0.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = FILE_IO.newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(SPEC, outputFile);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
