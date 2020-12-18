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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;

public abstract class TestForwardCompatibility {
  private static final Configuration CONF = new Configuration();

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  // create a spec for the schema that uses a "zero" transform that produces all 0s
  private static final PartitionSpec UNKNOWN_SPEC = PartitionSpecParser.fromJson(SCHEMA,
      "{ \"spec-id\": 0, \"fields\": [ { \"name\": \"id_zero\", \"transform\": \"zero\", \"source-id\": 1 } ] }");
  // create a fake spec to use to write table metadata
  private static final PartitionSpec FAKE_SPEC = PartitionSpecParser.fromJson(SCHEMA,
      "{ \"spec-id\": 0, \"fields\": [ { \"name\": \"id_zero\", \"transform\": \"identity\", \"source-id\": 1 } ] }");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestForwardCompatibility.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestForwardCompatibility.spark;
    TestForwardCompatibility.spark = null;
    currentSpark.stop();
  }

  protected abstract <T> MemoryStream<T> newMemoryStream(int id, SQLContext sqlContext, Encoder<T> encoder);

  protected abstract <T> void send(List<T> records, MemoryStream<T> stream);

  @Test
  public void testSparkWriteFailsUnknownTransform() throws IOException {
    File parent = temp.newFolder("avro");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, UNKNOWN_SPEC, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    AssertHelpers.assertThrows("Should reject write with unsupported transform",
        UnsupportedOperationException.class, "Cannot write using unsupported transforms: zero",
        () -> df.select("id", "data").write()
            .format("iceberg")
            .mode("append")
            .save(location.toString()));
  }

  @Test
  public void testSparkStreamingWriteFailsUnknownTransform() throws IOException {
    File parent = temp.newFolder("avro");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();
    File checkpoint = new File(parent, "checkpoint");
    checkpoint.mkdirs();

    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, UNKNOWN_SPEC, location.toString());

    MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
    StreamingQuery query = inputStream.toDF()
        .selectExpr("value AS id", "CAST (value AS STRING) AS data")
        .writeStream()
        .outputMode("append")
        .format("iceberg")
        .option("checkpointLocation", checkpoint.toString())
        .option("path", location.toString())
        .start();

    List<Integer> batch1 = Lists.newArrayList(1, 2);
    send(batch1, inputStream);

    AssertHelpers.assertThrows("Should reject streaming write with unsupported transform",
        StreamingQueryException.class, "Cannot write using unsupported transforms: zero",
        query::processAllAvailable);
  }

  @Test
  public void testSparkCanReadUnknownTransform() throws IOException {
    File parent = temp.newFolder("avro");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(SCHEMA, UNKNOWN_SPEC, location.toString());

    // enable snapshot inheritance to avoid rewriting the manifest with an unknown transform
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    List<GenericData.Record> expected = RandomData.generateList(table.schema(), 100, 1L);

    File parquetFile = new File(dataFolder,
        FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
    FileAppender<GenericData.Record> writer = Parquet.write(localOutput(parquetFile))
        .schema(table.schema())
        .build();
    try {
      writer.addAll(expected);
    } finally {
      writer.close();
    }

    DataFile file = DataFiles.builder(FAKE_SPEC)
        .withInputFile(localInput(parquetFile))
        .withMetrics(writer.metrics())
        .withPartitionPath("id_zero=0")
        .build();

    OutputFile manifestFile = localOutput(FileFormat.AVRO.addExtension(temp.newFile().toString()));
    ManifestWriter manifestWriter = ManifestFiles.write(FAKE_SPEC, manifestFile, table.location(), table.properties());
    try {
      manifestWriter.add(file);
    } finally {
      manifestWriter.close();
    }

    table.newFastAppend().appendManifest(manifestWriter.toManifestFile()).commit();

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Should contain 100 rows", 100, rows.size());

    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(table.schema().asStruct(), expected.get(i), rows.get(i));
    }
  }
}
