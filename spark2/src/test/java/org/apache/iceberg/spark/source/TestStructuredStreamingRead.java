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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestStructuredStreamingRead {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;
  private static Path parent = null;
  private static File tableLocation = null;
  private static Table table = null;
  private static List<List<SimpleRecord>> expected = null;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() throws Exception {
    TestStructuredStreamingRead.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate();

    parent = Files.createTempDirectory("test");
    tableLocation = new File(parent.toFile(), "table");
    tableLocation.mkdir();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    table = tables.create(SCHEMA, spec, tableLocation.toString());

    expected = Lists.newArrayList(
        Lists.newArrayList(new SimpleRecord(1, "1")),
        Lists.newArrayList(new SimpleRecord(2, "2")),
        Lists.newArrayList(new SimpleRecord(3, "3")),
        Lists.newArrayList(new SimpleRecord(4, "4"))
    );

    // Write records one by one to generate 4 snapshots.
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> df = spark.createDataFrame(l, SimpleRecord.class);
      df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation.toString());
    }
    table.refresh();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead.spark;
    TestStructuredStreamingRead.spark = null;
    currentSpark.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFromStart() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    // Getting all appends from initial snapshot.
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 0, true), Long.MAX_VALUE);
    Assert.assertEquals("Batches with unlimited size control should have 4 snapshots", 4, pendingBatches.size());

    List<Long> batchSnapshotIds = pendingBatches.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertEquals("Snapshot id of each batch should match snapshot id of table", snapshotIds, batchSnapshotIds);

    // Getting appends from initial snapshot with last index, 1st snapshot should be an empty batch.
    List<MicroBatch> pendingBatches1 = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 1, true), Long.MAX_VALUE);

    Assert.assertEquals("Batches with unlimited size control from initial id should have 3 snapshots",
        3, pendingBatches1.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFrom2ndSnapshot() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    // Getting appends from 2nd snapshot, 1st snapshot should be filtered out.
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(1), 0, false), Long.MAX_VALUE);

    Assert.assertEquals(3, pendingBatches.size());
    List<Long> batchSnapshotIds = pendingBatches.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertFalse("1st snapshot should be filtered", batchSnapshotIds.contains(initialSnapshotId));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFromLastSnapshot() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Getting appends from last snapshot with last index, should get an empty batch.
    long lastSnapshotId = snapshotIds.get(3);
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(lastSnapshotId, 0, false), Long.MAX_VALUE);

    Assert.assertEquals("Should only have 1 batch with last snapshot", 1, pendingBatches.size());
    MicroBatch batch = pendingBatches.get(0);
    Assert.assertTrue("Batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);
    Assert.assertEquals("Batch endFileIndex should be euqal to start", 1, batch.endFileIndex());
    Assert.assertEquals("Batch should have 1 task", 1, batch.tasks().size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit1000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // The size of each data file is around 600 bytes.
    // Max size set to 1000. One additional batch will be added because the left size is less than file size,
    // MicroBatchBuilder will add one more to avoid stuck.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 0, true), 1000);

    Assert.assertEquals("Should have 2 batches", 2L, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("1st batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertTrue("1st batch should be the last index of 1st snapshot", batch.lastIndexOfSnapshot());
    Assert.assertEquals("1st batch should only have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("1st batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);

    MicroBatch batch1 = rateLimitedBatches.get(1);
    Assert.assertEquals("2nd batch's endFileIndex should reach to the end of file indexes", 1, batch1.endFileIndex());
    Assert.assertTrue("2nd batch should be the last of 2nd snapshot", batch1.lastIndexOfSnapshot());
    Assert.assertEquals("2nd batch should only have 1 task", 1, batch1.tasks().size());
    Assert.assertTrue("2nd batch's size should be aound 600", batch1.sizeInBytes() < 1000 && batch1.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit100() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // Max size less than file size, should have one batch added to avoid stuck.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(1), 1, false), 100);

    Assert.assertEquals("Should only have 1 batch", 1, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("Batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertTrue("Batch should be the last of 1st snapshot", batch.lastIndexOfSnapshot());
    Assert.assertEquals("Batch should have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("Batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit10000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // Max size set to 10000, the last left batch will be added.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(2), 1, false), 10000);

    Assert.assertEquals("Should only have 1 batch", 1, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("Batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertEquals("Batch should have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("Batch should have 1 task", batch.lastIndexOfSnapshot());
    Assert.assertTrue("Batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithDefaultRateLimit() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Default max size per batch, this will consume all the data of this table.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);
    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());

    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals("Start offset's snapshot id should be 1st snapshot id",
        snapshotIds.get(0).longValue(), start.snapshotId());
    Assert.assertEquals("Start offset's index should be the start index of 1st snapshot", 0, start.position());
    Assert.assertTrue("Start offset's snapshot should do a full table scan", start.shouldScanAllFiles());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot should be the last snapshot id",
        snapshotIds.get(3).longValue(), end.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end.position());
    Assert.assertFalse("End offset's snapshot should not do a full table scan", end.shouldScanAllFiles());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset should be same to start offset since there's no more batches to consume",
        end1, end);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithRateLimit1000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Max size to 1000, this will generate two MicroBatches per consuming.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(0), 0, true), start);

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(1), 1, false), end);

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(3), 1, false), end1);

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(end2, end1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithRateLimit100() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Max size to 100, will generate 1 MicroBatch per consuming.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "100"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(0), 0, true), start);

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(0), 1, true), end);

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(1), 1, false), end1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpecifyInvalidSnapshotId() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();
    IcebergSource source = new IcebergSource();

    // test invalid snapshot id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "start-snapshot-id", "-1"));
    AssertHelpers.assertThrows("Test invalid snapshot id",
        IllegalArgumentException.class, "The option start-snapshot-id -1 is not an ancestor",
        () -> source.createMicroBatchReader(Optional.empty(), checkpoint.toString(), options));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpecifySnapshotId() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // test specify snapshot-id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "start-snapshot-id", snapshotIds.get(1).toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(1), 0, false), start);

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(2), 1, false), end);

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(3), 1, false), end1);

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    validateOffset(new StreamingOffset(snapshotIds.get(3), 1, false), end2);
  }

  @Test
  public void testStreamingRead() {
    Dataset<Row> read = spark.readStream()
        .format("iceberg")
        .load(tableLocation.toString());
    DataStreamWriter<Row> streamWriter = read.writeStream()
        .format("memory")
        .outputMode("append")
        .queryName("memoryStream");

    try {
      StreamingQuery query = streamWriter.start();
      query.processAllAvailable();

      List<SimpleRecord> actual = spark.table("memoryStream")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();
      List<SimpleRecord> expectedResult = expected.stream().flatMap(List::stream).collect(Collectors.toList());
      validateResult(expectedResult, actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @Test
  public void testStreamingReadWithSpecifiedSnapshotId() {
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    Dataset<Row> read = spark.readStream()
        .format("iceberg")
        .option("start-snapshot-id", snapshotIds.get(1).toString())
        .load(tableLocation.toString());

    try {
      StreamingQuery query = read.writeStream()
          .format("memory")
          .outputMode("append")
          .queryName("memoryStream1")
          .start();

      query.processAllAvailable();

      List<SimpleRecord> actual = spark.table("memoryStream1")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      List<SimpleRecord> expectedResult = expected.stream().flatMap(List::stream)
          .filter(d -> !d.equals(expected.get(0).get(0))).collect(Collectors.toList());
      validateResult(expectedResult, actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  private static void validateOffset(StreamingOffset expectedResult, StreamingOffset actualResult) {
    Assert.assertEquals(String.format("Actual StreamingOffset %s doesn't equal to the expected StreamingOffset %s",
        actualResult, expectedResult), expectedResult, actualResult);
  }

  private static void validateResult(List<SimpleRecord> expectedResult, List<SimpleRecord> actualResult) {
    expectedResult.sort(Comparator.comparingInt(SimpleRecord::getId));
    actualResult.sort(Comparator.comparingInt(SimpleRecord::getId));

    Assert.assertEquals("Streaming result should be matched", expectedResult, actualResult);
  }
}
