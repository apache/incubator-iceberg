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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestManifestListVersions {
  private static final String PATH = "s3://bucket/table/m1.avro";
  private static final long LENGTH = 1024L;
  private static final int SPEC_ID = 1;
  private static final long SEQ_NUM = 34L;
  private static final long MIN_SEQ_NUM = 10L;
  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final int ADDED_FILES = 2;
  private static final long ADDED_ROWS = 5292L;
  private static final int EXISTING_FILES = 343;
  private static final long EXISTING_ROWS = 857273L;
  private static final int DELETED_FILES = 1;
  private static final long DELETED_ROWS = 22910L;
  private static final List<ManifestFile.PartitionFieldSummary> PARTITION_SUMMARIES = ImmutableList.of();
  private static final ByteBuffer KEY_METADATA = null;

  private static final ManifestFile TEST_MANIFEST = new GenericManifestFile(
      PATH, LENGTH, SPEC_ID, ManifestContent.DATA, SEQ_NUM, MIN_SEQ_NUM, SNAPSHOT_ID,
      ADDED_FILES, ADDED_ROWS, EXISTING_FILES, EXISTING_ROWS, DELETED_FILES, DELETED_ROWS,
      PARTITION_SUMMARIES, KEY_METADATA);

  private static final ManifestFile TEST_DELETE_MANIFEST = new GenericManifestFile(
      PATH, LENGTH, SPEC_ID, ManifestContent.DELETES, SEQ_NUM, MIN_SEQ_NUM, SNAPSHOT_ID,
      ADDED_FILES, ADDED_ROWS, EXISTING_FILES, EXISTING_ROWS, DELETED_FILES, DELETED_ROWS,
      PARTITION_SUMMARIES, KEY_METADATA);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testV1WriteDeleteManifest() {
    AssertHelpers.assertThrows("Should fail to write a DELETE manifest to v1",
        IllegalArgumentException.class, "Cannot store delete manifests in a v1 table",
        () -> writeManifestList(TEST_DELETE_MANIFEST, 1));
  }

  @Test
  public void testV1Write() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(1);

    // v2 fields are not written and are defaulted
    Assert.assertEquals("Should not contain sequence number, default to 0", 0, manifest.sequenceNumber());
    Assert.assertEquals("Should not contain min sequence number, default to 0", 0, manifest.minSequenceNumber());

    // v1 fields are read correctly, even though order changed
    Assert.assertEquals("Path", PATH, manifest.path());
    Assert.assertEquals("Length", LENGTH, manifest.length());
    Assert.assertEquals("Spec id", SPEC_ID, manifest.partitionSpecId());
    Assert.assertEquals("Content", ManifestContent.DATA, manifest.content());
    Assert.assertEquals("Snapshot id", SNAPSHOT_ID, (long) manifest.snapshotId());
    Assert.assertEquals("Added files count", ADDED_FILES, (int) manifest.addedFilesCount());
    Assert.assertEquals("Existing files count", EXISTING_FILES, (int) manifest.existingFilesCount());
    Assert.assertEquals("Deleted files count", DELETED_FILES, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Added rows count", ADDED_ROWS, (long) manifest.addedRowsCount());
    Assert.assertEquals("Existing rows count", EXISTING_ROWS, (long) manifest.existingRowsCount());
    Assert.assertEquals("Deleted rows count", DELETED_ROWS, (long) manifest.deletedRowsCount());
  }

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(2);

    // all v2 fields should be read correctly
    Assert.assertEquals("Path", PATH, manifest.path());
    Assert.assertEquals("Length", LENGTH, manifest.length());
    Assert.assertEquals("Spec id", SPEC_ID, manifest.partitionSpecId());
    Assert.assertEquals("Content", ManifestContent.DATA, manifest.content());
    Assert.assertEquals("Sequence number", SEQ_NUM, manifest.sequenceNumber());
    Assert.assertEquals("Min sequence number", MIN_SEQ_NUM, manifest.minSequenceNumber());
    Assert.assertEquals("Snapshot id", SNAPSHOT_ID, (long) manifest.snapshotId());
    Assert.assertEquals("Added files count", ADDED_FILES, (int) manifest.addedFilesCount());
    Assert.assertEquals("Added rows count", ADDED_ROWS, (long) manifest.addedRowsCount());
    Assert.assertEquals("Existing files count", EXISTING_FILES, (int) manifest.existingFilesCount());
    Assert.assertEquals("Existing rows count", EXISTING_ROWS, (long) manifest.existingRowsCount());
    Assert.assertEquals("Deleted files count", DELETED_FILES, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Deleted rows count", DELETED_ROWS, (long) manifest.deletedRowsCount());
  }

  @Test
  public void testV1ForwardCompatibility() throws IOException {
    InputFile manifestList = writeManifestList(TEST_MANIFEST, 1);
    GenericData.Record generic = readGeneric(manifestList, V1Metadata.MANIFEST_LIST_SCHEMA);

    // v1 metadata should match even though order changed
    Assert.assertEquals("Path", PATH, generic.get("manifest_path").toString());
    Assert.assertEquals("Length", LENGTH, generic.get("manifest_length"));
    Assert.assertEquals("Spec id", SPEC_ID, generic.get("partition_spec_id"));
    Assert.assertEquals("Snapshot id", SNAPSHOT_ID, (long) generic.get("added_snapshot_id"));
    Assert.assertEquals("Added files count", ADDED_FILES, (int) generic.get("added_data_files_count"));
    Assert.assertEquals("Existing files count", EXISTING_FILES, (int) generic.get("existing_data_files_count"));
    Assert.assertEquals("Deleted files count", DELETED_FILES, (int) generic.get("deleted_data_files_count"));
    Assert.assertEquals("Added rows count", ADDED_ROWS, (long) generic.get("added_rows_count"));
    Assert.assertEquals("Existing rows count", EXISTING_ROWS, (long) generic.get("existing_rows_count"));
    Assert.assertEquals("Deleted rows count", DELETED_ROWS, (long) generic.get("deleted_rows_count"));
    Assert.assertNull("Content", generic.get(ManifestFile.MANIFEST_CONTENT.name()));
    Assert.assertNull("Sequence number", generic.get(ManifestFile.SEQUENCE_NUMBER.name()));
    Assert.assertNull("Min sequence number", generic.get(ManifestFile.MIN_SEQUENCE_NUMBER.name()));
  }

  @Test
  public void testV2ForwardCompatibility() throws IOException {
    // v2 manifest list files can be read by v1 readers, but the sequence numbers and content will be ignored.
    InputFile manifestList = writeManifestList(TEST_MANIFEST, 2);
    GenericData.Record generic = readGeneric(manifestList, V1Metadata.MANIFEST_LIST_SCHEMA);

    // v1 metadata should match even though order changed
    Assert.assertEquals("Path", PATH, generic.get("manifest_path").toString());
    Assert.assertEquals("Length", LENGTH, generic.get("manifest_length"));
    Assert.assertEquals("Spec id", SPEC_ID, generic.get("partition_spec_id"));
    Assert.assertEquals("Snapshot id", SNAPSHOT_ID, (long) generic.get("added_snapshot_id"));
    Assert.assertEquals("Added files count", ADDED_FILES, (int) generic.get("added_data_files_count"));
    Assert.assertEquals("Existing files count", EXISTING_FILES, (int) generic.get("existing_data_files_count"));
    Assert.assertEquals("Deleted files count", DELETED_FILES, (int) generic.get("deleted_data_files_count"));
    Assert.assertEquals("Added rows count", ADDED_ROWS, (long) generic.get("added_rows_count"));
    Assert.assertEquals("Existing rows count", EXISTING_ROWS, (long) generic.get("existing_rows_count"));
    Assert.assertEquals("Deleted rows count", DELETED_ROWS, (long) generic.get("deleted_rows_count"));
    Assert.assertNull("Content", generic.get(ManifestFile.MANIFEST_CONTENT.name()));
    Assert.assertNull("Sequence number", generic.get(ManifestFile.SEQUENCE_NUMBER.name()));
    Assert.assertNull("Min sequence number", generic.get(ManifestFile.MIN_SEQUENCE_NUMBER.name()));
  }

  @Test
  public void testManifestsWithoutRowStats() throws IOException {
    File manifestListFile = temp.newFile("manifest-list.avro");
    Assert.assertTrue(manifestListFile.delete());

    Collection<String> columnNamesWithoutRowStats = ImmutableList.of(
        "manifest_path", "manifest_length", "partition_spec_id", "added_snapshot_id",
        "added_data_files_count", "existing_data_files_count", "deleted_data_files_count",
        "partitions");
    Schema schemaWithoutRowStats = V1Metadata.MANIFEST_LIST_SCHEMA.select(columnNamesWithoutRowStats);

    OutputFile outputFile = Files.localOutput(manifestListFile);
    try (FileAppender<GenericData.Record> appender = Avro.write(outputFile)
        .schema(schemaWithoutRowStats)
        .named("manifest_file")
        .overwrite()
        .build()) {

      org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schemaWithoutRowStats, "manifest_file");
      GenericData.Record withoutRowStats = new GenericRecordBuilder(avroSchema)
          .set("manifest_path", "path/to/manifest.avro")
          .set("manifest_length", 1024L)
          .set("partition_spec_id", 1)
          .set("added_snapshot_id", 100L)
          .set("added_data_files_count", 2)
          .set("existing_data_files_count", 3)
          .set("deleted_data_files_count", 4)
          .set("partitions", null)
          .build();
      appender.add(withoutRowStats);
    }

    List<ManifestFile> files = ManifestLists.read(outputFile.toInputFile());
    ManifestFile manifest = Iterables.getOnlyElement(files);

    Assert.assertTrue("Added files should be present", manifest.hasAddedFiles());
    Assert.assertEquals("Added files count should match", 2, (int) manifest.addedFilesCount());
    Assert.assertNull("Added rows count should be null", manifest.addedRowsCount());

    Assert.assertTrue("Existing files should be present", manifest.hasExistingFiles());
    Assert.assertEquals("Existing files count should match", 3, (int) manifest.existingFilesCount());
    Assert.assertNull("Existing rows count should be null", manifest.existingRowsCount());

    Assert.assertTrue("Deleted files should be present", manifest.hasDeletedFiles());
    Assert.assertEquals("Deleted files count should match", 4, (int) manifest.deletedFilesCount());
    Assert.assertNull("Deleted rows count should be null", manifest.deletedRowsCount());
  }

  @Test
  public void testManifestsPartitionSummary() throws IOException {
    ByteBuffer firstSummaryLowerBound = Conversions.toByteBuffer(Types.IntegerType.get(), 10);
    ByteBuffer firstSummaryUpperBound = Conversions.toByteBuffer(Types.IntegerType.get(), 100);
    ByteBuffer secondSummaryLowerBound = Conversions.toByteBuffer(Types.IntegerType.get(), 20);
    ByteBuffer secondSummaryUpperBound = Conversions.toByteBuffer(Types.IntegerType.get(), 200);

    List<ManifestFile.PartitionFieldSummary> partitionFieldSummaries = Lists.newArrayList(
        new GenericPartitionFieldSummary(false, firstSummaryLowerBound, firstSummaryUpperBound),
        new GenericPartitionFieldSummary(true, false, secondSummaryLowerBound, secondSummaryUpperBound));
    ManifestFile manifest = new GenericManifestFile(
        PATH, LENGTH, SPEC_ID, ManifestContent.DATA, SEQ_NUM, MIN_SEQ_NUM, SNAPSHOT_ID,
        ADDED_FILES, ADDED_ROWS, EXISTING_FILES, EXISTING_ROWS, DELETED_FILES, DELETED_ROWS,
        partitionFieldSummaries, KEY_METADATA);

    InputFile manifestList = writeManifestList(manifest, 2);

    List<ManifestFile> files = ManifestLists.read(manifestList);
    ManifestFile returnedManifest = Iterables.getOnlyElement(files);
    Assert.assertEquals("Number of partition field summaries should match",
        2, returnedManifest.partitions().size());

    ManifestFile.PartitionFieldSummary first = returnedManifest.partitions().get(0);
    Assert.assertFalse("First partition field summary should not contain null", first.containsNull());
    Assert.assertNull("First partition field summary has unknown NaN", first.containsNaN());
    Assert.assertEquals("Lower bound for first partition field summary should match",
        firstSummaryLowerBound, first.lowerBound());
    Assert.assertEquals("Upper bound for first partition field summary should match",
        firstSummaryUpperBound, first.upperBound());

    ManifestFile.PartitionFieldSummary second = returnedManifest.partitions().get(1);
    Assert.assertTrue("Second partition field summary should contain null", second.containsNull());
    Assert.assertFalse("Second partition field summary should not contain NaN", second.containsNaN());
    Assert.assertEquals("Lower bound for second partition field summary should match",
        secondSummaryLowerBound, second.lowerBound());
    Assert.assertEquals("Upper bound for second partition field summary should match",
        secondSummaryUpperBound, second.upperBound());
  }

  private InputFile writeManifestList(ManifestFile manifest, int formatVersion) throws IOException {
    OutputFile manifestList = Files.localOutput(temp.newFile());
    try (FileAppender<ManifestFile> writer = ManifestLists.write(
        formatVersion, manifestList, SNAPSHOT_ID, SNAPSHOT_ID - 1, formatVersion > 1 ? SEQ_NUM : 0)) {
      writer.add(manifest);
    }
    return manifestList.toInputFile();
  }

  private GenericData.Record readGeneric(InputFile manifestList, Schema schema) throws IOException {
    try (CloseableIterable<GenericData.Record> files = Avro.read(manifestList)
        .project(schema)
        .reuseContainers(false)
        .build()) {
      List<GenericData.Record> records = Lists.newLinkedList(files);
      Assert.assertEquals("Should contain one manifest", 1, records.size());
      return records.get(0);
    }
  }

  private ManifestFile writeAndReadManifestList(int formatVersion) throws IOException {
    List<ManifestFile> manifests = ManifestLists.read(writeManifestList(TEST_MANIFEST, formatVersion));
    Assert.assertEquals("Should contain one manifest", 1, manifests.size());
    return manifests.get(0);
  }
}
