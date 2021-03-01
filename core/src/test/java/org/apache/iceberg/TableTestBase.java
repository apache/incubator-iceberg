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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TableTestBase {
  // Schema passed to create tables
  public static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get()),
      required(4, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_A2 = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a-2.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DeleteFile FILE_A_DELETES = FileMetadata.deleteFileBuilder(SPEC)
      .ofPositionDeletes()
      .withPath("/path/to/data-a-deletes.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=1") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DeleteFile FILE_B_DELETES = FileMetadata.deleteFileBuilder(SPEC)
      .ofPositionDeletes()
      .withPath("/path/to/data-b-deletes.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=1") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=2") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_D = DataFiles.builder(SPEC)
      .withPath("/path/to/data-d.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=3") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  static final FileIO FILE_IO = new TestTables.LocalFileIO();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected File tableDir = null;
  protected File metadataDir = null;
  public TestTables.TestTable table = null;

  protected final int formatVersion;
  @SuppressWarnings("checkstyle:MemberName")
  protected final Assertions V1Assert;
  @SuppressWarnings("checkstyle:MemberName")
  protected final Assertions V2Assert;

  public TableTestBase(int formatVersion) {
    this.formatVersion = formatVersion;
    this.V1Assert = new Assertions(1, formatVersion);
    this.V2Assert = new Assertions(2, formatVersion);
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, SPEC);
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  List<File> listManifestFiles() {
    return listManifestFiles(tableDir);
  }

  List<File> listManifestFiles(File tableDirToList) {
    return Lists.newArrayList(new File(tableDirToList, "metadata").listFiles((dir, name) ->
        !name.startsWith("snap") &&
            !name.startsWith("partitionStats") &&
            Files.getFileExtension(name).equalsIgnoreCase("avro")));
  }

  protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec, formatVersion);
  }

  TestTables.TestTable load() {
    return TestTables.load(tableDir, "test");
  }

  Integer version() {
    return TestTables.metadataVersion("test");
  }

  public TableMetadata readMetadata() {
    return TestTables.readMetadata("test");
  }

  ManifestFile writeManifest(DataFile... files) throws IOException {
    return writeManifest(null, files);
  }

  ManifestFile writeManifest(Long snapshotId, DataFile... files) throws IOException {
    File manifestFile = temp.newFile("input.m0.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestFile writeManifest(String fileName, ManifestEntry<?>... entries) throws IOException {
    return writeManifest(null, fileName, entries);
  }

  ManifestFile writeManifest(Long snapshotId, ManifestEntry<?>... entries) throws IOException {
    return writeManifest(snapshotId, "input.m0.avro", entries);
  }

  @SuppressWarnings("unchecked")
  <F extends ContentFile<F>> ManifestFile writeManifest(Long snapshotId, String fileName, ManifestEntry<?>... entries)
      throws IOException {
    File manifestFile = temp.newFile(fileName);
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<F> writer;
    if (entries[0].file() instanceof DataFile) {
      writer = (ManifestWriter<F>) ManifestFiles.write(
          formatVersion, table.spec(), outputFile, snapshotId);
    } else {
      writer = (ManifestWriter<F>) ManifestFiles.writeDeleteManifest(
          formatVersion, table.spec(), outputFile, snapshotId);
    }
    try {
      for (ManifestEntry<?> entry : entries) {
        writer.addEntry((ManifestEntry<F>) entry);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestFile writeManifestWithName(String name, DataFile... files) throws IOException {
    File manifestFile = temp.newFile(name + ".avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, null);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestEntry<DataFile> manifestEntry(ManifestEntry.Status status, Long snapshotId, DataFile file) {
    GenericManifestEntry<DataFile> entry = new GenericManifestEntry<>(table.spec().partitionType());
    switch (status) {
      case ADDED:
        return entry.wrapAppend(snapshotId, file);
      case EXISTING:
        return entry.wrapExisting(snapshotId, 0L, file);
      case DELETED:
        return entry.wrapDelete(snapshotId, file);
      default:
        throw new IllegalArgumentException("Unexpected entry status: " + status);
    }
  }

  void validateSnapshot(Snapshot old, Snapshot snap, DataFile... newFiles) {
    validateSnapshot(old, snap, null, newFiles);
  }

  void validateSnapshot(Snapshot old, Snapshot snap, long sequenceNumber, DataFile... newFiles) {
    validateSnapshot(old, snap, (Long) sequenceNumber, newFiles);
  }

  void validateSnapshot(Snapshot old, Snapshot snap, Long sequenceNumber, DataFile... newFiles) {
    Assert.assertEquals("Should not change delete manifests",
        old != null ? Sets.newHashSet(old.deleteManifests()) : ImmutableSet.of(),
        Sets.newHashSet(snap.deleteManifests()));
    List<ManifestFile> oldManifests = old != null ? old.dataManifests() : ImmutableList.of();

    // copy the manifests to a modifiable list and remove the existing manifests
    List<ManifestFile> newManifests = Lists.newArrayList(snap.dataManifests());
    for (ManifestFile oldManifest : oldManifests) {
      Assert.assertTrue("New snapshot should contain old manifests",
          newManifests.remove(oldManifest));
    }

    Assert.assertEquals("Should create 1 new manifest and reuse old manifests",
        1, newManifests.size());
    ManifestFile manifest = newManifests.get(0);

    long id = snap.snapshotId();
    Iterator<String> newPaths = paths(newFiles).iterator();

    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      if (sequenceNumber != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", sequenceNumber, entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected", newPaths.next(), file.path().toString());
      Assert.assertEquals("File's snapshot ID should match", id, (long) entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", newPaths.hasNext());
  }

  void validateTableFiles(Table tbl, DataFile... expectedFiles) {
    Set<CharSequence> expectedFilePaths = Sets.newHashSet();
    for (DataFile file : expectedFiles) {
      expectedFilePaths.add(file.path());
    }
    Set<CharSequence> actualFilePaths = Sets.newHashSet();
    for (FileScanTask task : tbl.newScan().planFiles()) {
      actualFilePaths.add(task.file().path());
    }
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
  }

  List<String> paths(DataFile... dataFiles) {
    List<String> paths = Lists.newArrayListWithExpectedSize(dataFiles.length);
    for (DataFile file : dataFiles) {
      paths.add(file.path().toString());
    }
    return paths;
  }

  void validateManifest(ManifestFile manifest,
                        Iterator<Long> ids,
                        Iterator<DataFile> expectedFiles) {
    validateManifest(manifest, null, ids, expectedFiles, null);
  }

  void validateManifest(ManifestFile manifest,
                        Iterator<Long> seqs,
                        Iterator<Long> ids,
                        Iterator<DataFile> expectedFiles) {
    validateManifest(manifest, seqs, ids, expectedFiles, null);
  }

  void validateManifest(ManifestFile manifest,
                        Iterator<Long> seqs,
                        Iterator<Long> ids,
                        Iterator<DataFile> expectedFiles,
                        Iterator<ManifestEntry.Status> statuses) {
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      if (seqs != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", seqs.next(), entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          ids.next(), entry.snapshotId());
      if (statuses != null) {
        Assert.assertEquals("Status should match expected",
            statuses.next(), entry.status());
      }
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  void validateDeleteManifest(ManifestFile manifest,
                              Iterator<Long> seqs,
                              Iterator<Long> ids,
                              Iterator<DeleteFile> expectedFiles,
                              Iterator<ManifestEntry.Status> statuses) {
    for (ManifestEntry<DeleteFile> entry : ManifestFiles.readDeleteManifest(manifest, FILE_IO, null).entries()) {
      DeleteFile file = entry.file();
      DeleteFile expected = expectedFiles.next();
      if (seqs != null) {
        V1Assert.assertEquals("Sequence number should default to 0", 0, entry.sequenceNumber().longValue());
        V2Assert.assertEquals("Sequence number should match expected", seqs.next(), entry.sequenceNumber());
      }
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          ids.next(), entry.snapshotId());
      Assert.assertEquals("Status should match expected",
          statuses.next(), entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  static void validateManifestEntries(ManifestFile manifest,
                                      Iterator<Long> ids,
                                      Iterator<DataFile> expectedFiles,
                                      Iterator<ManifestEntry.Status> expectedStatuses) {
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      final ManifestEntry.Status expectedStatus = expectedStatuses.next();
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          ids.next(), entry.snapshotId());
      Assert.assertEquals("Entry status should match expected ID",
          expectedStatus, entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  static Iterator<ManifestEntry.Status> statuses(ManifestEntry.Status... statuses) {
    return Iterators.forArray(statuses);
  }

  static Iterator<Long> seqs(long... seqs) {
    return LongStream.of(seqs).iterator();
  }

  static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  static Iterator<DataFile> files(DataFile... files) {
    return Iterators.forArray(files);
  }

  static Iterator<DeleteFile> files(DeleteFile... files) {
    return Iterators.forArray(files);
  }

  static Iterator<DataFile> files(ManifestFile manifest) {
    return ManifestFiles.read(manifest, FILE_IO).iterator();
  }

  /**
   * Used for assertions that only apply if the table version is v2.
   */
  protected static class Assertions {
    private final boolean enabled;

    private Assertions(int validForVersion, int formatVersion) {
      this.enabled = validForVersion == formatVersion;
    }

    void assertEquals(String context, int expected, int actual) {
      if (enabled) {
        Assert.assertEquals(context, expected, actual);
      }
    }

    void assertEquals(String context, long expected, long actual) {
      if (enabled) {
        Assert.assertEquals(context, expected, actual);
      }
    }

    void assertEquals(String context, Object expected, Object actual) {
      if (enabled) {
        Assert.assertEquals(context, expected, actual);
      }
    }
  }
}
