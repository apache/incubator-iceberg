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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReader extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestManifestReader(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestReaderWithEmptyInheritableMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 1000L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.location(),
        table.properties())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(Status.EXISTING, entry.status());
      Assert.assertEquals(FILE_A.path(), entry.file().path());
      Assert.assertEquals(1000L, (long) entry.snapshotId());
    }
  }

  @Test
  public void testReaderWithFilterWithoutSelect() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.location(), table.properties())
        .filterRows(Expressions.equal("id", 0))) {
      List<String> files = Streams.stream(reader)
          .map(file -> file.path().toString())
          .collect(Collectors.toList());

      // note that all files are returned because the reader returns data files that may match, and the partition is
      // bucketing by data, which doesn't help filter files
      Assert.assertEquals("Should read the expected files",
          Lists.newArrayList(FILE_A.path(), FILE_B.path(), FILE_C.path()), files);
    }
  }

  @Test
  public void testInvalidUsage() throws IOException {
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AssertHelpers.assertThrows(
        "Should not be possible to read manifest without explicit snapshot ids and inheritable metadata",
        IllegalArgumentException.class, "Cannot read from ManifestFile with null (unassigned) snapshot ID",
        () -> ManifestFiles.read(manifest, FILE_IO, table.location(), table.properties()));
  }

  @Test
  public void testManifestReaderWithPartitionMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.location(),
        table.properties())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(1, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("data_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());
    }
  }

  @Test
  public void testManifestReaderWithUpdatedPartitionMetadataForV1Table() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(table.schema())
        .bucket("id", 8)
        .bucket("data", 16)
        .build();
    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO,
        table.location(), table.properties())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(2, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("id_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());

      Assert.assertEquals(1001, fields.get(1).fieldId());
      Assert.assertEquals("data_bucket", fields.get(1).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(1).type());
    }
  }

  @Test
  public void testDataFilePositions() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.location(),
        table.properties())) {
      long expectedPos = 0L;
      for (DataFile file : reader) {
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        expectedPos += 1;
      }
    }
  }

  @Test
  public void testDeleteFilePositions() throws IOException {
    Assume.assumeTrue("Delete files only work for format version 2", formatVersion == 2);
    ManifestFile manifest = writeDeleteManifest(formatVersion, 1000L, FILE_A_DELETES, FILE_B_DELETES);
    try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, FILE_IO, null, null, null)) {
      long expectedPos = 0L;
      for (DeleteFile file : reader) {
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        expectedPos += 1;
      }
    }
  }
}
