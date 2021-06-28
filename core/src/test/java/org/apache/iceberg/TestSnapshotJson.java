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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;

public class TestSnapshotJson {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  public String tableLocation = "/tmp";

  // To test with relative paths enabled and disabled
  Boolean[] propertyValues = {true, false};

  @Test
  public void testJsonConversion() {
    for (Boolean propertyValue : propertyValues) {
      Snapshot expected = new BaseSnapshot(ops.io(), System.currentTimeMillis(), tableLocation, propertyValue,
          "file:/tmp/manifest1.avro", "file:/tmp/manifest2.avro");
      String json = SnapshotParser.toJson(expected);
      Snapshot snapshot = SnapshotParser.fromJson(ops.io(), json, tableLocation, propertyValue);

      Assert.assertEquals("Snapshot ID should match",
          expected.snapshotId(), snapshot.snapshotId());
      Assert.assertEquals("Files should match",
          expected.allManifests(), snapshot.allManifests());
      Assert.assertNull("Operation should be null", snapshot.operation());
      Assert.assertNull("Summary should be null", snapshot.summary());
    }
  }

  @Test
  public void testJsonConversionWithOperation() {
    long parentId = 1;
    long id = 2;
    List<ManifestFile> manifests = ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
        new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    for (Boolean propertyValue : propertyValues) {
      Snapshot expected = new BaseSnapshot(ops.io(), id, parentId, System.currentTimeMillis(),
          DataOperations.REPLACE, ImmutableMap.of("files-added", "4", "files-deleted", "100"),
          manifests, tableLocation, propertyValue);

      String json = SnapshotParser.toJson(expected);
      Snapshot snapshot = SnapshotParser.fromJson(ops.io(), json, tableLocation, propertyValue);

      Assert.assertEquals("Sequence number should default to 0 for v1",
          0, snapshot.sequenceNumber());
      Assert.assertEquals("Snapshot ID should match",
          expected.snapshotId(), snapshot.snapshotId());
      Assert.assertEquals("Timestamp should match",
          expected.timestampMillis(), snapshot.timestampMillis());
      Assert.assertEquals("Parent ID should match",
          expected.parentId(), snapshot.parentId());
      Assert.assertEquals("Manifest list should match",
          expected.manifestListLocation(), snapshot.manifestListLocation());
      Assert.assertEquals("Files should match",
          expected.allManifests(), snapshot.allManifests());
      Assert.assertEquals("Operation should match",
          expected.operation(), snapshot.operation());
      Assert.assertEquals("Summary should match",
          expected.summary(), snapshot.summary());
    }
  }

  @Test
  public void testJsonConversionWithManifestList() throws IOException {
    long parentId = 1;
    long id = 2;
    List<ManifestFile> manifests = ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
        new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    File manifestList = temp.newFile("manifests");
    Assert.assertTrue(manifestList.delete());
    manifestList.deleteOnExit();

    for (Boolean propertyValue : propertyValues) {

      try (ManifestListWriter writer = ManifestLists.write(1, Files.localOutput(manifestList), id, parentId, 0,
          tableLocation, propertyValue)) {
        writer.addAll(manifests);
      }

      Snapshot expected = new BaseSnapshot(
          ops.io(), id, 34, parentId, System.currentTimeMillis(), null, null, localInput(manifestList).location(),
          tableLocation, propertyValue);
      Snapshot inMemory = new BaseSnapshot(
          ops.io(), id, parentId, expected.timestampMillis(), null, null, manifests,
          tableLocation, propertyValue);

      Assert.assertEquals("Files should match in memory list",
          inMemory.allManifests(), expected.allManifests());

      String json = SnapshotParser.toJson(expected);
      Snapshot snapshot = SnapshotParser.fromJson(ops.io(), json, tableLocation, propertyValue);

      Assert.assertEquals("Sequence number should default to 0",
          expected.sequenceNumber(), snapshot.sequenceNumber());
      Assert.assertEquals("Snapshot ID should match",
          expected.snapshotId(), snapshot.snapshotId());
      Assert.assertEquals("Timestamp should match",
          expected.timestampMillis(), snapshot.timestampMillis());
      Assert.assertEquals("Parent ID should match",
          expected.parentId(), snapshot.parentId());
      Assert.assertEquals("Manifest list should match",
          expected.manifestListLocation(), snapshot.manifestListLocation());
      Assert.assertEquals("Files should match",
          expected.allManifests(), snapshot.allManifests());
      Assert.assertNull("Operation should be null", snapshot.operation());
      Assert.assertNull("Summary should be null", snapshot.summary());
    }
  }
}
