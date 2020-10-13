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
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestWriter extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestManifestWriter(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestStats() throws IOException {
    ManifestFile manifest = writeManifest(
        "manifest.avro",
        manifestEntry(Status.ADDED, null, newFile(10)),
        manifestEntry(Status.ADDED, null, newFile(20)),
        manifestEntry(Status.ADDED, null, newFile(5)),
        manifestEntry(Status.ADDED, null, newFile(5)),
        manifestEntry(Status.EXISTING, null, newFile(15)),
        manifestEntry(Status.EXISTING, null, newFile(10)),
        manifestEntry(Status.EXISTING, null, newFile(1)),
        manifestEntry(Status.DELETED, null, newFile(5)),
        manifestEntry(Status.DELETED, null, newFile(2)));

    Assert.assertTrue("Added files should be present", manifest.hasAddedFiles());
    Assert.assertEquals("Added files count should match", 4, (int) manifest.addedFilesCount());
    Assert.assertEquals("Added rows count should match", 40L, (long) manifest.addedRowsCount());

    Assert.assertTrue("Existing files should be present", manifest.hasExistingFiles());
    Assert.assertEquals("Existing files count should match", 3, (int) manifest.existingFilesCount());
    Assert.assertEquals("Existing rows count should match", 26L, (long) manifest.existingRowsCount());

    Assert.assertTrue("Deleted files should be present", manifest.hasDeletedFiles());
    Assert.assertEquals("Deleted files count should match", 2, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Deleted rows count should match", 7L, (long) manifest.deletedRowsCount());
  }

  private DataFile newFile(long recordCount) {
    String fileName = UUID.randomUUID().toString();
    return DataFiles.builder(SPEC)
        .withPath("data_bucket=0/" + fileName + ".parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(recordCount)
        .build();
  }
}
