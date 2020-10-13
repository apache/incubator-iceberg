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

import org.apache.iceberg.ManifestEntry.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDeleteFiles extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestDeleteFiles(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testMultipleDeletes() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .commit();

    Assert.assertEquals("Metadata should be at version 1", 1L, (long) version());
    Snapshot append = readMetadata().currentSnapshot();
    validateSnapshot(null, append, FILE_A, FILE_B, FILE_C);

    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Assert.assertEquals("Metadata should be at version 2", 2L, (long) version());
    Snapshot delete = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete.allManifests().size());
    validateManifestEntries(delete.allManifests().get(0),
        ids(delete.snapshotId(), append.snapshotId(), append.snapshotId()),
        files(FILE_A, FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING, Status.EXISTING));

    table.newDelete()
        .deleteFile(FILE_B)
        .commit();

    Assert.assertEquals("Metadata should be at version 3", 3L, (long) version());
    Snapshot delete2 = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete2.allManifests().size());
    validateManifestEntries(delete2.allManifests().get(0),
        ids(delete2.snapshotId(), append.snapshotId()),
        files(FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING));
  }
}
