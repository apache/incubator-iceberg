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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTableMetadataSerialization extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestTableMetadataSerialization(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testSerialization() throws Exception {
    // add a commit to the metadata so there is at least one snapshot, and history
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata meta = table.ops().current();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ObjectOutputStream writer = new ObjectOutputStream(out)) {
      writer.writeObject(meta);
    }

    TableMetadata result;
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ObjectInputStream reader = new ObjectInputStream(in)) {
      result = (TableMetadata) reader.readObject();
    }

    Assert.assertEquals("Metadata file location should match",
        meta.metadataFileLocation(), result.metadataFileLocation());
    Assert.assertEquals("UUID should match", meta.uuid(), result.uuid());
    Assert.assertEquals("Location should match", meta.location(), result.location());
    Assert.assertEquals("Last updated should match", meta.lastUpdatedMillis(), result.lastUpdatedMillis());
    Assert.assertEquals("Last column id", meta.lastColumnId(), result.lastColumnId());
    Assert.assertEquals("Schema should match", meta.schema().asStruct(), result.schema().asStruct());
    Assert.assertEquals("Spec should match", meta.defaultSpecId(), result.defaultSpecId());
    Assert.assertEquals("Spec list should match", meta.specs(), result.specs());
    Assert.assertEquals("Properties should match", meta.properties(), result.properties());
    Assert.assertEquals("Current snapshot ID should match",
        meta.currentSnapshot().snapshotId(), result.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshots should match",
        Lists.transform(meta.snapshots(), Snapshot::snapshotId),
        Lists.transform(result.snapshots(), Snapshot::snapshotId));
    Assert.assertEquals("History should match", meta.snapshotLog(), result.snapshotLog());
  }
}
