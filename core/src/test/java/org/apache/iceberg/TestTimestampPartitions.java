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
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestTimestampPartitions extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestTimestampPartitions(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testPartitionAppend() throws IOException {
    Schema dateSchema = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "timestamp", Types.TimestampType.withoutZone())
    );

    PartitionSpec partitionSpec = PartitionSpec
        .builderFor(dateSchema)
        .day("timestamp", "date")
        .build();

    DataFile dataFile = DataFiles.builder(partitionSpec)
        .withPath("/path/to/data-1.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(0)
        .withPartitionPath("date=2018-06-08")
        .build();

    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    this.table = TestTables.create(tableDir, "test_date_partition", dateSchema, partitionSpec, formatVersion);

    table.newAppend()
        .appendFile(dataFile)
        .commit();
    long id = table.currentSnapshot().snapshotId();
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 1);
    validateManifestEntries(table.currentSnapshot().allManifests().get(0),
        ids(id),
        files(dataFile),
        statuses(ManifestEntry.Status.ADDED));
  }
}
