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

import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestEntriesMetadataTable extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestEntriesMetadataTable(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEntriesTable() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);

    Schema expectedSchema = ManifestEntry.getSchema(table.spec().partitionType());

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        entriesTable.schema().asStruct());
  }

  @Test
  public void testEntriesTableScan() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    TableScan scan = entriesTable.newScan();

    Schema expectedSchema = ManifestEntry.getSchema(table.spec().partitionType());

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());

    FileScanTask file = Iterables.getOnlyElement(scan.planFiles());
    Assert.assertEquals("Data file should be the table's manifest",
        Iterables.getOnlyElement(table.currentSnapshot().allManifests()).path(), file.file().path());
    Assert.assertEquals("Should contain 2 data file records", 2, file.file().recordCount());
  }

  @Test
  public void testSplitPlanningWithMetadataSplitSizeProperty() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    // set the split size to a large value so that both manifests are in 1 split
    table.updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(128 * 1024 * 1024))
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);

    Assert.assertEquals(1, Iterables.size(entriesTable.newScan().planTasks()));

    // set the split size to a small value so that manifests end up in different splits
    table.updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(1))
        .commit();

    Assert.assertEquals(2, Iterables.size(entriesTable.newScan().planTasks()));

    // override the table property with a large value so that both manifests are in 1 split
    TableScan scan = entriesTable.newScan()
        .option(TableProperties.SPLIT_SIZE, String.valueOf(128 * 1024 * 1024));

    Assert.assertEquals(1, Iterables.size(scan.planTasks()));
  }

  @Test
  public void testSplitPlanningWithDefaultMetadataSplitSize() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    int splitSize = (int) TableProperties.METADATA_SPLIT_SIZE_DEFAULT; // default split size is 32 MB

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    Assert.assertEquals(1, entriesTable.currentSnapshot().allManifests().size());

    int expectedSplits =
        ((int) entriesTable.currentSnapshot().allManifests().get(0).length() + splitSize - 1) / splitSize;

    TableScan scan = entriesTable.newScan();

    Assert.assertEquals(expectedSplits, Iterables.size(scan.planTasks()));
  }

}
