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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMetadataTableScans extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMetadataTableScans(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestsTableAlwaysIgnoresResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);

    TableScan scan = manifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (FileScanTask task : tasks) {
        Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
      }
    }
  }

  @Test
  public void testDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan1 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);

    TableScan scan1 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);

    TableScan scan1 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);

    TableScan scan1 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllManifestsTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan1 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testDataFilesTableSelection() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .select("content", "record_count");
    validateTaskScanResiduals(scan, false);
    Types.StructType expected = new Schema(
        optional(134, "content", Types.IntegerType.get(),
                 "Contents of the file: 0=data, 1=position deletes, 2=equality deletes"),
        required(103, "record_count", Types.LongType.get(), "Number of records in the file")
    ).asStruct();
    Assert.assertEquals(expected, scan.schema().asStruct());
  }

  private void validateTaskScanResiduals(TableScan scan, boolean ignoreResiduals) throws IOException {
    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          if (ignoreResiduals) {
            Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
          } else {
            Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
          }
        }
      }
    }
  }
}
