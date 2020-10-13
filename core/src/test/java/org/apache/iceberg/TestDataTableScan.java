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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestDataTableScan extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestDataTableScan(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testTableScanHonorsSelect() {
    TableScan scan = table.newScan().select("id");

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());
  }

  @Test
  public void testTableBothProjectAndSelect() {
    AssertHelpers.assertThrows("Cannot set projection schema when columns are selected",
        IllegalStateException.class, () -> table.newScan().select("id").project(SCHEMA.select("data")));
    AssertHelpers.assertThrows("Cannot select columns when projection schema is set",
        IllegalStateException.class, () -> table.newScan().project(SCHEMA.select("data")).select("id"));
  }

  @Test
  public void testTableScanHonorsSelectWithoutCaseSensitivity() {
    TableScan scan1 = table.newScan().caseSensitive(false).select("ID");
    // order of refinements shouldn't matter
    TableScan scan2 = table.newScan().select("ID").caseSensitive(false);

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema without case sensitivity",
        expectedSchema.asStruct(),
        scan1.schema().asStruct());

    assertEquals("A tableScan.select() should prune the schema regardless of scan refinement order",
        expectedSchema.asStruct(),
        scan2.schema().asStruct());
  }

  @Test
  public void testTableScanHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableScan scan1 = table.newScan()
        .filter(Expressions.equal("id", 5));

    try (CloseableIterable<CombinedScanTask> tasks = scan1.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }

    TableScan scan2 = table.newScan()
        .filter(Expressions.equal("id", 5))
        .ignoreResiduals();

    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }
  }
}
