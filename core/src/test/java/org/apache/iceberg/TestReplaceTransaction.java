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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestReplaceTransaction extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestReplaceTransaction(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testReplaceTransactionWithCustomSortOrder() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    SortOrder newSortOrder = SortOrder.builderFor(schema)
        .asc("id", NULLS_FIRST)
        .build();

    Map<String, String> props = Maps.newHashMap();
    Transaction replace = TestTables.beginReplace(tableDir, "test", schema, unpartitioned(), newSortOrder, props);
    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should match previous schema",
        schema.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Partition spec should have no fields",
        0, table.spec().fields().size());
    Assert.assertEquals("Table should have 2 orders", 2, table.sortOrders().size());
    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
    Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
    Assert.assertEquals("Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
    Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
    Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());
  }

  @Test
  public void testReplaceTransaction() {
    Schema newSchema = new Schema(
        required(4, "id", Types.IntegerType.get()),
        required(5, "data", Types.StringType.get()));

    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", newSchema, unpartitioned());
    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should match previous schema",
        schema.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Partition spec should have no fields",
        0, table.spec().fields().size());
    Assert.assertEquals("Table should have 1 order", 1, table.sortOrders().size());
    Assert.assertEquals("Table order ID should match", 0, table.sortOrder().orderId());
    Assert.assertTrue("Table should be unsorted", table.sortOrder().isUnsorted());
  }

  @Test
  public void testReplaceWithIncompatibleSchemaUpdate() {
    Schema newSchema = new Schema(
        required(4, "obj_id", Types.IntegerType.get()));

    Snapshot start = table.currentSnapshot();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", newSchema, unpartitioned());
    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should use new schema, not compatible with previous",
        new Schema(required(3, "obj_id", Types.IntegerType.get())).asStruct(),
        table.schema().asStruct());
  }

  @Test
  public void testReplaceWithNewPartitionSpec() {
    PartitionSpec newSpec = PartitionSpec.unpartitioned();

    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), newSpec);
    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should use new schema, not compatible with previous",
        schema.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Table should have new unpartitioned spec",
        0, table.spec().fields().size());
  }

  @Test
  public void testReplaceWithNewData() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend()
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNotNull("Table should have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should use new schema, not compatible with previous",
        schema.asStruct(), table.schema().asStruct());

    validateSnapshot(null, table.currentSnapshot(), FILE_B, FILE_C, FILE_D);
  }

  @Test
  public void testReplaceDetectsUncommittedChangeOnCommit() {
    Assert.assertEquals("Version should be 0", 0L, (long) version());

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend() // not committed
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D);

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class, "Cannot commit transaction: last operation has not committed",
        replace::commitTransaction);

    Assert.assertEquals("Version should be 0", 0L, (long) version());
  }

  @Test
  public void testReplaceDetectsUncommittedChangeOnTableCommit() {
    Assert.assertEquals("Version should be 0", 0L, (long) version());

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.table().newAppend() // not committed
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D);

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class, "Cannot commit transaction: last operation has not committed",
        replace::commitTransaction);

    Assert.assertEquals("Version should be 0", 0L, (long) version());
  }

  @Test
  public void testReplaceTransactionRetry() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend()
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    // trigger eventual transaction retry
    ((TestTables.TestTableOperations) ((BaseTransaction) replace).ops()).failCommits(1);

    replace.commitTransaction();

    table.refresh();

    Assert.assertEquals("Version should be 2", 2L, (long) version());
    Assert.assertNotNull("Table should have a current snapshot", table.currentSnapshot());
    Assert.assertEquals("Schema should use new schema, not compatible with previous",
        schema.asStruct(), table.schema().asStruct());

    validateSnapshot(null, table.currentSnapshot(), FILE_B, FILE_C, FILE_D);
  }

  @Test
  public void testReplaceTransactionConflict() {
    Snapshot start = table.currentSnapshot();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    validateSnapshot(start, table.currentSnapshot(), FILE_A);
    Set<File> manifests = Sets.newHashSet(listManifestFiles());

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend()
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    // keep failing to trigger eventual transaction failure
    ((TestTables.TestTableOperations) ((BaseTransaction) replace).ops()).failCommits(100);

    AssertHelpers.assertThrows("Should reject commit when retries are exhausted",
        CommitFailedException.class, "Injected failure",
        replace::commitTransaction);

    Assert.assertEquals("Version should be 1", 1L, (long) version());

    table.refresh();

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Assert.assertEquals("Should clean up replace manifests", manifests, Sets.newHashSet(listManifestFiles()));
  }

  @Test
  public void testReplaceToCreateAndAppend() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    // this table doesn't exist.
    Transaction replace = TestTables.beginReplace(tableDir, "test_append", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    Assert.assertTrue("Should return a transaction table",
        replace.table() instanceof BaseTransaction.TransactionTable);

    replace.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertNull("Appending in a transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    replace.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_append"));
    Assert.assertEquals("Should have 1 manifest file",
        1, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        assignFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should have one snapshot", 1, meta.snapshots().size());

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  private static Schema assignFreshIds(Schema schema) {
    AtomicInteger lastColumnId = new AtomicInteger(0);
    return TypeUtil.assignFreshIds(schema, lastColumnId::incrementAndGet);
  }
}
