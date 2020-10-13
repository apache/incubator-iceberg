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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.PartitionSpec.unpartitioned;

@RunWith(Parameterized.class)
public class TestCreateTransaction extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestCreateTransaction(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testCreateTransaction() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_create", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_create"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_create"));

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_create");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_create"));
    Assert.assertEquals("Should have 0 manifest files",
        0, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should not have any snapshots", 0, meta.snapshots().size());
  }

  @Test
  public void testCreateAndAppendWithTransaction() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_append", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertNull("Appending in a transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_append"));
    Assert.assertEquals("Should have 1 manifest file",
        1, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should have one snapshot", 1, meta.snapshots().size());

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  @Test
  public void testCreateAndAppendWithTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_append", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    Assert.assertTrue("Should return a transaction table",
        txn.table() instanceof BaseTransaction.TransactionTable);

    txn.table().newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertNull("Appending in a transaction should not commit metadata",
        TestTables.readMetadata("test_append"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_append"));

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_append"));
    Assert.assertEquals("Should have 1 manifest file",
        1, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should have one snapshot", 1, meta.snapshots().size());

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  @Test
  public void testCreateAndUpdatePropertiesWithTransaction() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_properties", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_properties"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_properties"));

    txn.updateProperties()
        .set("test-property", "test-value")
        .commit();

    Assert.assertNull("Adding properties in a transaction should not commit metadata",
        TestTables.readMetadata("test_properties"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_properties"));

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_properties");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_properties"));
    Assert.assertEquals("Should have 0 manifest files",
        0, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should not have any snapshots", 0, meta.snapshots().size());
    Assert.assertEquals("Should have one table property", 1, meta.properties().size());
    Assert.assertEquals("Should have correct table property value",
        "test-value", meta.properties().get("test-property"));
  }

  @Test
  public void testCreateAndUpdatePropertiesWithTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_properties", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_properties"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_properties"));

    Assert.assertTrue("Should return a transaction table",
        txn.table() instanceof BaseTransaction.TransactionTable);

    txn.table().updateProperties()
        .set("test-property", "test-value")
        .commit();

    Assert.assertNull("Adding properties in a transaction should not commit metadata",
        TestTables.readMetadata("test_properties"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_properties"));

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_properties");
    Assert.assertNotNull("Table metadata should be created after transaction commits", meta);
    Assert.assertEquals("Should have metadata version 0",
        0, (int) TestTables.metadataVersion("test_properties"));
    Assert.assertEquals("Should have 0 manifest files",
        0, listManifestFiles(tableDir).size());

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), meta.schema().asStruct());
    Assert.assertEquals("Table spec should match", unpartitioned(), meta.spec());
    Assert.assertEquals("Table should not have any snapshots", 0, meta.snapshots().size());
    Assert.assertEquals("Should have one table property", 1, meta.properties().size());
    Assert.assertEquals("Should have correct table property value",
        "test-value", meta.properties().get("test-property"));
  }

  @Test
  public void testCreateDetectsUncommittedChange() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "uncommitted_change", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("uncommitted_change"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("uncommitted_change"));

    txn.updateProperties().set("test-property", "test-value"); // not committed

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class,
        "Cannot create new DeleteFiles: last operation has not committed",
        txn::newDelete);
  }

  @Test
  public void testCreateDetectsUncommittedChangeOnCommit() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "uncommitted_change", SCHEMA, unpartitioned());

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("uncommitted_change"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("uncommitted_change"));

    txn.updateProperties().set("test-property", "test-value"); // not committed

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class, "Cannot commit transaction: last operation has not committed",
        txn::commitTransaction);
  }

  @Test
  public void testCreateTransactionConflict() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Transaction txn = TestTables.beginCreate(tableDir, "test_conflict", SCHEMA, SPEC);

    // append in the transaction to ensure a manifest file is created
    txn.newAppend().appendFile(FILE_A).commit();

    Assert.assertNull("Starting a create transaction should not commit metadata",
        TestTables.readMetadata("test_conflict"));
    Assert.assertNull("Should have no metadata version",
        TestTables.metadataVersion("test_conflict"));

    Table conflict = TestTables.create(tableDir, "test_conflict", SCHEMA, unpartitioned(), formatVersion);

    Assert.assertEquals("Table schema should match with reassigned IDs",
        TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct(), conflict.schema().asStruct());
    Assert.assertEquals("Table spec should match conflict table, not transaction table",
        unpartitioned(), conflict.spec());
    Assert.assertFalse("Table should not have any snapshots",
        conflict.snapshots().iterator().hasNext());

    AssertHelpers.assertThrows("Transaction commit should fail",
        CommitFailedException.class, "Commit failed: table was updated", txn::commitTransaction);

    Assert.assertEquals("Should clean up metadata", Sets.newHashSet(), Sets.newHashSet(listManifestFiles(tableDir)));
  }
}
