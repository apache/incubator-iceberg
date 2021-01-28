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

package org.apache.iceberg.hadoop;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

public class TestHadoopCommitsBase extends HadoopTableTestBase {

  @Test
  public void testCreateTable() throws Exception {
    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
        .bucket("data", 16)
        .build();

    Assert.assertEquals("Table schema should match schema with reassigned ids",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Table partition spec should match with reassigned ids",
        expectedSpec, table.spec());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    Assert.assertTrue("Table location should exist",
        tableDir.exists());
    Assert.assertTrue("Should create metadata folder",
        metadataDir.exists() && metadataDir.isDirectory());
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());
    Assert.assertTrue("Should create version hint file",
        versionHintFile.exists());
    Assert.assertEquals("Should write the current version to the hint file",
        1, readVersionHint());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    Assert.assertEquals("Table schema should match schema with reassigned ids",
        UPDATED_SCHEMA.asStruct(), table.schema().asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testSchemaUpdateComplexType() throws Exception {
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    Types.StructType complexColumn = Types.StructType.of(
        required(0, "w", Types.IntegerType.get()),
        required(1, "x", Types.StringType.get()),
        required(2, "y", Types.BooleanType.get()),
        optional(3, "z", Types.MapType.ofOptional(
            0, 1, Types.IntegerType.get(), Types.StringType.get()
        ))
    );
    Schema updatedSchema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()),
        optional(3, "complex", Types.StructType.of(
            required(4, "w", Types.IntegerType.get()),
            required(5, "x", Types.StringType.get()),
            required(6, "y", Types.BooleanType.get()),
            optional(7, "z", Types.MapType.ofOptional(
                8, 9, Types.IntegerType.get(), Types.StringType.get()
            ))
        ))
    );

    table.updateSchema()
        .addColumn("complex", complexColumn)
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());
    Assert.assertEquals("Table schema should match schema with reassigned ids",
        updatedSchema.asStruct(), table.schema().asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testFailedCommit() throws Exception {
    // apply the change to metadata without committing
    UpdateSchema update = table.updateSchema().addColumn("n", Types.IntegerType.get());
    update.apply();

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    version(2).createNewFile();

    AssertHelpers.assertThrows("Should fail to commit change based on v1 when v2 exists",
        CommitFailedException.class, "Version 2 already exists", update::commit);

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testStaleMetadata() throws Exception {
    Table tableCopy = tables.load(tableLocation);

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    // prepare changes on the copy without committing
    UpdateSchema updateCopy = tableCopy.updateSchema()
        .addColumn("m", Types.IntegerType.get());
    updateCopy.apply();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertNotEquals("Unmodified copy should be out of date after update",
        table.schema().asStruct(), tableCopy.schema().asStruct());

    // update the table
    tableCopy.refresh();

    Assert.assertEquals("Copy should be back in sync",
        table.schema().asStruct(), tableCopy.schema().asStruct());

    AssertHelpers.assertThrows("Should fail with stale base metadata",
        CommitFailedException.class, "based on stale table metadata", updateCopy::commit);

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testStaleVersionHint() throws Exception {
    Table stale = tables.load(tableLocation);

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    Assert.assertNotEquals("Stable table schema should not match",
        UPDATED_SCHEMA.asStruct(), stale.schema().asStruct());

    // roll the version hint back to 1
    replaceVersionHint(1);

    Table reloaded = tables.load(tableLocation);
    Assert.assertEquals("Updated schema for newly loaded table should match",
        UPDATED_SCHEMA.asStruct(), reloaded.schema().asStruct());

    stale.refresh();
    Assert.assertEquals("Refreshed schema for stale table should match",
        UPDATED_SCHEMA.asStruct(), reloaded.schema().asStruct());
  }

  @Test
  public void testFastAppend() throws Exception {
    // first append
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 1 file", 1, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain only one Avro manifest file", 1, manifests.size());

    // second append
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Assert.assertTrue("Should create v3 for the update",
        version(3).exists() && version(3).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        3, readVersionHint());

    tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 2 files", 2, tasks.size());

    Assert.assertEquals("Should contain 2 Avro manifest files",
        2, listManifestFiles().size());

    TableMetadata metadata = readMetadataVersion(3);
    Assert.assertEquals("Current snapshot should contain 2 manifests",
        2, metadata.currentSnapshot().allManifests().size());
  }

  @Test
  public void testMergeAppend() throws Exception {
    testFastAppend(); // create 2 compatible manifest files that will be merged

    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    // third append
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 3 files", 3, tasks.size());

    Assert.assertEquals("Should contain 3 Avro manifest files",
        3, listManifestFiles().size());

    TableMetadata metadata = readMetadataVersion(5);
    Assert.assertEquals("Current snapshot should contain 1 merged manifest",
        1, metadata.currentSnapshot().allManifests().size());
  }

  /**
   * Test rename/close during {@link HadoopTableOperations#commit(TableMetadata, TableMetadata)} with the provided
   * {@link FileSystem} object. The provided FileSystem will be injected for commit call.
   * can optionally inject the {@link FileSystem} into the {@link FileIO} of the {@link HadoopTableOperations}
   */
  protected void testCommitWithFileSystem(FileSystem mockFs, boolean injectToOps) throws Exception {
    assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    assertFalse("Should not create v2 or newer versions",
        version(2).exists());
    assertTrue(table instanceof BaseTable);
    BaseTable baseTable = (BaseTable) table;
    // use v1 metafile as the test rename destination.
    TableMetadata meta1 = baseTable.operations().current();

    // create v2 metafile as base. This is solely for the convenience of rename testing later
    // (so that we have 2 valid and different metadata files, which will reach the rename part during commit)
    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();
    assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    // mock / spy the classes for testing
    TableOperations tops = baseTable.operations();
    TableMetadata meta2 = tops.current();
    assertTrue(tops instanceof HadoopTableOperations);
    HadoopTableOperations spyOps = Mockito.spy((HadoopTableOperations) tops);
    // inject the mockFS into the TableOperations
    doReturn(mockFs).when(spyOps).getFileSystem(any(), any());
    if (injectToOps) {
      // inject the mockFS into the FileIO of the table
      FileIO spyIO = Mockito.spy(spyOps.io());
      doAnswer((InvocationOnMock invocation) ->
              HadoopOutputFile.fromPath(new Path(invocation.getArgumentAt(0, String.class)), mockFs)
      ).when(spyIO).newOutputFile(any());
      doReturn(spyIO).when(spyOps).io();
    }

    try {
      spyOps.commit(meta2, meta1);
      fail("Commit should fail due to mock file system");
    } catch (CommitFailedException expected) {
    }

    // Verifies that there is no temporary metadata.json files left on rename failures.
    Set<String> actual = listMetadataJsonFiles().stream().map(File::getName).collect(Collectors.toSet());
    Set<String> expected = Sets.newHashSet("v1.metadata.json", "v2.metadata.json");
    assertEquals("only v1 and v2 metadata.json should exist.", expected, actual);
  }

  @Test
  public void testCanReadOldCompressedManifestFiles() throws Exception {
    assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());

    // do a file append
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // since we don't generate old file extensions anymore, let's convert existing metadata to old .metadata.json.gz
    // to test backwards compatibility
    rewriteMetadataAsGzipWithOldExtension();

    List<File> metadataFiles = listMetadataJsonFiles();

    assertEquals("Should have two versions", 2, metadataFiles.size());
    assertTrue("Metadata should be compressed with old format.",
        metadataFiles.stream().allMatch(f -> f.getName().endsWith(".metadata.json.gz")));

    Table reloaded = tables.load(tableLocation);

    List<FileScanTask> tasks = Lists.newArrayList(reloaded.newScan().planFiles());
    Assert.assertEquals("Should scan 1 files", 1, tasks.size());
  }
}
