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

package org.apache.iceberg.actions;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.collections.Sets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class TestBaseRewriteDataFilesAction extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestBaseRewriteDataFilesAction(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testReplaceCommitSuccess() throws IOException {
    TestRewriteDataFileAction testRewriteDataFileAction = new TestRewriteDataFileAction(table);
    DataFile dataFile = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withRecordCount(10)
        .withFileSizeInBytes(10)
        .build();

    DataFile rewrite = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withRecordCount(10)
        .withFileSizeInBytes(10)
        .build();

    table.newFastAppend()
        .appendFile(dataFile)
        .commit();

    table.newFastAppend()
        .appendFile(rewrite)
        .commit();

    testRewriteDataFileAction.replaceDataFiles(Sets.newSet(dataFile), Sets.newSet(rewrite));

    Assert.assertTrue(new File(dataFile.path().toString()).exists());
    Assert.assertTrue(new File(rewrite.path().toString()).exists());
  }

  @Test
  public void testReplaceCommitStateUnknown() throws IOException {
    TestRewriteDataFileAction testRewriteDataFileAction = new TestRewriteDataFileAction(table);
    DataFile dataFile = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withFileSizeInBytes(10)
        .withRecordCount(10)
        .build();

    DataFile rewrite = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withFileSizeInBytes(10)
        .withRecordCount(10)
        .build();

    table.newFastAppend()
        .appendFile(dataFile)
        .commit();
    table.newFastAppend()
        .appendFile(rewrite)
        .commit();

    BaseRewriteDataFilesAction spyRewriteDataFileAction = spy(testRewriteDataFileAction);
    doThrow(new CommitStateUnknownException(new Exception("Commit-state is unknown.")))
        .when(spyRewriteDataFileAction)
        .commit(any());

    AssertHelpers.assertThrows("We should rethrown commit state unknown exception", CommitStateUnknownException.class,
        () -> spyRewriteDataFileAction.replaceDataFiles(Sets.newSet(dataFile), Sets.newSet(rewrite)));
    Assert.assertTrue(new File(dataFile.path().toString()).exists());
    Assert.assertTrue(new File(rewrite.path().toString()).exists());
  }

  @Test
  public void testReplaceCommitStateFailed() throws IOException {
    TestRewriteDataFileAction testRewriteDataFileAction = new TestRewriteDataFileAction(table);
    DataFile dataFile = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withRecordCount(10)
        .withFileSizeInBytes(10)
        .build();

    DataFile rewrite = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(Files.localOutput(temp.newFile()).location())
        .withRecordCount(10)
        .withFileSizeInBytes(10)
        .build();

    table.newFastAppend()
        .appendFile(dataFile)
        .commit();
    table.newFastAppend()
        .appendFile(rewrite)
        .commit();

    BaseRewriteDataFilesAction spyRewriteDataFileAction = spy(testRewriteDataFileAction);
    doThrow(new RuntimeException("Commit-state is failed."))
        .when(spyRewriteDataFileAction)
        .commit(any());

    AssertHelpers.assertThrows("We should rethrown commit state failed exception.", RuntimeException.class,
        () -> spyRewriteDataFileAction.replaceDataFiles(Sets.newSet(dataFile), Sets.newSet(rewrite)));
    Assert.assertTrue(new File(dataFile.path().toString()).exists());
    Assert.assertFalse(new File(rewrite.path().toString()).exists());
  }

  class TestRewriteDataFileAction extends BaseRewriteDataFilesAction<TestRewriteDataFileAction> {
    protected TestRewriteDataFileAction(Table table) {
      super(table);
    }

    @Override
    protected FileIO fileIO() {
      return table.io();
    }

    @Override
    protected List<DataFile> rewriteDataForTasks(List<CombinedScanTask> combinedScanTask) {
      List<FileScanTask> tasks = combinedScanTask.stream()
          .flatMap(e -> e.files().stream())
          .collect(Collectors.toList());
      return tasks.stream()
          .map(FileScanTask::file)
          .collect(Collectors.toList());
    }

    @Override
    protected TestRewriteDataFileAction self() {
      return this;
    }
  }

}
