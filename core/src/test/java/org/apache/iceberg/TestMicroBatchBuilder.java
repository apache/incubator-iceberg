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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMicroBatchBuilder extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMicroBatchBuilder(int formatVersion) {
    super(formatVersion);
  }

  @Before
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();
  }

  @Test
  public void testGenerateMicroBatch() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(0, Long.MAX_VALUE, true);
    Assert.assertEquals(batch.snapshotId(), 1L);
    Assert.assertEquals(batch.startFileIndex(), 0);
    Assert.assertEquals(batch.endFileIndex(), 5);
    Assert.assertEquals(batch.sizeInBytes(), 50);
    Assert.assertTrue(batch.lastIndexOfSnapshot());
    filesMatch(Lists.newArrayList("A", "B", "C", "D", "E"), filesToScan(batch.tasks()));

    MicroBatch batch1 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(0, 15L, true);
    Assert.assertEquals(batch1.endFileIndex(), 1);
    Assert.assertEquals(batch1.sizeInBytes(), 10);
    Assert.assertFalse(batch1.lastIndexOfSnapshot());
    filesMatch(Lists.newArrayList("A"), filesToScan(batch1.tasks()));

    MicroBatch batch2 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch1.endFileIndex(), 30L, true);
    Assert.assertEquals(batch2.endFileIndex(), 4);
    Assert.assertEquals(batch2.sizeInBytes(), 30);
    Assert.assertFalse(batch2.lastIndexOfSnapshot());
    filesMatch(Lists.newArrayList("B", "C", "D"), filesToScan(batch2.tasks()));

    MicroBatch batch3 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch2.endFileIndex(), 50L, true);
    Assert.assertEquals(batch3.endFileIndex(), 5);
    Assert.assertEquals(batch3.sizeInBytes(), 10);
    Assert.assertTrue(batch3.lastIndexOfSnapshot());
    filesMatch(Lists.newArrayList("E"), filesToScan(batch3.tasks()));
  }

  @Test
  public void testGenerateMicroBatchWithSmallTargetSize() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(0, 10L, true);
    Assert.assertEquals(batch.snapshotId(), 1L);
    Assert.assertEquals(batch.startFileIndex(), 0);
    Assert.assertEquals(batch.endFileIndex(), 1);
    Assert.assertEquals(batch.sizeInBytes(), 10);
    Assert.assertFalse(batch.lastIndexOfSnapshot());
    filesMatch(Lists.newArrayList("A"), filesToScan(batch.tasks()));

    MicroBatch batch1 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch.endFileIndex(), 5L, true);
    Assert.assertEquals(batch1.endFileIndex(), 2);
    Assert.assertEquals(batch1.sizeInBytes(), 10);
    filesMatch(Lists.newArrayList("B"), filesToScan(batch1.tasks()));
    Assert.assertFalse(batch1.lastIndexOfSnapshot());

    MicroBatch batch2 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch1.endFileIndex(), 10L, true);
    Assert.assertEquals(batch2.endFileIndex(), 3);
    Assert.assertEquals(batch2.sizeInBytes(), 10);
    filesMatch(Lists.newArrayList("C"), filesToScan(batch2.tasks()));
    Assert.assertFalse(batch2.lastIndexOfSnapshot());

    MicroBatch batch3 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch2.endFileIndex(), 10L, true);
    Assert.assertEquals(batch3.endFileIndex(), 4);
    Assert.assertEquals(batch3.sizeInBytes(), 10);
    filesMatch(Lists.newArrayList("D"), filesToScan(batch3.tasks()));
    Assert.assertFalse(batch3.lastIndexOfSnapshot());

    MicroBatch batch4 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch3.endFileIndex(), 5L, true);
    Assert.assertEquals(batch4.endFileIndex(), 5);
    Assert.assertEquals(batch4.sizeInBytes(), 10);
    filesMatch(Lists.newArrayList("E"), filesToScan(batch4.tasks()));
    Assert.assertTrue(batch4.lastIndexOfSnapshot());

    MicroBatch batch5 = MicroBatches.from(table.snapshot(1L), table.io())
        .specsById(table.specs())
        .generate(batch4.endFileIndex(), 5L, true);
    Assert.assertEquals(batch5.endFileIndex(), 5);
    Assert.assertEquals(batch5.sizeInBytes(), 0);
    Assert.assertTrue(Iterables.isEmpty(batch5.tasks()));
    Assert.assertTrue(batch5.lastIndexOfSnapshot());
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
        .withPath(name + ".parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
  }

  private static void add(AppendFiles appendFiles, List<DataFile> adds) {
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private static void delete(DeleteFiles deleteFiles, List<DataFile> deletes) {
    for (DataFile f : deletes) {
      deleteFiles.deleteFile(f);
    }
    deleteFiles.commit();
  }

  private static List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestMicroBatchBuilder::file);
  }

  private static List<String> filesToScan(Iterable<FileScanTask> tasks) {
    Iterable<String> filesToRead = Iterables.transform(tasks, t -> {
      String path = t.file().path().toString();
      return path.split("\\.")[0];
    });
    return Lists.newArrayList(filesToRead);
  }

  private static void filesMatch(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
}
