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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.InputFile;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestPartitionStatsMap extends TableTestBase {
  public TestPartitionStatsMap(int formatVersion) {
    super(formatVersion);
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[]{1, 2};
  }

  @Test
  public void testAppendFilesInSingleTransaction() {
    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(1, statsFileMap.size());
    String loc = (String) statsFileMap.get(0);
    List<PartitionStatsEntry> ls = getOldPartitionStatsBySpec(loc, 0);
    Assert.assertEquals(33, ls.get(0).getRowCount());
    Assert.assertEquals(3, ls.get(0).getFileCount());
  }

  @Test
  public void testAppendFilesDifferentPartitions() {

    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-3.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(1, statsFileMap.size());
    String loc = (String) statsFileMap.get(0);
    List<PartitionStatsEntry> statsEntries = getOldPartitionStatsBySpec(loc, 0);
    Assert.assertEquals(2, statsEntries.size());

    for (PartitionStatsEntry partitionStatsEntry : statsEntries) {
      PartitionData pd = (PartitionData) partitionStatsEntry.getPartition();
      if (pd.get(0).equals(0)) {
        Assert.assertEquals(2, partitionStatsEntry.getFileCount());
        Assert.assertEquals(16, partitionStatsEntry.getRowCount());
      } else if (pd.get(0).equals(3)) {
        Assert.assertEquals(1, partitionStatsEntry.getFileCount());
        Assert.assertEquals(17, partitionStatsEntry.getRowCount());
      }
    }
  }

  @Test
  public void testAppendFilesAcrossTransaction() {
    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-3.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();

    DataFile file4 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(100)
        .build();

    DataFile file5 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(3)
        .build();

    DataFile file6 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-3.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3") // easy way to set partition data for now
        .withRecordCount(500)
        .build();

    table.newAppend()
        .appendFile(file4)
        .appendFile(file5)
        .appendFile(file6)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(1, statsFileMap.size());
    String loc = (String) statsFileMap.get(0);
    List<PartitionStatsEntry> statsEntries = getOldPartitionStatsBySpec(loc, 0);
    Assert.assertEquals(2, statsEntries.size());

    for (PartitionStatsEntry partitionStatsEntry : statsEntries) {
      PartitionData pd = (PartitionData) partitionStatsEntry.getPartition();
      if (pd.get(0).equals(0)) {
        Assert.assertEquals(4, partitionStatsEntry.getFileCount());
        Assert.assertEquals(119, partitionStatsEntry.getRowCount());
      } else if (pd.get(0).equals(3)) {
        Assert.assertEquals(2, partitionStatsEntry.getFileCount());
        Assert.assertEquals(517, partitionStatsEntry.getRowCount());
      }
    }
  }

  @Test
  public void testAppendFilesUpdatedPartitionSpec() {
    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();

    table.updateSpec().addField("id").commit();

    DataFile file4 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-31.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=1")
        .withRecordCount(21)
        .build();

    DataFile file5 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-0-31.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=1")
        .withRecordCount(12)
        .build();

    DataFile file6 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-0-31.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=2")
        .withRecordCount(80)
        .build();

    DataFile file7 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-0-31.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=2")
        .withRecordCount(5)
        .build();


    table.newAppend()
        .appendFile(file4)
        .appendFile(file5)
        .commit();

    table.newAppend()
        .appendFile(file6)
        .commit();

    table.newAppend()
        .appendFile(file7)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(2, statsFileMap.size());

    String loc = (String) statsFileMap.get(table.spec().specId());
    List<PartitionStatsEntry> statsEntries = getOldPartitionStatsBySpec(loc, table.spec().specId());
    Assert.assertEquals(2, statsEntries.size());

    for (PartitionStatsEntry partitionStatsEntry : statsEntries) {
      PartitionData pd = (PartitionData) partitionStatsEntry.getPartition();
      if (pd.get(1).equals(1)) {
        Assert.assertEquals(2, partitionStatsEntry.getFileCount());
        Assert.assertEquals(33, partitionStatsEntry.getRowCount());
      } else if (pd.get(1).equals(2)) {
        Assert.assertEquals(2, partitionStatsEntry.getFileCount());
        Assert.assertEquals(85, partitionStatsEntry.getRowCount());
      }
    }
  }

  @Test
  public void testDeleteFilesPartitionStats() {
    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-1.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-2.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-3.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    DataFile file4 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-4.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3") // easy way to set partition data for now
        .withRecordCount(5)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .appendFile(file4)
        .commit();

    table.newDelete()
        .deleteFile(file2)
        .deleteFile(file3)
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(1, statsFileMap.size());
    String loc = (String) statsFileMap.get(0);
    List<PartitionStatsEntry> statsEntries = getOldPartitionStatsBySpec(loc, 0);
    Assert.assertEquals(2, statsEntries.size());

    for (PartitionStatsEntry partitionStatsEntry : statsEntries) {
      PartitionData pd = (PartitionData) partitionStatsEntry.getPartition();
      if (pd.get(0).equals(0)) {
        Assert.assertEquals(1, partitionStatsEntry.getFileCount());
        Assert.assertEquals(7, partitionStatsEntry.getRowCount());
      } else if (pd.get(0).equals(3)) {
        Assert.assertEquals(1, partitionStatsEntry.getFileCount());
        Assert.assertEquals(5, partitionStatsEntry.getRowCount());
      }
    }
  }

  @Test
  public void testDeleteFilesAll() {
    DataFile file1 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-1.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(7)
        .build();

    DataFile file2 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-2.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(9)
        .build();

    DataFile file3 = DataFiles.builder(SPEC)
        .withPath("/path/to/data-3.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(17)
        .build();

    table.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();

    table.updateSpec().addField("id").commit();

    DataFile file4 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-4.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=1")
        .withRecordCount(21)
        .build();

    DataFile file5 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-5.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=1")
        .withRecordCount(12)
        .build();

    DataFile file6 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-6.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=2")
        .withRecordCount(80)
        .build();

    DataFile file7 = DataFiles.builder(table.spec())
        .withPath("/path/to/data-7.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=3/id=2")
        .withRecordCount(5)
        .build();

    table.newAppend()
        .appendFile(file4)
        .appendFile(file5)
        .commit();

    table.newAppend()
        .appendFile(file6)
        .commit();

    table.newAppend()
        .appendFile(file7)
        .commit();

    table.newDelete()
        .deleteFromRowFilter(Expressions.alwaysTrue())
        .commit();

    Snapshot committedSnapshot = table.currentSnapshot();
    Map statsFileMap = committedSnapshot.partitionStatsFiles();
    Assert.assertEquals(2, statsFileMap.size());

    String loc = (String) statsFileMap.get(table.spec().specId());
    List<PartitionStatsEntry> statsEntries = getOldPartitionStatsBySpec(loc, table.spec().specId());
    Assert.assertEquals(2, statsEntries.size());

    for (PartitionStatsEntry partitionStatsEntry : statsEntries) {
      PartitionData pd = (PartitionData) partitionStatsEntry.getPartition();
      if (pd.get(1).equals(1)) {
        Assert.assertEquals(0, partitionStatsEntry.getFileCount());
        Assert.assertEquals(0, partitionStatsEntry.getRowCount());
      } else if (pd.get(1).equals(2)) {
        Assert.assertEquals(0, partitionStatsEntry.getFileCount());
        Assert.assertEquals(0, partitionStatsEntry.getRowCount());
      }
    }
  }

  protected List<PartitionStatsEntry> getOldPartitionStatsBySpec(String location, int specID) {
    InputFile inpf = table.ops().io().newInputFile(location);
    return PartitionStats.read(inpf, specID, table.specs());
  }

}
