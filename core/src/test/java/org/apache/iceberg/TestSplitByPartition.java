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
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TestHelpers.Row;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSplitByPartition {
  private static final long MB = 1024 * 1024;
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "part_col1", Types.StringType.get()),
      optional(3, "part_col2", Types.DateType.get())
  );
  private static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("part_col1", 16)
      .day("part_col2")
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Table table = null;

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    table = TABLES.create(SCHEMA, PARTITION_SPEC, tableLocation);
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(128 * MB))
        .set(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(4 * MB))
        .set(TableProperties.SPLIT_LOOKBACK, String.valueOf(Integer.MAX_VALUE))
        .commit();
  }

  @Test
  public void testBasic() {
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        List<DataFile> partitionFiles = newFiles(2, 32 * MB, Row.of(i, 50 + j));
        appendFiles(partitionFiles);
      }
    }
    TableScan scan = table.newScan().preservePartitions("part_col1", "part_col2");
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    for (CombinedScanTask t : tasks) {
      assertEquals(64 * MB, taskSize(t));
    }
    assertEquals(20, Iterables.size(tasks));

    // each partition will have 10 32mb files, and 3 files after split combining.
    scan = table.newScan().preservePartitions("part_col1");
    tasks = scan.planTasks();
    for (CombinedScanTask t : tasks) {
      assertTrue(taskSize(t) >= 64 * MB);
    }
    assertEquals(12, Iterables.size(tasks));

    // each partition will have 8 32mb files, and 2 files after split combining.
    scan = table.newScan().preservePartitions("part_col2");
    tasks = scan.planTasks();
    for (CombinedScanTask t : tasks) {
      assertEquals(128 * MB, taskSize(t));
    }
    assertEquals(10, Iterables.size(tasks));

    // calling the method with empty list means no constraint on splits combining
    scan = table.newScan().preservePartitions();
    tasks = scan.planTasks();
    for (CombinedScanTask t : tasks) {
      assertEquals(128 * MB, taskSize(t));
    }
    assertEquals(10, Iterables.size(tasks));
  }

  @Test
  public void testInvalidCases() throws IOException {
    AssertHelpers.assertThrows(
        "should throw exception when input column is not a partition column",
        IllegalArgumentException.class,
        "is not a partition column",
        () -> table.newScan().preservePartitions("id"));

    AssertHelpers.assertThrows(
        "should throw exception when not all input columns are partition columns",
        IllegalArgumentException.class,
        "is not a partition column",
        () -> table.newScan().preservePartitions("id", "part_col1"));

    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    table = TABLES.create(SCHEMA, tableLocation);
    AssertHelpers.assertThrows(
        "should throw exception when applying to un-partitioned tables",
        IllegalArgumentException.class,
        "Can't call preservePartitions on un-partitioned tables",
        () -> table.newScan().preservePartitions("part_col1"));
  }

  private long taskSize(CombinedScanTask task) {
    return task.files().stream().map(FileScanTask::length).reduce(0L, Long::sum);
  }

  private void appendFiles(Iterable<DataFile> files) {
    AppendFiles appendFiles = table.newAppend();
    files.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes, StructLike partition) {
    List<DataFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newFile(sizeInBytes, partition));
    }
    return files;
  }

  private DataFile newFile(long sizeInBytes, StructLike partition) {
    String fileName = UUID.randomUUID().toString();
    return DataFiles.builder(PARTITION_SPEC)
        .withPartition(partition)
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withFileSizeInBytes(sizeInBytes)
        .withRecordCount(2)
        .build();
  }
}
