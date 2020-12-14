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

package org.apache.iceberg.flink.source.split;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

public class SplitHelpers {

  private static final AtomicLong splitLengthIncrement = new AtomicLong();

  private SplitHelpers() {

  }

  public static List<IcebergSourceSplit> createMockedSplits(int splitCount) {
    final List<IcebergSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < splitCount; ++i) {
      // make sure each task has a different length,
      // as it is part of the splitId calculation.
      // This way, we can make sure all generated splits have different splitIds
      FileScanTask fileScanTask = new MockFileScanTask(1024 + splitLengthIncrement.incrementAndGet());
      CombinedScanTask combinedScanTask = new BaseCombinedScanTask(fileScanTask);
      splits.add(IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
    }
    return splits;
  }

  public static List<IcebergSourceSplit> createFileSplits(
      TemporaryFolder temporaryFolder, int fileCount, int filesPerSplit) throws Exception {
    final File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    final String warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
    try {
      final Table table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
      final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
          table, FileFormat.PARQUET, temporaryFolder);
      for (int i = 0; i < fileCount; ++i) {
        List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
        dataAppender.appendToTable(records);
      }

      final ScanContext scanContext = ScanContext.builder().build();
      final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
      return splits.stream()
          .flatMap(split -> {
            List<List<FileScanTask>> filesList = Lists.partition(new ArrayList<>(split.task().files()), filesPerSplit);
            return filesList.stream()
                .map(files ->  new BaseCombinedScanTask(files))
                .map(combinedScanTask -> IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
          })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }
}
