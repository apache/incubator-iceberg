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

package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

public class TestIcebergSourceSplitReader {

  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  public static final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  @ClassRule
  public static final TestRule chain = RuleChain
      .outerRule(TEMPORARY_FOLDER)
      .around(tableResource);

  private static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();
  private static final FileFormat fileFormat = FileFormat.PARQUET;

  private static List<List<Record>> recordBatchList;
  private static List<DataFile> dataFileList;
  private static IcebergSourceSplit icebergSplit;

  @BeforeClass
  public static void beforeClass() throws IOException {
    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        tableResource.table(), fileFormat, TEMPORARY_FOLDER);
    recordBatchList = new ArrayList<>(3);
    dataFileList = new ArrayList<>(2);
    for (int i = 0; i < 3; ++i) {
      List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
      recordBatchList.add(records);
      DataFile dataFile = dataAppender.writeFile(null, records);
      dataFileList.add(dataFile);
      dataAppender.appendToTable(dataFile);
    }

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator
        .planIcebergSourceSplits(tableResource.table(), scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());
    icebergSplit = ReaderFactoryTestBase.sortFilesAsAppendOrder(splits.get(0), dataFileList);
  }

  @Test
  public void testFullScan() throws Exception {
    final IcebergSourceSplit split = icebergSplit;
    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(tableResource.table().schema());
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(
        new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType));
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch0 = reader.fetch();
    final List<Row> rowBatch0 = readRows(readBatch0, split.splitId(), 0L, 0L);
    TestHelpers.assertRecords(rowBatch0, recordBatchList.get(0), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch1
        = reader.fetch();
    final List<Row> rowBatch1 = readRows(readBatch1, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2 = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatchList.get(2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
    Assert.assertEquals(null, finishedBatch.nextSplit());
  }

  @Test
  public void testResumeFromEndOfFirstBatch() throws Exception {
    final IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(icebergSplit.task(), 0L, 2L);
    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(tableResource.table().schema());
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(
        new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType));
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch1 = reader.fetch();
    final List<Row> rowBatch1 = readRows(readBatch1, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2 = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatchList.get(2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
    Assert.assertEquals(null, finishedBatch.nextSplit());
  }

  @Test
  public void testResumeFromStartOfSecondBatch() throws Exception {
    final IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(icebergSplit.task(), 1L, 0L);
    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(tableResource.table().schema());
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(
        new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType));
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch1 = reader.fetch();
    final List<Row> rowBatch1 = readRows(readBatch1, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2 = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatchList.get(2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
    Assert.assertEquals(null, finishedBatch.nextSplit());
  }

  @Test
  public void testResumeFromMiddleOfSecondBatch() throws Exception {
    final IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(icebergSplit.task(), 1L, 1L);

    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(tableResource.table().schema());
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(
        new RowDataIteratorReaderFactory(config, tableResource.table(), scanContext, rowType));
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch1 = reader.fetch();
    final List<Row> rowBatch1 = readRows(readBatch1, split.splitId(), 1L, 1L);
    TestHelpers.assertRecords(rowBatch1, recordBatchList.get(1).subList(1, 2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2 = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatchList.get(2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
    Assert.assertEquals(null, finishedBatch.nextSplit());
  }

  private List<Row> readRows(
      RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch,
      String expectedSplitId, long expectedOffset, long expectedStartingRecordOffset) {
    Assert.assertEquals(expectedSplitId, readBatch.nextSplit());
    final List<RowData> rowDataList = new ArrayList<>();
    RecordAndPosition<RowData> row;
    int num = 0;
    while ((row = readBatch.nextRecordFromSplit()) != null) {
      Assert.assertEquals(expectedOffset, row.getOffset());
      num++;
      Assert.assertEquals(expectedStartingRecordOffset + num, row.getRecordSkipCount());
      rowDataList.add(row.getRecord());
    }
    readBatch.recycle();
    return TestHelpers.convertRowDataToRow(rowDataList, TestFixtures.ROW_TYPE);
  }

}
