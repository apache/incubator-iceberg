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
import java.util.Collection;
import java.util.List;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.Position;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class ReaderFunctionTestBase<T> {

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{FileFormat.AVRO},
        new Object[]{FileFormat.ORC},
        new Object[]{FileFormat.PARQUET}
    };
  }

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();

  @Rule
  public final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  protected abstract ReaderFunction<T> readerFunction();

  protected abstract void assertRecords(List<Record> expected, List<T> actual, Schema schema);

  private final FileFormat fileFormat;

  public ReaderFunctionTestBase(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  private List<List<Record>> recordBatchList;
  private List<DataFile> dataFileList;
  private IcebergSourceSplit icebergSplit;

  @Before
  public void before() throws IOException {
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
    icebergSplit = sortFilesAsAppendOrder(splits.get(0), dataFileList);
  }

  /**
   * Split planning doesn't guarantee the order is the same as appended.
   * So we re-arrange the list to make the assertion simpler
   */
  public static IcebergSourceSplit sortFilesAsAppendOrder(IcebergSourceSplit split, List<DataFile> dataFiles) {
    Collection<FileScanTask> files = split.task().files();
    Assert.assertEquals(files.size(), dataFiles.size());
    FileScanTask[] sortedFileArray = new FileScanTask[files.size()];
    for (FileScanTask fileScanTask : files) {
      for (int i = 0; i < dataFiles.size(); ++i) {
        if (fileScanTask.file().path().toString().equals(dataFiles.get(i).path().toString())) {
          sortedFileArray[i] = fileScanTask;
        }
      }
    }
    List<FileScanTask> sortedFileList = Lists.newArrayList(sortedFileArray);
    Assert.assertThat(sortedFileList, CoreMatchers.everyItem(CoreMatchers.notNullValue(FileScanTask.class)));
    CombinedScanTask rearrangedCombinedTask = new BaseCombinedScanTask(sortedFileList);
    return IcebergSourceSplit.fromCombinedScanTask(rearrangedCombinedTask);
  }

  /**
   * We have to combine the record extraction and position assertion in a single function,
   * because iterator is only valid for one pass.
   */
  private List<T> extractRecordsAndAssertPosition(
      RecordsWithSplitIds<RecordAndPosition<T>> batch,
      long expectedCount, long exptectedFileOffset, long startRecordOffset) {
    // need to call nextSplit first in order to read the batch
    batch.nextSplit();
    final List<T> records = new ArrayList<>();
    long recordOffset = startRecordOffset;
    RecordAndPosition<T> recordAndPosition;
    while ((recordAndPosition = batch.nextRecordFromSplit()) != null) {
      records.add(recordAndPosition.getRecord());
      Assert.assertEquals("expected file offset", exptectedFileOffset, recordAndPosition.getOffset());
      Assert.assertEquals("expected record offset", recordOffset, recordAndPosition.getRecordSkipCount() - 1);
      recordOffset++;
    }
    Assert.assertEquals("expected record count", expectedCount, records.size());
    return records;
  }

  @Test
  public void testNoCheckpointedPosition() throws IOException {
    final IcebergSourceSplit split = icebergSplit;
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    final List<T> actual0 = extractRecordsAndAssertPosition(batch0, recordBatchList.get(0).size(), 0L, 0L);
    assertRecords(recordBatchList.get(0), actual0, TestFixtures.SCHEMA);
    batch0.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch1, recordBatchList.get(1).size(), 1L, 0L);
    assertRecords(recordBatchList.get(1), actual1, TestFixtures.SCHEMA);
    batch1.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionBeforeFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new Position(0L, 0L));
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    final List<T> actual0 = extractRecordsAndAssertPosition(batch0, recordBatchList.get(0).size(), 0L, 0L);
    assertRecords(recordBatchList.get(0), actual0, TestFixtures.SCHEMA);
    batch0.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch1, recordBatchList.get(1).size(), 1L, 0L);
    assertRecords(recordBatchList.get(1), actual1, TestFixtures.SCHEMA);
    batch1.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionMiddleFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new Position(0L, 1L));
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    final List<T> actual0 = extractRecordsAndAssertPosition(batch0, 1L, 0L, 1L);
    assertRecords(recordBatchList.get(0).subList(1, 2), actual0, TestFixtures.SCHEMA);
    batch0.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch1, recordBatchList.get(1).size(), 1L, 0L);
    assertRecords(recordBatchList.get(1), actual1, TestFixtures.SCHEMA);
    batch1.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionAfterFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new Position(0L, 2L));
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch0, recordBatchList.get(1).size(), 1L, 0L);
    assertRecords(recordBatchList.get(1), actual1, TestFixtures.SCHEMA);
    batch0.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionBeforeSecondFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new Position(1L, 0L));
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch1, recordBatchList.get(1).size(), 1L, 0L);
    assertRecords(recordBatchList.get(1), actual1, TestFixtures.SCHEMA);
    batch1.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionMidSecondFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new Position(1L, 1L));
    final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader = readerFunction().apply(split);

    final RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    final List<T> actual1 = extractRecordsAndAssertPosition(batch1, 1L, 1L, 1L);
    assertRecords(recordBatchList.get(1).subList(1, 2), actual1, TestFixtures.SCHEMA);
    batch1.recycle();

    final RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    final List<T> actual2 = extractRecordsAndAssertPosition(batch2, recordBatchList.get(2).size(), 2L, 0L);
    assertRecords(recordBatchList.get(2), actual2, TestFixtures.SCHEMA);
    batch2.recycle();
  }

}
