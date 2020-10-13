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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.Files.localOutput;

public abstract class TestSparkBaseDataReader {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Configuration CONFD = new Configuration();

  // Simulates the closeable iterator of data to be read
  private static class CloseableIntegerRange implements CloseableIterator<Integer> {
    boolean closed;
    Iterator<Integer> iter;

    CloseableIntegerRange(long range) {
      this.closed = false;
      this.iter = IntStream.range(0, (int) range).iterator();
    }

    @Override
    public void close() {
      this.closed = true;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Integer next() {
      return iter.next();
    }
  }

  // Main reader class to test base class iteration logic.
  // Keeps track of iterator closure.
  private static class ClosureTrackingReader extends BaseDataReader<Integer> {
    private Map<String, CloseableIntegerRange> tracker = new HashMap<>();

    ClosureTrackingReader(List<FileScanTask> tasks) {
      super(new BaseCombinedScanTask(tasks),
          new HadoopFileIO(CONFD),
          new PlaintextEncryptionManager());
    }

    @Override
    CloseableIterator<Integer> open(FileScanTask task) {
      CloseableIntegerRange intRange = new CloseableIntegerRange(task.file().recordCount());
      tracker.put(getKey(task), intRange);
      return intRange;
    }

    public Boolean isIteratorClosed(FileScanTask task) {
      return tracker.get(getKey(task)).closed;
    }

    public Boolean hasIterator(FileScanTask task) {
      return tracker.containsKey(getKey(task));
    }

    private String getKey(FileScanTask task) {
      return task.file().path().toString();
    }
  }

  @Test
  public void testClosureOnDataExhaustion() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    int countRecords = 0;
    while (reader.next()) {
      countRecords += 1;
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    Assert.assertEquals("Reader returned incorrect number of records",
        totalTasks * recordPerTask,
        countRecords
    );
    tasks.forEach(t ->
        Assert.assertTrue("All iterators should be closed after read exhausion",
            reader.isIteratorClosed(t))
    );
  }

  @Test
  public void testClosureDuringIteration() throws IOException {
    Integer totalTasks = 2;
    Integer recordPerTask = 1;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);
    Assert.assertEquals(2, tasks.size());
    FileScanTask firstTask = tasks.get(0);
    FileScanTask secondTask = tasks.get(1);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    // Total of 2 elements
    Assert.assertTrue(reader.next());
    Assert.assertFalse("First iter should not be closed on its last element",
        reader.isIteratorClosed(firstTask));

    Assert.assertTrue(reader.next());
    Assert.assertTrue("First iter should be closed after moving to second iter",
        reader.isIteratorClosed(firstTask));
    Assert.assertFalse("Second iter should not be closed on its last element",
        reader.isIteratorClosed(secondTask));

    Assert.assertFalse(reader.next());
    Assert.assertTrue(reader.isIteratorClosed(firstTask));
    Assert.assertTrue(reader.isIteratorClosed(secondTask));
  }

  @Test
  public void testClosureWithoutAnyRead() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    reader.close();

    tasks.forEach(t ->
        Assert.assertFalse("Iterator should not be created eagerly for tasks",
            reader.hasIterator(t))
    );
  }

  @Test
  public void testExplicitClosure() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    Integer halfDataSize = (totalTasks * recordPerTask) / 2;
    for (int i = 0; i < halfDataSize; i++) {
      Assert.assertTrue("Reader should have some element", reader.next());
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    reader.close();

    // Some tasks might have not been opened yet, so we don't have corresponding tracker for it.
    // But all that have been created must be closed.
    tasks.forEach(t -> {
      if (reader.hasIterator(t)) {
        Assert.assertTrue("Iterator should be closed after read exhausion",
            reader.isIteratorClosed(t));
      }
    });
  }

  @Test
  public void testIdempotentExplicitClosure() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    // Total 100 elements, only 5 iterators have been created
    for (int i = 0; i < 45; i++) {
      Assert.assertTrue("eader should have some element", reader.next());
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    for (int closeAttempt = 0; closeAttempt < 5; closeAttempt++) {
      reader.close();
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue("Iterator should be closed after read exhausion",
            reader.isIteratorClosed(tasks.get(i)));
      }
      for (int i = 5; i < 10; i++) {
        Assert.assertFalse("Iterator should not be created eagerly for tasks",
            reader.hasIterator(tasks.get(i)));
      }
    }
  }

  private List<FileScanTask> createFileScanTasks(Integer totalTasks, Integer recordPerTask) throws IOException {
    String desc = "make_scan_tasks";
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    try {
      Table table = TestTables.create(location, desc, schema, PartitionSpec.unpartitioned());
      // Important: use the table's schema for the rest of the test
      // When tables are created, the column ids are reassigned.
      Schema tableSchema = table.schema();
      List<GenericData.Record> expected = RandomData.generateList(tableSchema, recordPerTask, 1L);

      AppendFiles appendFiles = table.newAppend();
      for (int i = 0; i < totalTasks; i++) {
        File parquetFile = new File(dataFolder, PARQUET.addExtension(UUID.randomUUID().toString()));
        try (FileAppender<GenericData.Record> writer = Parquet.write(localOutput(parquetFile))
            .schema(tableSchema)
            .build()) {
          writer.addAll(expected);
        }
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
            .withFileSizeInBytes(parquetFile.length())
            .withPath(parquetFile.toString())
            .withRecordCount(recordPerTask)
            .build();
        appendFiles.appendFile(file);
      }
      appendFiles.commit();

      return StreamSupport
          .stream(table.newScan().planFiles().spliterator(), false)
          .collect(Collectors.toList());
    } finally {
      TestTables.clearTables();
    }
  }
}
