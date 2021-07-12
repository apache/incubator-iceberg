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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGenericDeleteFilter {

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get())
  );

  public static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  public static final String TABLE_NAME = "test";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private List<Record> records;
  private DataFile dataFile;

  @Before
  public void before() throws Exception {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());
    this.table = TestTables.create(tableDir, TABLE_NAME, SCHEMA, SPEC, 2);
    this.records = Lists.newArrayList();

    GenericRecord record = GenericRecord.create(table.schema());
    records.add(record.copy("id", 29, "data", "a"));
    records.add(record.copy("id", 43, "data", "b"));
    records.add(record.copy("id", 61, "data", "c"));
    records.add(record.copy("id", 89, "data", "d"));
    records.add(record.copy("id", 100, "data", "e"));
    records.add(record.copy("id", 121, "data", "f"));
    records.add(record.copy("id", 122, "data", "g"));

    this.dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), records);

    table.newAppend()
        .appendFile(dataFile)
        .commit();
  }

  @After
  public void cleanup() throws IOException {
    TestTables.clearTables();
  }

  @Test
  public void testCheckShouldKeep() throws Exception {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), dataDeletes, dataSchema);

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(
        Pair.of(dataFile.path(), 3L), // id = 89
        Pair.of(dataFile.path(), 5L) // id = 121
    );

    Pair<DeleteFile, Set<CharSequence>> posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    Set<Integer> deletedRows = Sets.newHashSet(29, 89, 121, 122);
    // we know there is only a single data file
    FileScanTask task = table.newScan().planFiles().iterator().next();
    GenericDeleteFilter deleteFilter = new GenericDeleteFilter(table.io(), task, SCHEMA, dataSchema);

    List<Record> recordInputs = Lists.newArrayList();

    int idx = 0;
    for (Record record : records) {
      Record r2 = GenericRecord.create(deleteFilter.requiredSchema());
      r2.set(0, record.getField("data"));
      r2.set(1, (long) idx);
      idx++;
      recordInputs.add(r2);
    }

    CloseableIterator<Boolean> shouldKeep = deleteFilter.checkShouldKeep(
        CloseableIterable.withNoopClose(recordInputs)).iterator();
    for (Record record : records) {
      Assert.assertTrue(shouldKeep.hasNext());
      boolean shouldKeepRecord = shouldKeep.next();
      if (deletedRows.contains((int) record.getField("id"))) {
        Assert.assertFalse(shouldKeepRecord);
      } else {
        Assert.assertTrue(shouldKeepRecord);
      }
    }

    Assert.assertFalse(shouldKeep.hasNext());
  }
}
