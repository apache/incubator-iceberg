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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestRewriteDeletesAction extends SparkTestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testRewriteDeletesWithNoEqDeletes() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .build();
    Map<String, String> options = Maps.newHashMap();
    BaseTable table = (BaseTable) TABLES.create(SCHEMA, spec, options, tableLocation);
    TableOperations ops = table.operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(2));

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);
    table.refresh();

    DeleteRewriteActionResult result = Actions.forTable(table).replaceEqDeleteToPosDelete().execute();
    Assert.assertTrue("Shouldn't contain equality deletes", result.deletedFiles().isEmpty());
    Assert.assertTrue("Shouldn't generate position deletes", result.addedFiles().isEmpty());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDeletesUnpartitionedTable() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    BaseTable table = (BaseTable) TABLES.create(SCHEMA, spec, options, tableLocation);
    TableOperations ops = table.operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(2));

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    OutputFileFactory fileFactory = new OutputFileFactory(table.spec(), FileFormat.PARQUET, table.locationProvider(),
        table.io(), table.encryption(), 1, 1);

    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("c3").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("c3");
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        ArrayUtil.toIntArray(equalityFieldIds),
        eqDeleteRowSchema, null);

    Record record = GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of("c3", "AAAA"));

    EqualityDeleteWriter<Record> eqDeleteWriter =  appenderFactory.newEqDeleteWriter(
        createEncryptedOutputFile(createPartitionKey(table, record),  fileFactory),
        FileFormat.PARQUET,
        createPartitionKey(table, record));

    try (EqualityDeleteWriter<Record> closeableWriter = eqDeleteWriter) {
      closeableWriter.delete(record);
    }

    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();

    Actions.forTable(table).replaceEqDeleteToPosDelete().execute();

    CloseableIterable<FileScanTask> tasks = CloseableIterable.filter(
        table.newScan().planFiles(), task -> task.deletes().stream()
            .anyMatch(delete -> delete.content().equals(FileContent.EQUALITY_DELETES))
    );
    Assert.assertFalse("Should not contain any equality deletes", tasks.iterator().hasNext());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(records1.get(1));
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDeletesInPartitionedTable() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c3", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    BaseTable table = (BaseTable) TABLES.create(SCHEMA, spec, options, tableLocation);
    TableOperations ops = table.operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(2));

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);
    table.refresh();

    OutputFileFactory fileFactory = new OutputFileFactory(table.spec(), FileFormat.PARQUET, table.locationProvider(),
        table.io(), table.encryption(), 1, 1);

    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("c2").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("c2");
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        ArrayUtil.toIntArray(equalityFieldIds),
        eqDeleteRowSchema, null);

    PartitionKey key = createPartitionKey(table,
        GenericRecord.create(table.schema()).copy(ImmutableMap.of("c1", 1, "c2", "ignore", "c3", "AAAA")));

    EqualityDeleteWriter<Record> eqDeleteWriter = appenderFactory.newEqDeleteWriter(
        createEncryptedOutputFile(key, fileFactory), FileFormat.PARQUET, key);

    try (EqualityDeleteWriter<Record> closeableWriter = eqDeleteWriter) {
      closeableWriter.delete(GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of("c2", "AAAAAAAA")));
    }

    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();

    Actions.forTable(table).replaceEqDeleteToPosDelete().execute();

    CloseableIterable<FileScanTask> tasks = CloseableIterable.filter(
        table.newScan().planFiles(), task -> task.deletes().stream()
            .anyMatch(delete -> delete.content().equals(FileContent.EQUALITY_DELETES))
    );
    Assert.assertFalse("Should not contain any equality deletes", tasks.iterator().hasNext());

    table.refresh();
    CloseableIterable<FileScanTask> newTasks = table.newScan().ignoreResiduals().planFiles();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    newTasks.forEach(task -> {
      deleteFiles.addAll(task.deletes());
    });

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(records1.get(1));
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDeletesWithDeletes() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .build();
    Map<String, String> options = Maps.newHashMap();
    BaseTable table = (BaseTable) TABLES.create(SCHEMA, spec, options, tableLocation);
    TableOperations ops = table.operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(2));

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE")
    );
    writeRecords(records3);
    table.refresh();

    DataFile fileForPosDelete = table.currentSnapshot().addedFiles().iterator().next();

    OutputFileFactory fileFactory = new OutputFileFactory(table.spec(), FileFormat.PARQUET, table.locationProvider(),
        table.io(), table.encryption(), 1, 1);

    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("c3").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("c3");
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        ArrayUtil.toIntArray(equalityFieldIds),
        eqDeleteRowSchema, null);

    // write equality delete
    Record partitionRecord = GenericRecord.create(table.schema().select("c1")).copy(ImmutableMap.of("c1", 1));
    EncryptedOutputFile file = createEncryptedOutputFile(createPartitionKey(table, partitionRecord), fileFactory);
    EqualityDeleteWriter<Record> eqDeleteWriter = appenderFactory.newEqDeleteWriter(
        file, FileFormat.PARQUET, createPartitionKey(table, partitionRecord));
    Record record = GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of("c3", "AAAA"));
    try (EqualityDeleteWriter<Record> closeableWriter = eqDeleteWriter) {
      closeableWriter.delete(record);
    }
    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();

    // write positional delete
    partitionRecord = partitionRecord.copy(ImmutableMap.of("c1", 3));
    file = createEncryptedOutputFile(createPartitionKey(table, partitionRecord), fileFactory);
    PositionDeleteWriter<Record> posDeleteWriter = appenderFactory.newPosDeleteWriter(
        file, FileFormat.PARQUET, createPartitionKey(table, partitionRecord));
    posDeleteWriter.delete(fileForPosDelete.path(), 0);
    posDeleteWriter.close();
    table.newRowDelta().addDeletes(posDeleteWriter.toDeleteFile()).commit();

    DeleteRewriteActionResult result = Actions.forTable(table).replaceEqDeleteToPosDelete().execute();
    Assert.assertEquals("Should contain one equality delete to delete", 1, result.deletedFiles().size());
    Assert.assertEquals("Should contain one position delete to add", 1, result.addedFiles().size());

    CloseableIterable<FileScanTask> tasks = CloseableIterable.filter(
        table.newScan().planFiles(), task -> task.deletes().stream()
            .anyMatch(delete -> delete.content().equals(FileContent.EQUALITY_DELETES))
    );
    Assert.assertFalse("Should not contain any equality deletes", tasks.iterator().hasNext());

    table.refresh();
    CloseableIterable<FileScanTask> newTasks = table.newScan().ignoreResiduals().planFiles();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    newTasks.forEach(task -> {
      deleteFiles.addAll(task.deletes());
    });

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(records1.get(1));
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  private PartitionKey createPartitionKey(Table table, Record record) {
    if (table.spec().isUnpartitioned()) {
      return null;
    }

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  private EncryptedOutputFile createEncryptedOutputFile(PartitionKey partition, OutputFileFactory fileFactory) {
    if (partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(partition);
    }
  }

}
