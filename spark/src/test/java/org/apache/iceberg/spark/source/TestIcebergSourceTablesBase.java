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

import java.util.Comparator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class TestIcebergSourceTablesBase extends SparkTestBase {

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public abstract Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec);

  public abstract Table loadTable(TableIdentifier ident, String entriesSuffix);

  public abstract String loadLocation(TableIdentifier ident, String entriesSuffix);

  public abstract String loadLocation(TableIdentifier ident);

  @Test
  public synchronized void testTablesSupport() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "1"),
        new SimpleRecord(2, "2"),
        new SimpleRecord(3, "3"));

    Dataset<Row> inputDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode(SaveMode.Append)
        .save(loadLocation(tableIdentifier));

    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier));
    List<SimpleRecord> actualRecords = resultDf.orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Records should match", expectedRecords, actualRecords);
  }

  @Test
  public void testEntriesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "entries_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Table entriesTable = loadTable(tableIdentifier, "entries");

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));

    Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);
    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "entries"))
        .collectAsList();

    Snapshot snapshot = table.currentSnapshot();

    Assert.assertEquals("Should only contain one manifest", 1, snapshot.allManifests().size());

    InputFile manifest = table.io().newInputFile(snapshot.allManifests().get(0).path());
    List<GenericData.Record> expected = Lists.newArrayList();
    try (CloseableIterable<GenericData.Record> rows = Avro.read(manifest).project(entriesTable.schema()).build()) {
      // each row must inherit snapshot_id and sequence_number
      rows.forEach(row -> {
        row.put(2, 0L);
        GenericData.Record file = (GenericData.Record) row.get("data_file");
        file.put(0, FileContent.DATA.id());
        expected.add(row);
      });
    }

    Assert.assertEquals("Entries table should have one row", 1, expected.size());
    Assert.assertEquals("Actual results should have one row", 1, actual.size());
    TestHelpers.assertEqualsSafe(entriesTable.schema().asStruct(), expected.get(0), actual.get(0));
  }

  @Test
  public void testAllEntriesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "entries_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Table entriesTable = loadTable(tableIdentifier, "all_entries");

    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // delete the first file to test that not only live files are listed
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 1)).commit();

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // ensure table data isn't stale
    table.refresh();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_entries"))
        .orderBy("snapshot_id")
        .collectAsList();

    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : Iterables.concat(Iterables.transform(table.snapshots(), Snapshot::allManifests))) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
        // each row must inherit snapshot_id and sequence_number
        rows.forEach(row -> {
          row.put(2, 0L);
          GenericData.Record file = (GenericData.Record) row.get("data_file");
          file.put(0, FileContent.DATA.id());
          expected.add(row);
        });
      }
    }

    expected.sort(Comparator.comparing(o -> (Long) o.get("snapshot_id")));

    Assert.assertEquals("Entries table should have 3 rows", 3, expected.size());
    Assert.assertEquals("Actual results should have 3 rows", 3, actual.size());
    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(entriesTable.schema().asStruct(), expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testCountEntriesTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "count_entries_test");
    createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

    // init load
    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
    Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);
    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    final int expectedEntryCount = 1;

    // count entries
    Assert.assertEquals("Count should return " + expectedEntryCount,
        expectedEntryCount, spark.read().format("iceberg").load(loadLocation(tableIdentifier, "entries")).count());

    // count all_entries
    Assert.assertEquals("Count should return " + expectedEntryCount,
        expectedEntryCount, spark.read().format("iceberg").load(loadLocation(tableIdentifier, "all_entries")).count());
  }

  @Test
  public void testFilesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "files_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Table entriesTable = loadTable(tableIdentifier, "entries");
    Table filesTable = loadTable(tableIdentifier, "files");

    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // delete the first file to test that only live files are listed
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 1)).commit();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "files"))
        .collectAsList();

    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests()) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
        for (GenericData.Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            file.put(0, FileContent.DATA.id());
            expected.add(file);
          }
        }
      }
    }

    Assert.assertEquals("Files table should have one row", 1, expected.size());
    Assert.assertEquals("Actual results should have one row", 1, actual.size());
    TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(0), actual.get(0));
  }

  @Test
  public void testFilesTableWithSnapshotIdInheritance() throws Exception {
    spark.sql("DROP TABLE IF EXISTS parquet_table");

    TableIdentifier tableIdentifier = TableIdentifier.of("db", "files_inheritance_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();
    Table entriesTable = loadTable(tableIdentifier, "entries");
    Table filesTable = loadTable(tableIdentifier, "files");

    spark.sql(String.format(
        "CREATE TABLE parquet_table (data string, id int) " +
            "USING parquet PARTITIONED BY (id) LOCATION '%s'",
        temp.newFolder()));

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
     );

    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
    inputDF.select("data", "id").write()
        .mode("overwrite")
        .insertInto("parquet_table");

    try {
      String stagingLocation = table.location() + "/metadata";
      SparkTableUtil.importSparkTable(spark,
          new org.apache.spark.sql.catalyst.TableIdentifier("parquet_table"),
          table, stagingLocation);

      List<Row> actual = spark.read()
          .format("iceberg")
          .load(loadLocation(tableIdentifier, "files"))
          .collectAsList();

      List<GenericData.Record> expected = Lists.newArrayList();
      for (ManifestFile manifest : table.currentSnapshot().dataManifests()) {
        InputFile in = table.io().newInputFile(manifest.path());
        try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
          for (GenericData.Record record : rows) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            file.put(0, FileContent.DATA.id());
            expected.add(file);
          }
        }
      }

      Assert.assertEquals("Files table should have one row", 2, expected.size());
      Assert.assertEquals("Actual results should have one row", 2, actual.size());
      TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(0), actual.get(0));
      TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(1), actual.get(1));
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }

  }

  @Test
  public void testEntriesTableWithSnapshotIdInheritance() throws Exception {
    spark.sql("DROP TABLE IF EXISTS parquet_table");

    TableIdentifier tableIdentifier = TableIdentifier.of("db", "entries_inheritance_test");
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = createTable(tableIdentifier, SCHEMA, spec);

    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    spark.sql(String.format(
        "CREATE TABLE parquet_table (data string, id int) " +
            "USING parquet PARTITIONED BY (id) LOCATION '%s'",
        temp.newFolder()));

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );

    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
    inputDF.select("data", "id").write()
        .mode("overwrite")
        .insertInto("parquet_table");

    try {
      String stagingLocation = table.location() + "/metadata";
      SparkTableUtil.importSparkTable(
          spark, new org.apache.spark.sql.catalyst.TableIdentifier("parquet_table"), table, stagingLocation);

      List<Row> actual = spark.read()
          .format("iceberg")
          .load(loadLocation(tableIdentifier, "entries"))
          .select("sequence_number", "snapshot_id", "data_file")
          .collectAsList();

      table.refresh();

      long snapshotId = table.currentSnapshot().snapshotId();

      Assert.assertEquals("Entries table should have 2 rows", 2, actual.size());
      Assert.assertEquals("Sequence number must match", 0, actual.get(0).getLong(0));
      Assert.assertEquals("Snapshot id must match", snapshotId, actual.get(0).getLong(1));
      Assert.assertEquals("Sequence number must match", 0, actual.get(1).getLong(0));
      Assert.assertEquals("Snapshot id must match", snapshotId, actual.get(1).getLong(1));
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testFilesUnpartitionedTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "unpartitioned_files_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Table entriesTable = loadTable(tableIdentifier, "entries");
    Table filesTable = loadTable(tableIdentifier, "files");

    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    DataFile toDelete = Iterables.getOnlyElement(table.currentSnapshot().addedFiles());

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // delete the first file to test that only live files are listed
    table.newDelete().deleteFile(toDelete).commit();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "files"))
        .collectAsList();

    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests()) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
        for (GenericData.Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            file.put(0, FileContent.DATA.id());
            expected.add(file);
          }
        }
      }
    }

    Assert.assertEquals("Files table should have one row", 1, expected.size());
    Assert.assertEquals("Actual results should have one row", 1, actual.size());
    TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(0), actual.get(0));
  }

  @Test
  public void testAllMetadataTablesWithStagedCommits() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "stage_aggregate_table_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());

    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
    spark.conf().set("spark.wap.id", "1234567");
    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    List<Row> actualAllData = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_data_files"))
        .collectAsList();

    List<Row> actualAllManifests = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_manifests"))
        .collectAsList();

    List<Row> actualAllEntries = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_entries"))
        .collectAsList();

    Assert.assertTrue("Stage table should have some snapshots", table.snapshots().iterator().hasNext());
    Assert.assertEquals("Stage table should have null currentSnapshot",
        null, table.currentSnapshot());
    Assert.assertEquals("Actual results should have two rows", 2, actualAllData.size());
    Assert.assertEquals("Actual results should have two rows", 2, actualAllManifests.size());
    Assert.assertEquals("Actual results should have two rows", 2, actualAllEntries.size());
  }

  @Test
  public void testAllDataFilesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "files_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Table entriesTable = loadTable(tableIdentifier, "entries");
    Table filesTable = loadTable(tableIdentifier, "all_data_files");

    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // delete the first file to test that not only live files are listed
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 1)).commit();

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    // ensure table data isn't stale
    table.refresh();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_data_files"))
        .orderBy("file_path")
        .collectAsList();
    actual.sort(Comparator.comparing(o -> o.getString(1)));

    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : Iterables.concat(Iterables.transform(table.snapshots(), Snapshot::dataManifests))) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
        for (GenericData.Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            file.put(0, FileContent.DATA.id());
            expected.add(file);
          }
        }
      }
    }

    expected.sort(Comparator.comparing(o -> o.get("file_path").toString()));

    Assert.assertEquals("Files table should have two rows", 2, expected.size());
    Assert.assertEquals("Actual results should have two rows", 2, actual.size());
    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testHistoryTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "history_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Table historyTable = loadTable(tableIdentifier, "history");

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
    Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);

    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    long firstSnapshotTimestamp = table.currentSnapshot().timestampMillis();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    long secondSnapshotTimestamp = table.currentSnapshot().timestampMillis();
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // rollback the table state to the first snapshot
    table.rollback().toSnapshotId(firstSnapshotId).commit();
    long rollbackTimestamp = Iterables.getLast(table.history()).timestampMillis();

    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    long thirdSnapshotTimestamp = table.currentSnapshot().timestampMillis();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "history"))
        .collectAsList();

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(historyTable.schema(), "history"));
    List<GenericData.Record> expected = Lists.newArrayList(
        builder.set("made_current_at", firstSnapshotTimestamp * 1000)
            .set("snapshot_id", firstSnapshotId)
            .set("parent_id", null)
            .set("is_current_ancestor", true)
            .build(),
        builder.set("made_current_at", secondSnapshotTimestamp * 1000)
            .set("snapshot_id", secondSnapshotId)
            .set("parent_id", firstSnapshotId)
            .set("is_current_ancestor", false) // commit rolled back, not an ancestor of the current table state
            .build(),
        builder.set("made_current_at", rollbackTimestamp * 1000)
            .set("snapshot_id", firstSnapshotId)
            .set("parent_id", null)
            .set("is_current_ancestor", true)
            .build(),
        builder.set("made_current_at", thirdSnapshotTimestamp * 1000)
            .set("snapshot_id", thirdSnapshotId)
            .set("parent_id", firstSnapshotId)
            .set("is_current_ancestor", true)
            .build()
    );

    Assert.assertEquals("History table should have a row for each commit", 4, actual.size());
    TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(0), actual.get(0));
    TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(1), actual.get(1));
    TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(2), actual.get(2));
  }

  @Test
  public void testSnapshotsTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "snapshots_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Table snapTable = loadTable(tableIdentifier, "snapshots");

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
    Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);

    inputDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    long firstSnapshotTimestamp = table.currentSnapshot().timestampMillis();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    String firstManifestList = table.currentSnapshot().manifestListLocation();

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    long secondSnapshotTimestamp = table.currentSnapshot().timestampMillis();
    long secondSnapshotId = table.currentSnapshot().snapshotId();
    String secondManifestList = table.currentSnapshot().manifestListLocation();

    // rollback the table state to the first snapshot
    table.rollback().toSnapshotId(firstSnapshotId).commit();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "snapshots"))
        .collectAsList();

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(snapTable.schema(), "snapshots"));
    List<GenericData.Record> expected = Lists.newArrayList(
        builder.set("committed_at", firstSnapshotTimestamp * 1000)
            .set("snapshot_id", firstSnapshotId)
            .set("parent_id", null)
            .set("operation", "append")
            .set("manifest_list", firstManifestList)
            .set("summary", ImmutableMap.of(
                "added-records", "1",
                "added-data-files", "1",
                "changed-partition-count", "1",
                "total-data-files", "1",
                "total-records", "1"
            ))
            .build(),
        builder.set("committed_at", secondSnapshotTimestamp * 1000)
            .set("snapshot_id", secondSnapshotId)
            .set("parent_id", firstSnapshotId)
            .set("operation", "delete")
            .set("manifest_list", secondManifestList)
            .set("summary", ImmutableMap.of(
                "deleted-records", "1",
                "deleted-data-files", "1",
                "changed-partition-count", "1",
                "total-records", "0",
                "total-data-files", "0"
            ))
            .build()
    );

    Assert.assertEquals("Snapshots table should have a row for each snapshot", 2, actual.size());
    TestHelpers.assertEqualsSafe(snapTable.schema().asStruct(), expected.get(0), actual.get(0));
    TestHelpers.assertEqualsSafe(snapTable.schema().asStruct(), expected.get(1), actual.get(1));
  }

  @Test
  public void testManifestsTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "manifests_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Table manifestTable = loadTable(tableIdentifier, "manifests");
    Dataset<Row> df1 = spark.createDataFrame(
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(null, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "manifests"))
        .collectAsList();

    table.refresh();

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        manifestTable.schema(), "manifests"));
    GenericRecordBuilder summaryBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        manifestTable.schema().findType("partition_summaries.element").asStructType(), "partition_summary"));
    List<GenericData.Record> expected = Lists.transform(table.currentSnapshot().allManifests(), manifest ->
        builder.set("path", manifest.path())
            .set("length", manifest.length())
            .set("partition_spec_id", manifest.partitionSpecId())
            .set("added_snapshot_id", manifest.snapshotId())
            .set("added_data_files_count", manifest.addedFilesCount())
            .set("existing_data_files_count", manifest.existingFilesCount())
            .set("deleted_data_files_count", manifest.deletedFilesCount())
            .set("partition_summaries", Lists.transform(manifest.partitions(), partition ->
                summaryBuilder
                    .set("contains_null", true)
                    .set("contains_nan", false)
                    .set("lower_bound", "1")
                    .set("upper_bound", "1")
                    .build()
                ))
            .set("content", manifest.content().id())
            .build()
    );

    Assert.assertEquals("Manifests table should have one manifest row", 1, actual.size());
    TestHelpers.assertEqualsSafe(manifestTable.schema().asStruct(), expected.get(0), actual.get(0));
  }

  @Test
  public void testAllManifestsTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "manifests_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Table manifestTable = loadTable(tableIdentifier, "all_manifests");
    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);

    List<ManifestFile> manifests = Lists.newArrayList();

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    manifests.addAll(table.currentSnapshot().allManifests());

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    manifests.addAll(table.currentSnapshot().allManifests());

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "all_manifests"))
        .orderBy("path")
        .collectAsList();

    table.refresh();

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        manifestTable.schema(), "manifests"));
    GenericRecordBuilder summaryBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        manifestTable.schema().findType("partition_summaries.element").asStructType(), "partition_summary"));
    List<GenericData.Record> expected = Lists.newArrayList(Iterables.transform(manifests, manifest ->
        builder.set("path", manifest.path())
            .set("length", manifest.length())
            .set("partition_spec_id", manifest.partitionSpecId())
            .set("added_snapshot_id", manifest.snapshotId())
            .set("added_data_files_count", manifest.addedFilesCount())
            .set("existing_data_files_count", manifest.existingFilesCount())
            .set("deleted_data_files_count", manifest.deletedFilesCount())
            .set("partition_summaries", Lists.transform(manifest.partitions(), partition ->
                summaryBuilder
                    .set("contains_null", false)
                    .set("contains_nan", false)
                    .set("lower_bound", "1")
                    .set("upper_bound", "1")
                    .build()
            ))
            .set("content", manifest.content().id())
            .build()
    ));

    expected.sort(Comparator.comparing(o -> o.get("path").toString()));

    Assert.assertEquals("Manifests table should have two manifest rows", 2, actual.size());
    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(manifestTable.schema().asStruct(), expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testUnpartitionedPartitionsTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "unpartitioned_partitions_test");
    createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

    Dataset<Row> df = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    Types.StructType expectedSchema = Types.StructType.of(
        required(2, "record_count", Types.LongType.get()),
        required(3, "file_count", Types.IntegerType.get()));

    Table partitionsTable = loadTable(tableIdentifier, "partitions");

    Assert.assertEquals("Schema should not have partition field",
        expectedSchema, partitionsTable.schema().asStruct());

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        partitionsTable.schema(), "partitions"));
    GenericData.Record expectedRow = builder
        .set("record_count", 1L)
        .set("file_count", 1)
        .build();

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "partitions"))
        .collectAsList();

    Assert.assertEquals("Unpartitioned partitions table should have one row", 1, actual.size());
    TestHelpers.assertEqualsSafe(expectedSchema, expectedRow, actual.get(0));
  }

  @Test
  public void testPartitionsTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "partitions_test");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Table partitionsTable = loadTable(tableIdentifier, "partitions");
    Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
    Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

    df1.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    table.refresh();
    long firstCommitId = table.currentSnapshot().snapshotId();

    // add a second file
    df2.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    List<Row> actual = spark.read()
        .format("iceberg")
        .load(loadLocation(tableIdentifier, "partitions"))
        .orderBy("partition.id")
        .collectAsList();

    GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        partitionsTable.schema(), "partitions"));
    GenericRecordBuilder partitionBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(
        partitionsTable.schema().findType("partition").asStructType(), "partition"));
    List<GenericData.Record> expected = Lists.newArrayList();
    expected.add(builder
        .set("partition", partitionBuilder.set("id", 1).build())
        .set("record_count", 1L)
        .set("file_count", 1)
        .build());
    expected.add(builder
        .set("partition", partitionBuilder.set("id", 2).build())
        .set("record_count", 1L)
        .set("file_count", 1)
        .build());

    Assert.assertEquals("Partitions table should have two rows", 2, expected.size());
    Assert.assertEquals("Actual results should have two rows", 2, actual.size());
    for (int i = 0; i < 2; i += 1) {
      TestHelpers.assertEqualsSafe(partitionsTable.schema().asStruct(), expected.get(i), actual.get(i));
    }

    // check time travel
    List<Row> actualAfterFirstCommit = spark.read()
        .format("iceberg")
        .option("snapshot-id", String.valueOf(firstCommitId))
        .load(loadLocation(tableIdentifier, "partitions"))
        .orderBy("partition.id")
        .collectAsList();

    Assert.assertEquals("Actual results should have one row", 1, actualAfterFirstCommit.size());
    TestHelpers.assertEqualsSafe(partitionsTable.schema().asStruct(), expected.get(0), actualAfterFirstCommit.get(0));
  }

  @Test
  public void testRemoveOrphanFilesActionSupport() throws InterruptedException {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    Table table = createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "1")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(loadLocation(tableIdentifier));

    df.write().mode("append").parquet(table.location() + "/data");

    // sleep for 1 second to ensure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result1 = actions.removeOrphanFiles()
        .location(table.location() + "/metadata")
        .olderThan(System.currentTimeMillis())
        .execute();
    Assert.assertTrue("Should not delete any metadata files", result1.isEmpty());

    List<String> result2 = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();
    Assert.assertEquals("Should delete 1 data file", 1, result2.size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(loadLocation(tableIdentifier));
    List<SimpleRecord> actualRecords = resultDF
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", records, actualRecords);
  }
}
