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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Ordering;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Conversions.fromByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestRepairManifestsAction extends SparkTestBase {

  @Parameterized.Parameters(name = "snapshotIdInheritanceEnabled = {0}")
  public static Object[] parameters() {
    return new Object[] { "true", "false" };
  }

  public TestRepairManifestsAction(String snapshotIdInheritanceEnabled) {
    this.snapshotIdInheritanceEnabled = snapshotIdInheritanceEnabled;
  }

  private final String snapshotIdInheritanceEnabled;
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
  public void testRepairMetricsUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

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

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    List<SparkDataFile> filesBefore = getManifestEntries(table);
    assertLowerBoundPresent(filesBefore, 1, Optional.empty(), 4,
        "Should have no lower bounds before repair");
    assertUpperBoundPresent(filesBefore, 1, Optional.empty(), 4,
        "Should have no upper bounds before repair");
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)").commit();

    List<String> manifestPathsBefore = getManifestFilePaths(table);

    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests.repairIf(manifest -> true).execute();

    Assert.assertEquals("Action should have added 2 manifest", 2, result.addedManifests().size());
    Assert.assertEquals("Action should have deleted 2 manifest", 2, result.deletedManifests().size());

    table.refresh();
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests after rewrite", 2, newManifests.size());

    List<String> manifestPathsAfter = getManifestFilePaths(table);
    Assert.assertEquals("Results should report previous manifests",
        Ordering.natural().sortedCopy(result.deletedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsBefore));
    Assert.assertEquals("Results should report after manifests",
        Ordering.natural().sortedCopy(result.addedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsAfter));

    Assert.assertEquals(2, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());
    Assert.assertEquals(2, (long) newManifests.get(1).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    List<SparkDataFile> files = getManifestEntries(table);
    assertLowerBoundPresent(files, 1, Optional.of(1), 2,
        "Should have correct lower bounds after repair");
    assertLowerBoundPresent(files, 1, Optional.of(2), 2,
        "Should have correct lower bounds after repair");
    assertUpperBoundPresent(files, 1, Optional.of(1), 2,
        "Should have correct lower bounds after repair");
    assertUpperBoundPresent(files, 1, Optional.of(2), 2,
        "Should have correct lower bounds after repair");

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
  public void testRepairMetricsPartitioned() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

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
        new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"),
        new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
        new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests before rewrite", 4, manifests.size());
    List<SparkDataFile> filesBefore = getManifestEntries(table);
    assertLowerBoundPresent(filesBefore, 1, Optional.empty(), 8,
        "Should have no lower bounds before repair");
    assertUpperBoundPresent(filesBefore, 1, Optional.empty(), 8,
        "Should have no upper bounds before repair");

    List<String> manifestPathsBefore = getManifestFilePaths(table);

    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)").commit();

    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests.repairIf(manifest -> true).execute();

    Assert.assertEquals("Action should have added 4 manifest", 4, result.addedManifests().size());
    Assert.assertEquals("Action should have deleted 4 manifest", 4, result.deletedManifests().size());
    table.refresh();

    List<String> manifestPathsAfter = getManifestFilePaths(table);
    Assert.assertEquals("Results should report previous manifests",
        Ordering.natural().sortedCopy(result.deletedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsBefore));
    Assert.assertEquals("Results should report after manifests",
        Ordering.natural().sortedCopy(result.addedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsAfter));

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests after rewrite", 4, newManifests.size());

    Assert.assertEquals(2, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    Assert.assertEquals(2, (long) newManifests.get(1).existingFilesCount());
    Assert.assertFalse(newManifests.get(1).hasAddedFiles());
    Assert.assertFalse(newManifests.get(1).hasDeletedFiles());

    Assert.assertEquals(2, (long) newManifests.get(2).existingFilesCount());
    Assert.assertFalse(newManifests.get(2).hasAddedFiles());
    Assert.assertFalse(newManifests.get(2).hasDeletedFiles());

    Assert.assertEquals(2, (long) newManifests.get(3).existingFilesCount());
    Assert.assertFalse(newManifests.get(3).hasAddedFiles());
    Assert.assertFalse(newManifests.get(3).hasDeletedFiles());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairFileSizeRecordCountUnpartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
        createGenericRecord(1, null, "AAAA"),
        createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
        createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
        createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
        .withPath(f1.getPath())
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df2 = DataFiles.builder(spec)
        .withPath(f2.getPath())
        .withFileSizeInBytes(20)
        .withRecordCount(2)
        .withFormat(FileFormat.PARQUET)
        .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());
    List<SparkDataFile> entriesBefore = getManifestEntries(table);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        10, 1);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        20, 2);

    List<String> manifestPathsBefore = getManifestFilePaths(table);

    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests.repairIf(manifest -> true).execute();

    Assert.assertEquals("Action should have added 2 manifest", 2, result.addedManifests().size());
    Assert.assertEquals("Action should have deleted 2 manifest", 2, result.deletedManifests().size());

    table.refresh();
    List<String> manifestPathsAfter = getManifestFilePaths(table);
    Assert.assertEquals("Results should report previous manifests",
        Ordering.natural().sortedCopy(result.deletedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsBefore));
    Assert.assertEquals("Results should report after manifests",
        Ordering.natural().sortedCopy(result.addedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsAfter));

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f1.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f2.length(), 2);

    List<ThreeColumnRecord> expectedRecords = Stream.concat(records1.stream(), records2.stream())
        .map(r -> new ThreeColumnRecord(
            (Integer) r.getField("c1"),
            (String) r.getField("c2"),
            (String) r.getField("c3")))
        .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairFileSizeRecordCountPartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
        createGenericRecord(1, null, "AAAA"),
        createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
        createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
        createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    List<Record> records3 = Lists.newArrayList(
        createGenericRecord(3, "EEEEEEEEEE", "EEEE"),
        createGenericRecord(3, "FFFFFFFFFF", "FFFF")
    );
    List<Record> records4 = Lists.newArrayList(
        createGenericRecord(4, "GGGGGGGGGG", "GGGG"),
        createGenericRecord(4, "HHHHHHHHHG", "HHHH")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);
    File f3 = writeToFile(records3, FileFormat.PARQUET);
    File f4 = writeToFile(records4, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
        .withPath(f1.getPath())
        .withFileSizeInBytes(10)
        .withPartitionPath("c1=1")
        .withRecordCount(1)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df2 = DataFiles.builder(spec)
        .withPath(f2.getPath())
        .withFileSizeInBytes(20)
        .withPartitionPath("c1=2")
        .withRecordCount(2)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df3 = DataFiles.builder(spec)
        .withPath(f3.getPath())
        .withFileSizeInBytes(30)
        .withPartitionPath("c1=3")
        .withRecordCount(3)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df4 = DataFiles.builder(spec)
        .withPath(f4.getPath())
        .withFileSizeInBytes(40)
        .withPartitionPath("c1=4")
        .withRecordCount(4)
        .withFormat(FileFormat.PARQUET)
        .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.newFastAppend().appendFile(df3).commit();
    table.newFastAppend().appendFile(df4).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests before rewrite", 4, manifests.size());
    List<SparkDataFile> entriesBefore = getManifestEntries(table);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        10, 1);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        20, 2);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        30, 3);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
        40, 4);

    List<String> manifestPathsBefore = getManifestFilePaths(table);

    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)").commit();
    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests.repairIf(manifest -> true).execute();

    Assert.assertEquals("Action should added 4 manifests", 4, result.addedManifests().size());
    Assert.assertEquals("Action should deleted 4 manifests", 4, result.deletedManifests().size());

    table.refresh();
    List<String> manifestPathsAfter = getManifestFilePaths(table);
    Assert.assertEquals("Results should report previous manifests",
        Ordering.natural().sortedCopy(result.deletedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsBefore));
    Assert.assertEquals("Results should report after manifests",
        Ordering.natural().sortedCopy(result.addedManifests()
            .stream().map(ManifestFile::path).collect(Collectors.toList())),
        Ordering.natural().sortedCopy(manifestPathsAfter));

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f1.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f2.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f3.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
        entriesAfter, f4.length(), 2);

    List<ThreeColumnRecord> expectedRecords = Stream.concat(
        Stream.concat(records1.stream(), records2.stream()),
        Stream.concat(records3.stream(), records4.stream()))
        .map(r -> new ThreeColumnRecord(
            (Integer) r.getField("c1"),
            (String) r.getField("c2"),
            (String) r.getField("c3")))
        .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairManifestsPredicate() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
        createGenericRecord(1, null, "AAAA"),
        createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
        createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
        createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
        .withPath(f1.toURI().toString())
        .withFileSizeInBytes(f1.length())
        .withRecordCount(100)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df2 = DataFiles.builder(spec)
        .withPath(f2.toURI().toString())
        .withFileSizeInBytes(f2.length())
        .withRecordCount(200)
        .withFormat(FileFormat.PARQUET)
        .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests
        .repairIf(manifest -> manifest.path().equals(manifests.get(0).path()))
        .execute();

    Assert.assertEquals("Action should rewrite 1 manifest", 1, result.deletedManifests().size());
    Assert.assertEquals("Action should add 1 manifests", 1, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should still have 2 manifests after repair", 2, newManifests.size());

    Assert.assertFalse("First manifest must be repaired", newManifests.contains(manifests.get(0)));
    Assert.assertTrue("Second manifest must not be repaired", newManifests.contains(manifests.get(1)));

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed record count after repair",
        entriesAfter, f2.length(), 2);
    assertFileSizeRecordCount("Should have initial record count before repair",
        entriesAfter, f1.length(), 100);

    // Data Validation
    List<ThreeColumnRecord> expectedRecords = Stream.concat(records1.stream(), records2.stream())
        .map(r -> new ThreeColumnRecord(
            (Integer) r.getField("c1"),
            (String) r.getField("c2"),
            (String) r.getField("c3")))
        .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairNoMetrics() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
        createGenericRecord(1, null, "AAAA"),
        createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
        createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
        createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
        .withPath(f1.toURI().toString())
        .withFileSizeInBytes(10)
        .withRecordCount(100)
        .withFormat(FileFormat.PARQUET)
        .build();
    DataFile df2 = DataFiles.builder(spec)
        .withPath(f2.toURI().toString())
        .withFileSizeInBytes(20)
        .withRecordCount(200)
        .withFormat(FileFormat.PARQUET)
        .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests
        .repairMetrics(false)
        .execute();

    Assert.assertEquals("Action should delete 2 manifest", 2, result.deletedManifests().size());
    Assert.assertEquals("Action should add 2 manifests", 2, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should still have 2 manifests after repair", 2, newManifests.size());

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed file length count after repair, but not record count",
        entriesAfter, f2.length(), 200);
    assertFileSizeRecordCount("Should have fixed file length count after repair, but not record count",
        entriesAfter, f1.length(), 100);
    Assert.assertFalse("Should not have repaired metrics", entriesAfter.stream().filter(
        f -> f.lowerBounds() != null).findAny().isPresent());
    Assert.assertFalse("Should not have repaired metrics", entriesAfter.stream().filter(
        f -> f.upperBounds() != null).findAny().isPresent());

    // Data Validation
    List<ThreeColumnRecord> expectedRecords = Stream.concat(records1.stream(), records2.stream())
        .map(r -> new ThreeColumnRecord(
            (Integer) r.getField("c1"),
            (String) r.getField("c2"),
            (String) r.getField("c3")))
        .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testNothingToRepair() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

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
        new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"),
        new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
        new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH")
    );
    writeRecords(records4);

    table.refresh();
    RepairManifests repairManifests = SparkActions.get().repairManifests(table);
    RepairManifests.Result result = repairManifests
        .execute();

    Assert.assertEquals("No manifests should have been added", 0, result.addedManifests().size());
    Assert.assertEquals("No manifests should have been added", 0, result.deletedManifests().size());
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

  private Record createGenericRecord(int c1, String c2, String c3) {
    GenericRecord result = GenericRecord.create(SCHEMA);
    result.setField("c1", c1);
    result.setField("c2", c2);
    result.setField("c3", c3);
    return result;
  }

  private File writeToFile(List<Record> records, FileFormat fileFormat) throws IOException {
    File file = temp.newFile();
    Assert.assertTrue(file.delete());

    GenericAppenderFactory factory = new GenericAppenderFactory(SCHEMA);
    try (FileAppender<Record> appender = factory.newAppender(Files.localOutput(file), fileFormat)) {
      appender.addAll(records);
    }
    return file;
  }

  protected List<SparkDataFile> getManifestEntries(Table table) {
    Dataset<Row> df = spark.read().format("iceberg").load(table.name() + "#entries");
    StructType sparkType = (StructType) df.schema().apply("data_file").dataType();
    Types.StructType dataFileType = DataFile.getType(table.spec().partitionType());
    List<Row> rows = df.collectAsList();
    return rows.stream().map(r -> {
      SparkDataFile wrapper = new SparkDataFile(dataFileType, sparkType);
      return wrapper.wrap(r.getStruct(3));
    }).collect(Collectors.toList());
  }

  protected List<String> getManifestFilePaths(Table table) {
    Dataset<Row> df = spark.read().format("iceberg").load(table.name() + "#manifests");
    return df.collectAsList().stream().map(r -> (String) r.getAs("path")).collect(Collectors.toList());
  }

  protected void assertLowerBoundPresent(List<SparkDataFile> manifestEntries,
                                         int fieldId,
                                         Optional<Integer> expectedBound, int expectedCount,
                                         String message) {
    List<Integer> lowerBounds = manifestEntries.stream().map(f ->
        (Integer) fromByteBuffer(Types.IntegerType.get(), f.lowerBounds().get(fieldId)))
        .collect(Collectors.toList());
    if (expectedBound.isPresent()) {
      Assert.assertEquals(message, expectedCount,
          lowerBounds.stream().filter(b -> b == expectedBound.get()).count());
    } else {
      Assert.assertEquals(message, expectedCount,
          lowerBounds.stream().filter(b -> b == null).count());
    }
  }

  private void assertUpperBoundPresent(List<SparkDataFile> manifestEntries, int fieldId,
                                       Optional<Integer> expectedBound, int expectedCount,
                                       String message) {
    List<Integer> upperBounds = manifestEntries.stream().map(f ->
        (Integer) fromByteBuffer(Types.IntegerType.get(), f.upperBounds().get(fieldId)))
        .collect(Collectors.toList());
    if (expectedBound.isPresent()) {
      Assert.assertEquals(message, expectedCount,
          upperBounds.stream().filter(b -> b == expectedBound.get()).count());
    } else {
      Assert.assertEquals(message, expectedCount,
          upperBounds.stream().filter(b -> b == null).count());
    }
  }

  private void assertFileSizeRecordCount(String message,
                                         List<SparkDataFile> manifestEntries,
                                         long fileSize,
                                         long recordCount) {
    Assert.assertTrue(manifestEntries.stream().filter(mf ->
        mf.fileSizeInBytes() == fileSize && mf.recordCount() == recordCount).count() > 0);
  }
}

