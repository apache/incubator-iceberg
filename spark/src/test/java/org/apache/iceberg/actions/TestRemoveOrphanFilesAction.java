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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestRemoveOrphanFilesAction extends SparkTestBase {

  public static final String USER_LOG_DATA_DUMMY_FILE = "/user/log/data/dummy_file";
  public static final String HDFS_USER_LOG_DATA_DUMMY_FILE = "hdfs://user/log/data/dummy_file";
  public static final String HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE =
      "hdfs://servicename/user/log/data/dummy_file";
  public static final String HDFS_HOST_PORT_USER_LOG_DATA_DUMMY_FILE =
      "hdfs://localhost:8020/user/log/data/dummy_file";
  public static final String HDFS_SERVICENAME1_USER_LOG_DATA_DUMMY_FILE =
      "hdfs://servicename1/user/log/data/dummy_file";
  public static final String HDFS_THREE_FWD_SLASHES_USER_LOG_DATA_DUMMY_FILE =
      "hdfs:///user/log/data/dummy_file";
  public static final String USER_LOG_DATA_SPACE_DUMMY_FILE = "/user/log/space data/dummy_file";
  public static final String HDFS_SERVICENAME_USER_LOG_DATA_SPACE_DUMMY_FILE =
      "/user/log/space data/dummy_file";

  public static final String USER_LOG_DATA_SPECIAL_DUMMY_FILE = "/user/log/*space data/\\dummy_file";
  public static final String HDFS_SERVICENAME_USER_LOG_DATA_SPECIAL_DUMMY_FILE =
      "hdfs://servicename/user/log/*space data/\\dummy_file";

  public static final String HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE =
      "hdfs://servicename/user/log/data/orphan_file";
  public static final String HDFS_HOST_PORT_USER_LOG_DATA_ORPHAN_FILE =
      "hdfs://localhost:8020/user/log/data/orphan_file";
  public static final String HDFS_SERVICENAME1_USER_LOG_DATA_ORPHAN_FILE =
      "hdfs://servicename1/user/log/data/orphan_file";
  public static final String HDFS_THREE_FWD_SLASHES_USER_LOG_DATA_ORPHAN_FILE =
      "hdfs:///user/log/data/orphan_file";
  public static final String HDFS_SERVICENAME_USER_LOG_DATA_SPACE_ORPHAN_FILE =
      "hdfs://servicename/user/log/space data/orphan_file";
  public static final String HDFS_SERVICENAME_USER_LOG_DATA_SPECIAL_ORPHAN_FILE =
      "hdfs://servicename/user/log/*space data/\\orphan_file";

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );
  private static final Schema SCHEMA_WITH_SPACE_ESCAPED = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "`c2 with space`", Types.StringType.get()),
      optional(3, "`c3 with space`", Types.StringType.get())
  );


  private static final Schema SCHEMA_WITH_SPACE = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2 with space", Types.StringType.get()),
      optional(3, "c3 with space", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .truncate("c2", 2)
      .identity("c3")
      .build();

  private static final PartitionSpec SPEC_WITH_SPACE = PartitionSpec.builderFor(SCHEMA_WITH_SPACE)
      .truncate("c2 with space", 2)
      .identity("c3 with space")
      .build();

  private static final PartitionSpec SPEC_WITH_SPACE_ESCAPED = PartitionSpec.builderFor(SCHEMA_WITH_SPACE_ESCAPED)
      .truncate("`c2 with space`", 2)
      .identity("`c3 with space`")
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testDryRun() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<String> validFiles = spark.read().format("iceberg")
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 2 valid files", 2, validFiles.size());

    df.write().mode("append").parquet(tableLocation + "/data");

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles = Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
        .filter(FileStatus::isFile)
        .map(file -> file.getPath().toString())
        .collect(Collectors.toList());
    Assert.assertEquals("Should be 3 files", 3, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeAll(validFiles);
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result1 = actions.removeOrphanFiles()
        .deleteWith(s -> { })
        .execute();
    Assert.assertTrue("Default olderThan interval should be safe", result1.isEmpty());

    List<String> result2 = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .deleteWith(s -> { })
        .execute();
    Assert.assertEquals("Action should find 1 file", invalidFiles, result2);
    Assert.assertTrue("Invalid file should be present", fs.exists(new Path(invalidFiles.get(0))));

    List<String> result3 = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();
    Assert.assertEquals("Action should delete 1 file", invalidFiles, result3);
    Assert.assertFalse("Invalid file should not be present", fs.exists(new Path(invalidFiles.get(0))));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testAllValidFilesAreKept() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df1 = spark.createDataFrame(records1, ThreeColumnRecord.class).coalesce(1);

    // original append
    df1.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df2 = spark.createDataFrame(records2, ThreeColumnRecord.class).coalesce(1);

    // dynamic partition overwrite
    df2.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("overwrite")
        .save(tableLocation);

    // second append
    df2.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    List<String> snapshotFiles1 = snapshotFiles(snapshots.get(0).snapshotId());
    Assert.assertEquals(1, snapshotFiles1.size());

    List<String> snapshotFiles2 = snapshotFiles(snapshots.get(1).snapshotId());
    Assert.assertEquals(1, snapshotFiles2.size());

    List<String> snapshotFiles3 = snapshotFiles(snapshots.get(2).snapshotId());
    Assert.assertEquals(2, snapshotFiles3.size());

    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/invalid/invalid");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete 4 files", 4, result.size());

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());

    for (String fileLocation : snapshotFiles1) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }

    for (String fileLocation : snapshotFiles2) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }

    for (String fileLocation : snapshotFiles3) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }
  }

  @Test
  public void testWapFilesAreKept() throws InterruptedException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    // normal write
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    spark.conf().set("spark.wap.id", "1");

    // wap write
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Should not return data from the staged snapshot", records, actualRecords);

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertTrue("Should not delete any files", result.isEmpty());
  }

  @Test
  public void testMetadataFolderIsIntact() throws InterruptedException {
    // write data directly to the table location
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_NEW_DATA_LOCATION, tableLocation);
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/c2_trunc=AA/c3=AAAA");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete 1 file", 1, result.size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testOlderThanTimestamp() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    Thread.sleep(1000);

    long timestamp = System.currentTimeMillis();

    Thread.sleep(1000);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(timestamp)
        .execute();

    Assert.assertEquals("Should delete only 2 files", 2, result.size());
  }

  @Test
  public void testOlderThanTimestampWithPartitionWithWhitSpace()
      throws InterruptedException, IOException, AnalysisException {
    String partitionCol1 = "c2 with space";
    String partitionCol2 = "c3 with space";
    String spacedTableName = "whitespacetable_with_space";

    List<Row> record1 = Lists.newArrayList(RowFactory.create(Integer.valueOf(1), "AA AA", "BB BB"));
    List<Row> record2 = Lists.newArrayList(RowFactory.create(Integer.valueOf(2), "CC CC", "DD DD"));

    Dataset<Row> df1 = spark.createDataFrame(record1, SparkSchemaUtil.convert(SCHEMA));
    Dataset<Row> df2 = spark.createDataFrame(record2, SparkSchemaUtil.convert(SCHEMA));

    df1.withColumnRenamed("c2", partitionCol1)
        .withColumnRenamed("c3", partitionCol2)
        .write().mode("overwrite").partitionBy(partitionCol1, partitionCol2).format("parquet")
        .saveAsTable(spacedTableName);

    File icebergLocation = temp.newFolder("partitioned_table_with_space");

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, spacedTableName),
        SparkSchemaUtil.specForTable(spark, spacedTableName),
        ImmutableMap.of(),
        icebergLocation.getCanonicalPath());

    df1.withColumnRenamed("c2", partitionCol1)
        .withColumnRenamed("c3", partitionCol2)
        .write().mode("overwrite").partitionBy(partitionCol1, partitionCol2).format("parquet")
        .save(icebergLocation.getCanonicalPath() + "/data");

    df2.withColumnRenamed("c2", partitionCol1)
        .withColumnRenamed("c3", partitionCol2)
        .write().mode("overwrite").partitionBy(partitionCol1, partitionCol2).format("parquet")
        .save(icebergLocation.getCanonicalPath() + "/data");

    df2.withColumnRenamed("c2", partitionCol1)
        .withColumnRenamed("c3", partitionCol2)
        .write().mode("overwrite").partitionBy(partitionCol1, partitionCol2).format("iceberg")
        .save(icebergLocation.toString());

    List<Row> initialResult = spark.read().format("iceberg").load(icebergLocation.toString())
        .withColumnRenamed(partitionCol1, "c2")
        .withColumnRenamed(partitionCol2, "c3")
        .as(RowEncoder.apply(SparkSchemaUtil.convert(SCHEMA)))
        .collectAsList();

    Assert.assertEquals("should match only 1 record after insert", initialResult, record2);

    Thread.sleep(1000);

    long timestamp = System.currentTimeMillis();

    Thread.sleep(1000);

    // add another orphan file, as now there are 3 files, but 2 should be removed.
    df2.withColumnRenamed("c2", partitionCol1)
        .withColumnRenamed("c3", partitionCol2)
        .write().mode("append").partitionBy(partitionCol1, partitionCol2).format("parquet")
        .save(icebergLocation.getCanonicalPath() + "/data");

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(timestamp)
        .execute();

    Assert.assertEquals("Should delete only 2 files", 2, result.size());

    List<Row> resultsAfterRemove = spark.read().format("iceberg").load(icebergLocation.toString())
        .withColumnRenamed(partitionCol1, "c2")
        .withColumnRenamed(partitionCol2, "c3")
        .as(RowEncoder.apply(SparkSchemaUtil.convert(SCHEMA)))
        .collectAsList();

    Assert.assertEquals("should match records after remove", initialResult, resultsAfterRemove);
  }

  @Test
  public void testRemoveUnreachableMetadataVersionFiles() throws InterruptedException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_NEW_DATA_LOCATION, tableLocation);
    props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete 1 file", 1, result.size());
    Assert.assertTrue("Should remove v1 file", result.get(0).contains("v1.metadata.json"));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testManyTopLevelPartitions() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i)));
    }

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertTrue("Should not delete any files", result.isEmpty());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testManyLeafPartitions() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i % 3), String.valueOf(i)));
    }

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertTrue("Should not delete any files", result.isEmpty());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testRemoveOrphanFilesWithRelativeFilePath() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableDir.getAbsolutePath());

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableDir.getAbsolutePath());

    List<String> validFiles = spark.read().format("iceberg")
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 1 valid files", 1, validFiles.size());
    String validFile = validFiles.get(0);

    df.write().mode("append").parquet(tableLocation + "/data");

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles = Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
        .filter(FileStatus::isFile)
        .map(file -> file.getPath().toString())
        .collect(Collectors.toList());
    Assert.assertEquals("Should be 2 files", 2, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeIf(file -> file.contains(validFile));
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);
    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .deleteWith(s -> { })
        .execute();
    Assert.assertEquals("Action should find 1 file", invalidFiles, result);
    Assert.assertTrue("Invalid file should be present", fs.exists(new Path(invalidFiles.get(0))));
  }

  @Test
  public void testRemoveOrphanFilesWithSpecialCharsFilePath() throws IOException, InterruptedException {
    File whiteSpaceDir =  new File(temp.newFolder() + "/white space");
    whiteSpaceDir.mkdirs();

    Table table = TABLES.create(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Maps.newHashMap(),
            whiteSpaceDir.getAbsolutePath());

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(whiteSpaceDir.getAbsolutePath());

    List<String> validFiles = spark.read().format("iceberg")
        .load(whiteSpaceDir + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 1 valid files", 1, validFiles.size());
    String validFile = validFiles.get(0);

    df.write().mode("append").parquet(whiteSpaceDir + "/data");

    Path dataPath = new Path(whiteSpaceDir + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles = Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
        .filter(FileStatus::isFile)
        .map(file -> file.getPath().toString())
        .collect(Collectors.toList());
    Assert.assertEquals("Should be 2 files", 2, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeIf(file -> file.contains(validFile));
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);
    List<String> result = actions.removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();
    Assert.assertEquals("Action should find 1 file", invalidFiles, result);
    Assert.assertTrue("Invalid file should be present", !fs.exists(new Path(invalidFiles.get(0))));
    Assert.assertTrue("Invalid file should match", result.get(0).equals(invalidFiles.get(0)));
  }

  @Test
  public void testRemoveOrphanFilesWithHadoopCatalog() throws InterruptedException {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tableLocation);
    String namespaceName = "testDb";
    String tableName = "testTb";

    Namespace namespace = Namespace.of(namespaceName);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
    Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap());

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(table.location());

    df.write().mode("append").parquet(table.location() + "/data");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    table.refresh();

    List<String> result = Actions.forTable(table)
        .removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete only 1 files", 1, result.size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(table.location());
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testFindOrphanFilesWithSameAuthority() throws Exception {
    executeTest(HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 3,
        HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 3,
        HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithValidFileHasNoAuthority() throws Exception {
    executeTest(HDFS_USER_LOG_DATA_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithDifferentAuthority() throws Exception {
    executeTest(HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME1_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME1_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithValidFilesHasThreeForwardSlashes() throws URISyntaxException {
    executeTest(HDFS_THREE_FWD_SLASHES_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithActualFilesHasThreeForwardSlashes() throws URISyntaxException {
    executeTest(HDFS_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_THREE_FWD_SLASHES_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_THREE_FWD_SLASHES_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithActualFilesHasHostPort() throws URISyntaxException {
    executeTest(HDFS_USER_LOG_DATA_DUMMY_FILE, 4, HDFS_HOST_PORT_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_HOST_PORT_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithValiFilesHasAuthoritydAndActualFilesHasHostPort() throws URISyntaxException {
    executeTest(HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_HOST_PORT_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_HOST_PORT_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithValidFilesHasNoScheme() throws URISyntaxException {
    executeTest(USER_LOG_DATA_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithSimilarFiles() throws URISyntaxException {
    executeTest(USER_LOG_DATA_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithSimilarFilesAndActualFilesHasAuthority() throws URISyntaxException {
    executeTest(HDFS_USER_LOG_DATA_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        null, 0);
  }

  @Test
  public void testFindOrphanFilesWithSimilarFilesAndValidFilesHasNoScheme() throws URISyntaxException {
    executeTest(USER_LOG_DATA_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_DUMMY_FILE, 4,
        null, 0);
  }

  @Test
  public void testFindOrphanFilesWithSimilarFilesAndPathHasSpaceChars() throws URISyntaxException {
    executeTest(USER_LOG_DATA_SPACE_DUMMY_FILE, 4, HDFS_SERVICENAME_USER_LOG_DATA_SPACE_DUMMY_FILE, 4,
        null, 0);
  }

  @Test
  public void testFindOrphanFilesWithPathHasSpaceChars() throws URISyntaxException, IOException {
    executeTest(USER_LOG_DATA_SPACE_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_SPACE_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_SPACE_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithSimilarFilesAndPathHasSpecialChars() throws URISyntaxException {
    executeTest(USER_LOG_DATA_SPECIAL_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_SPECIAL_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_SPECIAL_ORPHAN_FILE, 4);
  }

  @Test
  public void testFindOrphanFilesWithPathHasSpecialChars() throws URISyntaxException {
    executeTest(USER_LOG_DATA_SPECIAL_DUMMY_FILE, 4,
        HDFS_SERVICENAME_USER_LOG_DATA_SPECIAL_DUMMY_FILE, 4, null, 0);
  }


  private List<String> snapshotFiles(long snapshotId) {
    return spark.read().format("iceberg")
        .option("snapshot-id", snapshotId)
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
  }

  private static StructType constructStructureWithString() {
    StructType customStructType = new StructType();
    customStructType = customStructType.add("file_path", DataTypes.StringType, true);
    return customStructType;
  }

  private List<Row> getRowsWithFilePath(String completeFilePath, int numOfRows) throws URISyntaxException {
    Row fileRow = null;
    List<Row> rowFiles = new ArrayList<>();
    for (int i = 0; i < numOfRows; i++) {
      fileRow = RowFactory.create(completeFilePath + i);
      rowFiles.add(fileRow);
    }

    return rowFiles;
  }

  private void executeTest(
      String validFilePathPrefix,
      int numberOfValidFiles,
      String actualFilePrefix,
      int numberOfActualFiles,
      String orphanFilesPrefix,
      int numberOfOrphanFiles) throws URISyntaxException {

    List<Row> validFilesData = getRowsWithFilePath(validFilePathPrefix, numberOfValidFiles);

    Dataset<Row> validFileDS = RemoveOrphanFilesAction
            .addFilePathOnlyColumn(
                    spark.createDataset(validFilesData, RowEncoder.apply(constructStructureWithString())));

    List<Row> actualFilesData = new ArrayList<>();
    List<Row> expectedOrphanFiles = new ArrayList<>();

    actualFilesData.addAll(getRowsWithFilePath(actualFilePrefix, numberOfActualFiles));

    if (orphanFilesPrefix != null) {
      expectedOrphanFiles.addAll(getRowsWithFilePath(orphanFilesPrefix, numberOfOrphanFiles));
      actualFilesData.addAll(expectedOrphanFiles);
    }

    Dataset<Row> actualFileDS = RemoveOrphanFilesAction
            .addFilePathOnlyColumn(
                    spark.createDataset(actualFilesData, RowEncoder.apply(constructStructureWithString())));

    List<String> orphanFiles = RemoveOrphanFilesAction.findOrphanFiles(validFileDS, actualFileDS);

    Assert.assertEquals(expectedOrphanFiles.stream().map(row -> row.get(0).toString())
            .collect(Collectors.toList()), orphanFiles);
  }
}
