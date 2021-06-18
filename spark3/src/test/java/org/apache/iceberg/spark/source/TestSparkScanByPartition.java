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
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TestHelpers.Row;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;

public class TestSparkScanByPartition {
  private static final long MB = 1024 * 1024;

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();
  private static SparkSession spark = null;
  private static CaseInsensitiveStringMap options = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Table table = null;

  @BeforeClass
  public static void setupSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = spark;
    spark = null;
    currentSpark.stop();
  }

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
    options = new CaseInsensitiveStringMap(
        ImmutableMap.of(
            "path", table.location(),
            SparkReadOptions.BY_PARTITION, "true"));
  }

  @Test
  public void testBasic() {
    for (int i = 0; i < 4; i++) {
      List<DataFile> partitionFiles = newFiles(1, 32 * MB, Row.of(i));
      appendFiles(partitionFiles);
    }
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, options);
    Batch scan = builder.build().toBatch();
    InputPartition[] splits = scan.planInputPartitions();
    // even though size of each file is less than the split size, they are from different
    // partitions and therefore can't be put in the same bin.
    assertEquals(4, splits.length);
  }

  @Test
  public void testUnpartitioned() throws IOException {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    table = TABLES.create(SCHEMA, tableLocation);

    for (int i = 0; i < 4; i++) {
      List<DataFile> partitionFiles = newFiles(1, 32 * MB, Row.of(i));
      appendFiles(partitionFiles);
    }
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, options);

    Batch scan = builder.build().toBatch();
    InputPartition[] splits = scan.planInputPartitions();
    assertEquals(1, splits.length);
  }

  @Test
  public void testGroupWithinPartition() {
    for (int i = 0; i < 4; i++) {
      List<DataFile> files = newFiles(4, 32 * MB, Row.of(i));
      appendFiles(files);
    }
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, options);
    InputPartition[] splits = builder.build().toBatch().planInputPartitions();
    assertEquals(4, splits.length);

    // add more files for each partition and we should get more splits
    for (int i = 0; i < 4; i++) {
      List<DataFile> files = newFiles(4, 64 * MB, Row.of(i));
      appendFiles(files);
    }
    splits = builder.build().toBatch().planInputPartitions();
    assertEquals(12, splits.length);
  }

  @Test
  public void testMergeScan() {
    for (int i = 0; i < 4; i++) {
      List<DataFile> files = newFiles(6, 32 * MB, Row.of(i));
      appendFiles(files);
    }

    LogicalWriteInfo info = new LogicalWriteInfoImpl(
        UUID.randomUUID().toString(), SparkSchemaUtil.convert(table.schema()), options);
    SparkMergeBuilder builder = new SparkMergeBuilder(spark, table, "merge", info);
    InputPartition[] splits = builder.asScanBuilder().build().toBatch().planInputPartitions();
    assertEquals(8, splits.length);
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
