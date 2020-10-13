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
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.Files.localOutput;

@RunWith(Parameterized.class)
public abstract class TestParquetScan extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestParquetScan.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestParquetScan.spark;
    TestParquetScan.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[] parameters() {
    return new Object[] { false, true };
  }

  private final boolean vectorized;

  public TestParquetScan(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Assume.assumeTrue("Cannot handle non-string map keys in parquet-avro",
        null == TypeUtil.find(
            schema,
            type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    File parquetFile = new File(dataFolder,
        FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location.toString());

    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    Schema tableSchema = table.schema();

    List<GenericData.Record> expected = RandomData.generateList(tableSchema, 100, 1L);

    try (FileAppender<GenericData.Record> writer = Parquet.write(localOutput(parquetFile))
        .schema(tableSchema)
        .build()) {
      writer.addAll(expected);
    }

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withFileSizeInBytes(parquetFile.length())
        .withPath(parquetFile.toString())
        .withRecordCount(100)
        .build();

    table.newAppend().appendFile(file).commit();
    table.updateProperties().set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized)).commit();

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Should contain 100 rows", 100, rows.size());

    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(tableSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
