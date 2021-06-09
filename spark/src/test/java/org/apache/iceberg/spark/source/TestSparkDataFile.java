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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataPathUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class TestSparkDataFile {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      required(100, "id", Types.LongType.get()),
      optional(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      optional(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      optional(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      optional(107, "date", Types.DateType.get()),
      required(108, "ts", Types.TimestampType.withZone()),
      required(110, "s", Types.StringType.get()),
      optional(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("b")
      .bucket("i", 2)
      .identity("l")
      .identity("f")
      .identity("d")
      .identity("date")
      .hour("ts")
      .identity("ts")
      .truncate("s", 2)
      .identity("bytes")
      .bucket("dec_9_0", 2)
      .bucket("dec_11_2", 2)
      .bucket("dec_38_10", 2)
      .build();

  private static SparkSession spark;
  private static JavaSparkContext sparkContext = null;

  @BeforeClass
  public static void startSpark() {
    TestSparkDataFile.spark = SparkSession.builder().master("local[2]").getOrCreate();
    TestSparkDataFile.sparkContext = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDataFile.spark;
    TestSparkDataFile.spark = null;
    TestSparkDataFile.sparkContext = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testValueConversion() throws IOException {
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);
    checkSparkDataFile(table);
  }

  @Test
  public void testValueConversionPartitionedTable() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
    checkSparkDataFile(table);
  }

  @Test
  public void testValueConversionWithEmptyStats() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);
    checkSparkDataFile(table);
  }

  private void checkSparkDataFile(Table table) throws IOException {
    Iterable<InternalRow> rows = RandomData.generateSpark(table.schema(), 200, 0);
    JavaRDD<InternalRow> rdd = sparkContext.parallelize(Lists.newArrayList(rows));
    Dataset<Row> df = spark.internalCreateDataFrame(JavaRDD.toRDD(rdd), SparkSchemaUtil.convert(table.schema()), false);

    df.write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 manifest", 1, manifests.size());

    List<DataFile> dataFiles = Lists.newArrayList();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io(),
        table.location(), MetadataPathUtils.shouldUseRelativePath(table.properties()))) {
      reader.forEach(dataFile -> dataFiles.add(dataFile.copy()));
    }

    Dataset<Row> dataFileDF = spark.read().format("iceberg").load(tableLocation + "#files");

    // reorder columns to test arbitrary projections
    List<Column> columns = Arrays.stream(dataFileDF.columns())
        .map(ColumnName::new)
        .collect(Collectors.toList());
    Collections.shuffle(columns);

    List<Row> sparkDataFiles = dataFileDF
        .select(Iterables.toArray(columns, Column.class))
        .collectAsList();

    Assert.assertEquals("The number of files should match", dataFiles.size(), sparkDataFiles.size());

    Types.StructType dataFileType = DataFile.getType(table.spec().partitionType());
    StructType sparkDataFileType = sparkDataFiles.get(0).schema();
    SparkDataFile wrapper = new SparkDataFile(dataFileType, sparkDataFileType);

    for (int i = 0; i < dataFiles.size(); i++) {
      checkDataFile(dataFiles.get(i), wrapper.wrap(sparkDataFiles.get(i)));
    }
  }

  private void checkDataFile(DataFile expected, DataFile actual) {
    Assert.assertEquals("Path must match", expected.path(), actual.path());
    Assert.assertEquals("Format must match", expected.format(), actual.format());
    Assert.assertEquals("Record count must match", expected.recordCount(), actual.recordCount());
    Assert.assertEquals("Size must match", expected.fileSizeInBytes(), actual.fileSizeInBytes());
    Assert.assertEquals("Record value counts must match", expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals("Record null value counts must match", expected.nullValueCounts(), actual.nullValueCounts());
    Assert.assertEquals("Record nan value counts must match", expected.nanValueCounts(), actual.nanValueCounts());
    Assert.assertEquals("Lower bounds must match", expected.lowerBounds(), actual.lowerBounds());
    Assert.assertEquals("Upper bounds must match", expected.upperBounds(), actual.upperBounds());
    Assert.assertEquals("Key metadata must match", expected.keyMetadata(), actual.keyMetadata());
    Assert.assertEquals("Split offsets must match", expected.splitOffsets(), actual.splitOffsets());
    Assert.assertEquals("Sort order id must match", expected.sortOrderId(), actual.sortOrderId());

    checkStructLike(expected.partition(), actual.partition());
  }

  private void checkStructLike(StructLike expected, StructLike actual) {
    Assert.assertEquals("Struct size should match", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Struct values must match", expected.get(i, Object.class), actual.get(i, Object.class));
    }
  }
}
