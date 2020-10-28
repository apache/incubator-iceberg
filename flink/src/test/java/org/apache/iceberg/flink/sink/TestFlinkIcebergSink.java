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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkTableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends AbstractTestBase {
  private static final TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
      SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
  private static final DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());
  private static final int ROW_COUNTS = 1000;
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;

  @Parameterized.Parameters(name = "format={0}, parallelism = {1}, partitioned = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro", 1, true},
        {"avro", 1, false},
        {"avro", 2, true},
        {"avro", 2, false},
        {"orc", 1, true},
        {"orc", 1, false},
        {"orc", 2, true},
        {"orc", 2, false},
        {"parquet", 1, true},
        {"parquet", 1, false},
        {"parquet", 2, true},
        {"parquet", 2, false}
    };
  }

  public TestFlinkIcebergSink(String format, int parallelism, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    String warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);

    env = StreamExecutionEnvironment.getExecutionEnvironment()
        .enableCheckpointing(100)
        .setParallelism(parallelism)
        .setMaxParallelism(parallelism);

    tableLoader = TableLoader.fromHadoopTable(tablePath);
  }

  private List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  @Test
  public void testWriteRowDataWithRewrite() throws Exception {
    List<Row> rows = SimpleDataUtil.generateRows(ROW_COUNTS);

    DataStream<RowData> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));
    File folder = tempFolder.newFolder();
    String warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);

    table.updateProperties()
        .set(FlinkTableProperties.NUMS_NEW_SNAPSHOT_MERGE, String.valueOf(5))
        .set(FlinkTableProperties.SNAPSHOT_RETAIN_LAST_MINUTES, String.valueOf(1))
        .set(FlinkTableProperties.SNAPSHOT_RETAIN_LAST_NUMS, String.valueOf(1))
        .commit();

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .build();

    env.execute("Test Iceberg Write with Rewrite");

    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows),
        convertToRowData(rows)));
    SimpleDataUtil.assertTableRows(tablePath, expectedRows);

    table.updateProperties()
        .remove(FlinkTableProperties.NUMS_NEW_SNAPSHOT_MERGE)
        .remove(FlinkTableProperties.SNAPSHOT_RETAIN_LAST_MINUTES)
        .remove(FlinkTableProperties.SNAPSHOT_RETAIN_LAST_NUMS)
        .commit();
  }

  @Test
  public void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(
        Row.of(1, "hello"),
        Row.of(2, "world"),
        Row.of(3, "foo")
    );
    DataStream<RowData> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .build();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records. NOTICE: the FiniteTestSource will checkpoint the same rows twice, so it will
    // commit the same row list into iceberg twice.
    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows), convertToRowData(rows)));
    SimpleDataUtil.assertTableRows(tablePath, expectedRows);
  }

  private void testWriteRow(TableSchema tableSchema) throws Exception {
    List<Row> rows = Lists.newArrayList(
        Row.of(4, "bar"),
        Row.of(5, "apache")
    );
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .writeParallelism(parallelism)
        .build();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows), convertToRowData(rows)));
    SimpleDataUtil.assertTableRows(tablePath, expectedRows);
  }

  @Test
  public void testWriteRow() throws Exception {
    testWriteRow(null);
  }

  @Test
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA);
  }
}
