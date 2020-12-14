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

package org.apache.iceberg.flink.source;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.RowDataIteratorReaderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergSourceBounded extends TestFlinkScan {

  public TestIcebergSourceBounded(String fileFormat) {
    super(fileFormat);
  }

  @Override
  protected List<Row> runWithProjection(String... projected) throws Exception {
    Schema icebergTableSchema = catalog.loadTable(TestFixtures.TABLE_IDENTIFIER).schema();
    TableSchema.Builder builder = TableSchema.builder();
    TableSchema schema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergTableSchema));
    for (String field : projected) {
      TableColumn column = schema.getTableColumn(field).get();
      builder.field(column.getName(), column.getType());
    }
    TableSchema flinkSchema = builder.build();
    Schema projectedSchema = FlinkSchemaUtil.convert(icebergTableSchema, flinkSchema);
    ScanContext scanContext = ScanContext.builder()
        .project(projectedSchema)
        .build();
    return run(scanContext);
  }

  @Override
  protected List<Row> runWithFilter(Expression filter, String sqlFilter) throws Exception {
    Schema tableSchema = catalog.loadTable(TestFixtures.TABLE_IDENTIFIER).schema();
    ScanContext scanContext = ScanContext.builder()
        .project(tableSchema)
        .filters(Arrays.asList(filter))
        .build();
    return run(scanContext);
  }

  @Override
  protected List<Row> runWithOptions(Map<String, String> options) throws Exception {
    Schema tableSchema = catalog.loadTable(TestFixtures.TABLE_IDENTIFIER).schema();
    ScanContext scanContext = ScanContext.builder()
        .project(tableSchema)
        .fromProperties(options)
        .build();
    return run(scanContext);
  }

  @Override
  protected List<Row> run() throws Exception {
    Schema tableSchema = catalog.loadTable(TestFixtures.TABLE_IDENTIFIER).schema();
    ScanContext scanContext = ScanContext.builder()
        .project(tableSchema)
        .build();
    return run(scanContext);
  }

  private List<Row> run(ScanContext scanContext) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_SIZE, 128);
    final Table table;
    try (TableLoader tableLoader = tableLoader()) {
      tableLoader.open();
      table = tableLoader.loadTable();
    }
    final RowType rowType = FlinkSchemaUtil.convert(scanContext.project());

    final DataStream<Row> stream = env.fromSource(
        IcebergSource.<RowData>builder()
            .tableLoader(tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .readerFactory(new RowDataIteratorReaderFactory(config, table, scanContext, rowType))
            .scanContext(scanContext)
            .build(),
        WatermarkStrategy.noWatermarks(),
        "testBasicRead",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

    try (CloseableIterator<Row> iter = stream.executeAndCollect()) {
      return Lists.newArrayList(iter);
    }
  }

}
