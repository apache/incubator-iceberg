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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Test {@link FlinkInputFormat}.
 */
public class TestFlinkInputFormat extends TestFlinkScan {

  public TestFlinkInputFormat(String fileFormat) {
    super(fileFormat);
  }

  @Override
  public void before() throws IOException {
    super.before();
  }

  private TableLoader loader() {
    return TableLoader.fromHadoopTable(warehouse + "/default/t");
  }

  @Override
  protected List<Row> run(
      FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter, String... sqlSelectedFields)
      throws IOException {
    return runFormat(formatBuilder.tableLoader(loader()).buildFormat());
  }

  @Test
  public void testNestedProjection() throws Exception {
    Schema schema = new Schema(
        required(1, "data", Types.StringType.get()),
        required(2, "nested", Types.StructType.of(
            Types.NestedField.required(3, "f1", Types.StringType.get()),
            Types.NestedField.required(4, "f2", Types.StringType.get()),
            Types.NestedField.required(5, "f3", Types.LongType.get()))),
        required(6, "id", Types.LongType.get()));

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), schema);

    List<Record> writeRecords = RandomGenericData.generate(schema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    // Schema: [data, nested[f1, f2, f3], id]
    // Projection: [nested.f2, data]
    // The Flink SQL output: [f2, data]
    // The FlinkInputFormat output: [nested[f2], data]

    TableSchema projectedSchema = TableSchema.builder()
        .field("nested", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING())))
        .field("data", DataTypes.STRING()).build();
    List<Row> result = runFormat(FlinkSource.forRowData().tableLoader(loader()).project(projectedSchema).buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : writeRecords) {
      Row nested = Row.of(((Record) record.get(1)).get(1));
      expected.add(Row.of(nested, record.get(0)));
    }

    assertRows(result, expected);
  }

  private List<Row> runFormat(FlinkInputFormat inputFormat) throws IOException {
    FlinkInputSplit[] splits = inputFormat.createInputSplits(0);
    List<Row> results = Lists.newArrayList();

    RowType rowType = FlinkSchemaUtil.convert(inputFormat.projectedSchema());

    DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));

    for (FlinkInputSplit s : splits) {
      inputFormat.open(s);
      while (!inputFormat.reachedEnd()) {
        RowData row = inputFormat.nextRecord(null);
        results.add((Row) converter.toExternal(row));
      }
    }
    inputFormat.close();
    return results;
  }
}
