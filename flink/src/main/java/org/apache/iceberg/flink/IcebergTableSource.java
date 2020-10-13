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

package org.apache.iceberg.flink;

import java.util.Arrays;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * Flink Iceberg table source.
 * TODO: Implement {@link FilterableTableSource} and {@link LimitableTableSource}.
 */
public class IcebergTableSource implements StreamTableSource<RowData>, ProjectableTableSource<RowData> {

  private final TableLoader loader;
  private final Configuration hadoopConf;
  private final TableSchema schema;
  private final Map<String, String> options;
  private final int[] projectedFields;

  public IcebergTableSource(TableLoader loader, Configuration hadoopConf, TableSchema schema,
                            Map<String, String> options) {
    this(loader, hadoopConf, schema, options, null);
  }

  private IcebergTableSource(TableLoader loader, Configuration hadoopConf, TableSchema schema,
                             Map<String, String> options, int[] projectedFields) {
    this.loader = loader;
    this.hadoopConf = hadoopConf;
    this.schema = schema;
    this.options = options;
    this.projectedFields = projectedFields;
  }

  @Override
  public boolean isBounded() {
    return FlinkSource.isBounded(options);
  }

  @Override
  public TableSource<RowData> projectFields(int[] fields) {
    return new IcebergTableSource(loader, hadoopConf, schema, options, fields);
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData().env(execEnv).tableLoader(loader).hadoopConf(hadoopConf)
        .project(getProjectedSchema()).properties(options).build();
  }

  @Override
  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public DataType getProducedDataType() {
    return getProjectedSchema().toRowDataType().bridgedTo(RowData.class);
  }

  private TableSchema getProjectedSchema() {
    TableSchema fullSchema = getTableSchema();
    if (projectedFields == null) {
      return fullSchema;
    } else {
      String[] fullNames = fullSchema.getFieldNames();
      DataType[] fullTypes = fullSchema.getFieldDataTypes();
      return TableSchema.builder().fields(
          Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
          Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new)).build();
    }
  }

  @Override
  public String explainSource() {
    String explain = "Iceberg table: " + loader.toString();
    if (projectedFields != null) {
      explain += ", ProjectedFields: " + Arrays.toString(projectedFields);
    }
    return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames()) + explain;
  }
}
