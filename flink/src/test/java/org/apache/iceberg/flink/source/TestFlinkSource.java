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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public abstract class TestFlinkSource extends TestFlinkScan {

  TestFlinkSource(String fileFormat) {
    super(fileFormat);
  }

  @Override
  protected List<Row> runWithProjection(String... projected) throws Exception {
    TableSchema.Builder builder = TableSchema.builder();
    Schema icebergSchema = catalog.loadTable(TableIdentifier.of("default", "t")).schema();
    TableSchema schema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema), icebergSchema.asStruct());
    for (String field : projected) {
      TableColumn column = schema.getTableColumn(field).get();
      builder.field(column.getName(), column.getType());
    }
    return run(FlinkSource.forRowData().project(builder.build()), Maps.newHashMap(), "", projected);
  }

  @Override
  protected List<Row> runWithFilter(Expression filter, String sqlFilter) throws Exception {
    FlinkSource.Builder builder = FlinkSource.forRowData().filters(Collections.singletonList(filter));
    return run(builder, Maps.newHashMap(), sqlFilter, "*");
  }

  @Override
  protected List<Row> runWithOptions(Map<String, String> options) throws Exception {
    FlinkSource.Builder builder = FlinkSource.forRowData();
    Optional.ofNullable(options.get("snapshot-id")).ifPresent(value -> builder.snapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("start-snapshot-id"))
        .ifPresent(value -> builder.startSnapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("end-snapshot-id"))
        .ifPresent(value -> builder.endSnapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("as-of-timestamp"))
        .ifPresent(value -> builder.asOfTimestamp(Long.parseLong(value)));
    return run(builder, options, "", "*");
  }

  @Override
  protected List<Row> run() throws Exception {
    return run(FlinkSource.forRowData(), Maps.newHashMap(), "", "*");
  }

  protected abstract List<Row> run(FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter,
                                   String... sqlSelectedFields) throws Exception;
}
