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

package org.apache.iceberg.flink.data;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class RowDataToRowMapper extends RichMapFunction<RowData, Row> {

  private final RowType rowType;

  private transient DataStructureConverter<Object, Object> converter;

  public RowDataToRowMapper(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));
  }

  @Override
  public Row map(RowData value) throws Exception {
    return (Row) converter.toExternal(value);
  }
}
