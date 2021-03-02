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

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.TestAppenderFactory;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;

public class TestFlinkAppenderFactory extends TestAppenderFactory<RowData> {

  private final RowType rowType;

  public TestFlinkAppenderFactory(String fileFormat, boolean partitioned) {
    super(fileFormat, partitioned);
    this.rowType = FlinkSchemaUtil.convert(SCHEMA);
  }

  @Override
  protected FileAppenderFactory<RowData> createAppenderFactory(List<Integer> equalityFieldIds,
                                                               Schema eqDeleteSchema,
                                                               Schema posDeleteRowSchema) {
    return new FlinkAppenderFactory(table.schema(), rowType, table.properties(), table.spec(), table.sortOrder(),
        ArrayUtil.toIntArray(equalityFieldIds), eqDeleteSchema, posDeleteRowSchema);
  }

  @Override
  protected RowData createRow(Integer id, String data) {
    return SimpleDataUtil.createRowData(id, data);
  }

  @Override
  protected StructLikeSet expectedRowSet(Iterable<RowData> rows) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    for (RowData row : rows) {
      RowDataWrapper wrapper = new RowDataWrapper(rowType, table.schema().asStruct());
      set.add(wrapper.wrap(row));
    }
    return set;
  }
}
