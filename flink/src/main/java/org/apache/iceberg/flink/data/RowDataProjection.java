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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class RowDataProjection {
  private final RowData.FieldGetter[] getters;

  public static RowDataProjection create(Schema schema, Schema projectSchema) {
    return new RowDataProjection(FlinkSchemaUtil.convert(schema), schema.asStruct(), projectSchema.asStruct());
  }

  private RowDataProjection(RowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
    this.getters = new RowData.FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      getters[i] = createFieldGetter(rowType, rowStruct, projectType.fields().get(i));
    }
  }

  private static RowData.FieldGetter createFieldGetter(RowType rowType,
                                                       Types.StructType rowStruct,
                                                       Types.NestedField projectField) {
    for (int i = 0; i < rowStruct.fields().size(); i++) {
      Types.NestedField rowField = rowStruct.fields().get(i);
      if (rowField.fieldId() == projectField.fieldId()) {
        Preconditions.checkArgument(rowField.type().typeId() == projectField.type().typeId(),
            String.format("Different iceberg type between row field <%s> and project field <%s>",
                rowField, projectField));

        switch (projectField.type().typeId()) {
          case STRUCT:
            RowType nestedRowType = (RowType) rowType.getTypeAt(i);
            int rowPos = i;
            return row -> {
              RowData nestedRow = row.isNullAt(rowPos) ? null : row.getRow(rowPos, nestedRowType.getFieldCount());
              return new RowDataProjection(nestedRowType, rowField.type().asStructType(),
                  projectField.type().asStructType()).project(nestedRow);
            };

          case MAP:
          case LIST:
            throw new IllegalArgumentException(String.format("Cannot project list or map field: %s", projectField));
          default:
            return RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }
      }
    }
    throw new IllegalArgumentException(String.format("Cannot find field %s in %s", projectField, rowStruct));
  }

  public RowData project(RowData row) {
    GenericRowData projectedRow = new GenericRowData(getters.length);
    if (row != null) {
      projectedRow.setRowKind(row.getRowKind());
      for (int i = 0; i < getters.length; i++) {
        projectedRow.setField(i, getters[i].getFieldOrNull(row));
      }
    }
    return projectedRow;
  }
}
