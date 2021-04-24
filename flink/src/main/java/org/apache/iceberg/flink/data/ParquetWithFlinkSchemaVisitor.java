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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.parquet.ParquetTypeWithPartnerVisitor;

public class ParquetWithFlinkSchemaVisitor<T> extends ParquetTypeWithPartnerVisitor<LogicalType, T> {

  @Override
  protected LogicalType arrayElementType(LogicalType arrayType) {
    if (arrayType == null) {
      return null;
    }

    return ((ArrayType) arrayType).getElementType();
  }

  @Override
  protected LogicalType mapKeyType(LogicalType mapType) {
    if (mapType == null) {
      return null;
    }

    return ((MapType) mapType).getKeyType();
  }

  @Override
  protected LogicalType mapValueType(LogicalType mapType) {
    if (mapType == null) {
      return null;
    }

    return ((MapType) mapType).getValueType();
  }

  @Override
  protected LogicalType fieldType(LogicalType structType, int pos, Integer fieldId) {
    if (structType == null || ((RowType) structType).getFieldCount() < pos + 1) {
      return null;
    }

    return ((RowType) structType).getTypeAt(pos);
  }

}
