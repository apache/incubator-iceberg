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

package org.apache.iceberg.spark.data;

import org.apache.iceberg.parquet.ParquetTypeWithPartnerVisitor;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

/**
 * Visitor for traversing a Parquet type with a companion Spark type.
 *
 * @param <T> the Java class returned by the visitor
 */
public class ParquetWithSparkSchemaVisitor<T> extends ParquetTypeWithPartnerVisitor<DataType, T> {

  @Override
  protected DataType arrayElementType(DataType arrayType) {
    if (arrayType == null) {
      return null;
    }

    return ((ArrayType) arrayType).elementType();
  }

  @Override
  protected DataType mapKeyType(DataType mapType) {
    if (mapType == null) {
      return null;
    }

    return ((MapType) mapType).keyType();
  }

  @Override
  protected DataType mapValueType(DataType mapType) {
    if (mapType == null) {
      return null;
    }

    return ((MapType) mapType).valueType();
  }

  @Override
  protected DataType fieldType(DataType structType, int pos, Integer fieldId) {
    if (structType == null || ((StructType) structType).size() <  pos + 1) {
      return null;
    }

    return ((StructType) structType).apply(pos).dataType();
  }
}
