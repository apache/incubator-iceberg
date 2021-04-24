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

package org.apache.iceberg.parquet;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Visitor for traversing a Parquet type with a companion Iceberg type.
 *
 * @param <T> the Java class returned by the visitor
 */
public class TypeWithSchemaVisitor<T> extends ParquetTypeWithPartnerVisitor<Type, T> {

  @Override
  protected Type arrayElementType(Type arrayType) {
    if (arrayType == null) {
      return null;
    }

    return arrayType.asListType().elementType();
  }

  @Override
  protected Type mapKeyType(Type mapType) {
    if (mapType == null) {
      return null;
    }

    return mapType.asMapType().keyType();
  }

  @Override
  protected Type mapValueType(Type mapType) {
    if (mapType == null) {
      return null;
    }

    return mapType.asMapType().valueType();
  }

  @Override
  protected Type fieldType(Type structType, int pos, Integer fieldId) {
    if (structType == null || fieldId == null) {
      return null;
    }

    Types.NestedField field = structType.asStructType().field(fieldId);
    return field == null ? null : field.type();
  }
}
