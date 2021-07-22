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

package org.apache.iceberg.avro;

import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public abstract class AvroSchemaWithTypeVisitor<T> {
  public static <T> T visit(org.apache.iceberg.Schema iSchema, Schema schema, AvroSchemaWithTypeVisitor<T> visitor) {
    return visit(iSchema.asStruct(), schema, visitor);
  }

  public static <T> T visit(Type iType, Schema schema, AvroSchemaWithTypeVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        return visitRecord(iType != null ? iType.asStructType() : null, schema, visitor);

      case UNION:
        return visitUnion(iType, schema, visitor);

      case ARRAY:
        return visitArray(iType, schema, visitor);

      case MAP:
        Types.MapType map = iType != null ? iType.asMapType() : null;
        return visitor.map(map, schema,
            visit(map != null ? map.valueType() : null, schema.getValueType(), visitor));

      default:
        return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, schema);
    }
  }

  private static <T> T visitRecord(Types.StructType struct, Schema record, AvroSchemaWithTypeVisitor<T> visitor) {
    // check to make sure this hasn't been visited before
    String name = record.getFullName();
    Preconditions.checkState(!visitor.recordLevels.contains(name),
        "Cannot process recursive Avro record %s", name);

    visitor.recordLevels.push(name);

    List<Schema.Field> fields = record.getFields();
    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (Schema.Field field : fields) {
      int fieldId = AvroSchemaUtil.getFieldId(field);
      Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
      names.add(field.name());
      results.add(visit(iField != null ? iField.type() : null, field.schema(), visitor));
    }

    visitor.recordLevels.pop();

    return visitor.record(struct, record, names, results);
  }

  private static <T> T visitUnion(Type type, Schema union, AvroSchemaWithTypeVisitor<T> visitor) {
    List<Schema> types = union.getTypes();
    List<T> options = Lists.newArrayListWithExpectedSize(types.size());
    for (Schema branch : types) {
      if (branch.getType() == Schema.Type.NULL) {
        options.add(visit((Type) null, branch, visitor));
      } else {
        options.add(visit(type, branch, visitor));
      }
    }
    return visitor.union(type, union, options);
  }

  private static <T> T visitArray(Type type, Schema array, AvroSchemaWithTypeVisitor<T> visitor) {
    if (array.getLogicalType() instanceof LogicalMap || type != null && type.isMapType()) {
      Preconditions.checkState(
          AvroSchemaUtil.isKeyValueSchema(array.getElementType()),
          "Cannot visit invalid logical map type: %s", array);
      Types.MapType map = type != null ? type.asMapType() : null;
      List<Schema.Field> keyValueFields = array.getElementType().getFields();
      return visitor.map(map, array,
          visit(map != null ? map.keyType() : null, keyValueFields.get(0).schema(), visitor),
          visit(map != null ? map.valueType() : null, keyValueFields.get(1).schema(), visitor));

    } else {
      Types.ListType list = type != null ? type.asListType() : null;
      return visitor.array(list, array,
          visit(list != null ? list.elementType() : null, array.getElementType(), visitor));
    }
  }

  private Deque<String> recordLevels = Lists.newLinkedList();

  public T record(Types.StructType iStruct, Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(Type iType, Schema union, List<T> options) {
    return null;
  }

  public T array(Types.ListType iList, Schema array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, Schema map, T key, T value) {
    return null;
  }

  public T map(Types.MapType iMap, Schema map, T value) {
    return null;
  }

  public T primitive(Type.PrimitiveType iPrimitive, Schema primitive) {
    return null;
  }
}
