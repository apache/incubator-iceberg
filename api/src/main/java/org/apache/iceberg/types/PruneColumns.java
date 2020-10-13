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

package org.apache.iceberg.types;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class PruneColumns extends TypeUtil.SchemaVisitor<Type> {
  private final Set<Integer> selected;

  PruneColumns(Set<Integer> selected) {
    Preconditions.checkNotNull(selected, "Selected field ids cannot be null");
    this.selected = selected;
  }

  @Override
  public Type schema(Schema schema, Type structResult) {
    return structResult;
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    List<Types.NestedField> fields = struct.fields();
    List<Types.NestedField> selectedFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean sameTypes = true;

    for (int i = 0; i < fieldResults.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      Type projectedType = fieldResults.get(i);
      if (field.type() == projectedType) {
        // uses identity because there is no need to check structure. if identity doesn't match
        // then structure should not either.
        selectedFields.add(field);
      } else if (projectedType != null) {
        sameTypes = false; // signal that some types were altered
        if (field.isOptional()) {
          selectedFields.add(
              Types.NestedField.optional(field.fieldId(), field.name(), projectedType, field.doc()));
        } else {
          selectedFields.add(
              Types.NestedField.required(field.fieldId(), field.name(), projectedType, field.doc()));
        }
      }
    }

    if (!selectedFields.isEmpty()) {
      if (selectedFields.size() == fields.size() && sameTypes) {
        return struct;
      } else {
        return Types.StructType.of(selectedFields);
      }
    }

    return null;
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    if (selected.contains(field.fieldId())) {
      return field.type();
    } else if (fieldResult != null) {
      // this isn't necessarily the same as field.type() because a struct may not have all
      // fields selected.
      return fieldResult;
    }
    return null;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    if (selected.contains(list.elementId())) {
      return list;
    } else if (elementResult != null) {
      if (list.elementType() == elementResult) {
        return list;
      } else if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementResult);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementResult);
      }
    }
    return null;
  }

  @Override
  public Type map(Types.MapType map, Type ignored, Type valueResult) {
    if (selected.contains(map.valueId())) {
      return map;
    } else if (valueResult != null) {
      if (map.valueType() == valueResult) {
        return map;
      } else if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueResult);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueResult);
      }
    } else if (selected.contains(map.keyId())) {
      // right now, maps can't be selected without values
      return map;
    }
    return null;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return null;
  }
}
