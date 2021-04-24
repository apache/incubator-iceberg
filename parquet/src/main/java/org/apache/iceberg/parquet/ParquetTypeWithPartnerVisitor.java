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

import java.util.Deque;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public abstract class ParquetTypeWithPartnerVisitor<P, T> {
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public static <P, T> T visit(P partnerType, Type type, ParquetTypeWithPartnerVisitor<P, T> visitor) {
    if (type instanceof MessageType) {
      return visitor.message(partnerType, (MessageType) type, visitFields(partnerType, type.asGroupType(), visitor));
    } else if (type.isPrimitive()) {
      return visitor.primitive(partnerType, type.asPrimitiveType());
    } else {
      // if not a primitive, the typeId must be a group
      GroupType group = type.asGroupType();
      OriginalType annotation = group.getOriginalType();
      if (annotation != null) {
        switch (annotation) {
          case LIST:
            return visitList(partnerType, group, visitor);
          case MAP:
            return visitMap(partnerType, group, visitor);
          default:
        }
      }
      return visitor.struct(partnerType, group, visitFields(partnerType, group, visitor));
    }
  }

  private static <P, T> T visitList(P list, GroupType group, ParquetTypeWithPartnerVisitor<P, T> visitor) {
    Preconditions.checkArgument(!group.isRepetition(Type.Repetition.REPEATED),
        "Invalid list: top-level group is repeated: %s", group);
    Preconditions.checkArgument(group.getFieldCount() == 1,
        "Invalid list: does not contain single repeated field: %s", group);

    GroupType repeatedElement = group.getFields().get(0).asGroupType();
    Preconditions.checkArgument(repeatedElement.isRepetition(Type.Repetition.REPEATED),
        "Invalid list: inner group is not repeated");
    Preconditions.checkArgument(repeatedElement.getFieldCount() <= 1,
        "Invalid list: repeated group is not a single field: %s", group);

    visitor.beforeRepeatedElement(repeatedElement);
    try {
      T elementResult = null;
      if (repeatedElement.getFieldCount() > 0) {
        Type elementField = repeatedElement.getType(0);
        visitor.beforeElementField(elementField);
        try {
          elementResult = visit(visitor.arrayElementType(list), elementField, visitor);
        } finally {
          visitor.afterElementField(elementField);
        }
      }
      return visitor.list(list, group, elementResult);
    } finally {
      visitor.afterRepeatedElement(repeatedElement);
    }
  }

  private static <P, T> T visitMap(P map, GroupType group, ParquetTypeWithPartnerVisitor<P, T> visitor) {
    Preconditions.checkArgument(!group.isRepetition(Type.Repetition.REPEATED),
        "Invalid map: top-level group is repeated: %s", group);
    Preconditions.checkArgument(group.getFieldCount() == 1,
        "Invalid map: does not contain single repeated field: %s", group);

    GroupType repeatedKeyValue = group.getType(0).asGroupType();
    Preconditions.checkArgument(repeatedKeyValue.isRepetition(Type.Repetition.REPEATED),
        "Invalid map: inner group is not repeated");
    Preconditions.checkArgument(repeatedKeyValue.getFieldCount() <= 2,
        "Invalid map: repeated group does not have 2 fields");

    visitor.beforeRepeatedKeyValue(repeatedKeyValue);
    try {
      T keyResult = null;
      T valueResult = null;
      switch (repeatedKeyValue.getFieldCount()) {
        case 2:
          // if there are 2 fields, both key and value are projected
          Type keyType = repeatedKeyValue.getType(0);
          visitor.beforeKeyField(keyType);
          try {
            keyResult = visit(visitor.mapKeyType(map), keyType, visitor);
          } finally {
            visitor.afterKeyField(keyType);
          }
          Type valueType = repeatedKeyValue.getType(1);
          visitor.beforeValueField(valueType);
          try {
            valueResult = visit(visitor.mapValueType(map), repeatedKeyValue.getType(1), visitor);
          } finally {
            visitor.afterValueField(valueType);
          }
          break;
        case 1:
          // if there is just one, use the name to determine what it is
          Type keyOrValue = repeatedKeyValue.getType(0);
          if (keyOrValue.getName().equalsIgnoreCase("key")) {
            visitor.beforeKeyField(keyOrValue);
            try {
              keyResult = visit(visitor.mapKeyType(map), keyOrValue, visitor);
            } finally {
              visitor.afterKeyField(keyOrValue);
            }
            // value result remains null
          } else {
            visitor.beforeValueField(keyOrValue);
            try {
              valueResult = visit(visitor.mapValueType(map), keyOrValue, visitor);
            } finally {
              visitor.afterValueField(keyOrValue);
            }
            // key result remains null
          }
          break;
        default:
          // both results will remain null
      }

      return visitor.map(map, group, keyResult, valueResult);

    } finally {
      visitor.afterRepeatedKeyValue(repeatedKeyValue);
    }
  }

  protected static <P, T> List<T> visitFields(P struct, GroupType group, ParquetTypeWithPartnerVisitor<P, T> visitor) {
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (int i = 0; i < group.getFieldCount(); i += 1) {
      Type field = group.getFields().get(i);
      visitor.beforeField(field);
      try {
        Integer fieldId = field.getId() == null ? null : field.getId().intValue();
        results.add(visit(visitor.fieldType(struct, i, fieldId), field, visitor));
      } finally {
        visitor.afterField(field);
      }
    }

    return results;
  }

  protected abstract P arrayElementType(P arrayType);
  protected abstract P mapKeyType(P mapType);
  protected abstract P mapValueType(P mapType);
  protected abstract P fieldType(P structType, int pos, Integer parquetFieldId);

  public T message(P struct, MessageType message, List<T> fields) {
    return null;
  }

  public T struct(P pStruct, GroupType struct, List<T> fields) {
    return null;
  }

  public T list(P pArray, GroupType array, T element) {
    return null;
  }

  public T map(P pMap, GroupType map, T key, T value) {
    return null;
  }

  public T primitive(P pType, PrimitiveType primitive) {
    return null;
  }


  public void beforeField(Type type) {
    fieldNames.push(type.getName());
  }

  public void afterField(Type type) {
    fieldNames.pop();
  }

  public void beforeRepeatedElement(Type element) {
    beforeField(element);
  }

  public void afterRepeatedElement(Type element) {
    afterField(element);
  }

  public void beforeElementField(Type element) {
    beforeField(element);
  }

  public void afterElementField(Type element) {
    afterField(element);
  }

  public void beforeRepeatedKeyValue(Type keyValue) {
    beforeField(keyValue);
  }

  public void afterRepeatedKeyValue(Type keyValue) {
    afterField(keyValue);
  }

  public void beforeKeyField(Type key) {
    beforeField(key);
  }

  public void afterKeyField(Type key) {
    afterField(key);
  }

  public void beforeValueField(Type value) {
    beforeField(value);
  }

  public void afterValueField(Type value) {
    afterField(value);
  }

  protected String[] currentPath() {
    return Lists.newArrayList(fieldNames.descendingIterator()).toArray(new String[0]);
  }

  protected String[] path(String name) {
    List<String> list = Lists.newArrayList(fieldNames.descendingIterator());
    list.add(name);
    return list.toArray(new String[0]);
  }

}
