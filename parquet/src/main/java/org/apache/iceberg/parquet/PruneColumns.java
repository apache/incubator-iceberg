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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

class PruneColumns extends ParquetTypeVisitor<Type> {
  private final Set<Integer> selectedIds;

  PruneColumns(Set<Integer> selectedIds) {
    Preconditions.checkNotNull(selectedIds, "Selected field ids cannot be null");
    this.selectedIds = selectedIds;
  }

  @Override
  public Type message(MessageType message, List<Type> fields) {
    Types.MessageTypeBuilder builder = Types.buildMessage();

    boolean hasChange = false;
    int fieldCount = 0;
    for (int i = 0; i < fields.size(); i += 1) {
      Type originalField = message.getType(i);
      Type field = fields.get(i);
      Integer fieldId = getId(originalField);
      if (fieldId != null && selectedIds.contains(fieldId)) {
        builder.addField(originalField);
        fieldCount += 1;
      } else if (field != null) {
        builder.addField(field);
        fieldCount += 1;
        hasChange = true;
      }
    }

    if (hasChange) {
      return builder.named(message.getName());
    } else if (message.getFieldCount() == fieldCount) {
      return message;
    }

    return builder.named(message.getName());
  }

  @Override
  public Type struct(GroupType struct, List<Type> fields) {
    boolean hasChange = false;
    List<Type> filteredFields = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i += 1) {
      Type originalField = struct.getType(i);
      Type field = fields.get(i);
      Integer fieldId = getId(originalField);
      if (fieldId != null && selectedIds.contains(fieldId)) {
        filteredFields.add(originalField);
      } else if (field != null) {
        filteredFields.add(originalField);
        hasChange = true;
      }
    }

    if (hasChange) {
      return struct.withNewFields(filteredFields);
    } else if (struct.getFieldCount() == filteredFields.size()) {
      return struct;
    } else if (!filteredFields.isEmpty()) {
      return struct.withNewFields(filteredFields);
    }

    return null;
  }

  @Override
  public Type list(GroupType list, Type element) {
    GroupType repeated = list.getType(0).asGroupType();
    Type originalElement = repeated.getType(0);
    Integer elementId = getId(originalElement);

    if (elementId != null && selectedIds.contains(elementId)) {
      return list;
    } else if (element != null) {
      if (!Objects.equal(element, originalElement)) {
        Integer listId = getId(list);
        // the element type was projected
        Type listType = Types.list(list.getRepetition())
            .element(element)
            .named(list.getName());
        return listId == null ? listType : listType.withId(listId);
      }
      return list;
    }

    return null;
  }

  @Override
  public Type map(GroupType map, Type key, Type value) {
    GroupType repeated = map.getType(0).asGroupType();
    Type originalKey = repeated.getType(0);
    Type originalValue = repeated.getType(1);

    Integer keyId = getId(originalKey);
    Integer valueId = getId(originalValue);

    if (keyId != null && selectedIds.contains(keyId) || valueId != null && selectedIds.contains(valueId)) {
      return map;
    } else if (value != null) {
      Integer mapId = getId(map);
      if (!Objects.equal(value, originalValue)) {
        Type mapType = Types.map(map.getRepetition())
            .key(originalKey)
            .value(value)
            .named(map.getName());

        return mapId == null ? mapType : mapType.withId(mapId);
      }
      return map;
    }

    return null;
  }

  @Override
  public Type primitive(PrimitiveType primitive) {
    return null;
  }

  private Integer getId(Type type) {
    return type.getId() == null ? null : type.getId().intValue();
  }
}
