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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PruneColumns extends AvroSchemaVisitor<Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(PruneColumns.class);

  private final Set<Integer> selectedIds;
  private final NameMapping nameMapping;

  PruneColumns(Set<Integer> selectedIds, NameMapping nameMapping) {
    Preconditions.checkNotNull(selectedIds, "Selected field ids cannot be null");
    this.selectedIds = selectedIds;
    this.nameMapping = nameMapping;
  }

  Schema rootSchema(Schema record) {
    Schema result = visit(record, this);
    if (result != null) {
      return result;
    }

    return copyRecord(record, ImmutableList.of());
  }

  @Override
  public Schema record(Schema record, List<String> names, List<Schema> fields) {
    // Then this should access the record's fields by name
    List<Schema.Field> filteredFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean hasChange = false;
    for (Schema.Field field : record.getFields()) {
      Integer fieldId = AvroSchemaUtil.getFieldId(field, nameMapping, fieldNames());
      if (fieldId == null) {
        // Both the schema and the nameMapping does not have field id. We prune this field.
        continue;
      }

      if (!AvroSchemaUtil.hasFieldId(field)) {
        // fieldId was resolved from nameMapping, we updated hasChange
        // flag to make sure a new field is created with the field id
        hasChange = true;
      }

      if (isOptionSchemaWithNonNullFirstOption(field.schema())) {
        // if the field has an optional schema where the first option is not NULL,
        // we update hasChange flag to make sure we reorder the schema and make the
        // NULL option as the first
        hasChange = true;
      }

      Schema fieldSchema = fields.get(field.pos());
      // All primitives are selected by selecting the field, but map and list
      // types can be selected by projecting the keys, values, or elements.
      // This creates two conditions where the field should be selected: if the
      // id is selected or if the result of the field is non-null. The only
      // case where the converted field is non-null is when a map or list is
      // selected by lower IDs.
      if (selectedIds.contains(fieldId)) {
        filteredFields.add(copyField(field, field.schema(), fieldId));
      } else if (fieldSchema != null) {
        hasChange = true;
        filteredFields.add(copyField(field, fieldSchema, fieldId));
      }
    }

    if (hasChange) {
      return copyRecord(record, filteredFields);
    } else if (record.getFields().size() != 0 && filteredFields.size() == record.getFields().size()) {
      return record;
    } else if (!filteredFields.isEmpty()) {
      return copyRecord(record, filteredFields);
    }

    return null;
  }

  @Override
  public Schema union(Schema union, List<Schema> options) {
    Preconditions.checkState(AvroSchemaUtil.isOptionSchema(union),
        "Invalid schema: non-option unions are not supported: %s", union);

    // only unions with null are allowed, and a null schema results in null
    Schema pruned = null;
    if (options.get(0) != null) {
      pruned = options.get(0);
    } else if (options.get(1) != null) {
      pruned = options.get(1);
    }

    if (pruned != null) {
      if (!Objects.equals(pruned, AvroSchemaUtil.fromOption(union))) {
        return AvroSchemaUtil.toOption(pruned);
      }
      return union;
    }

    return null;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Schema array(Schema array, Schema element) {
    if (array.getLogicalType() instanceof LogicalMap) {
      Schema keyValue = array.getElementType();
      Integer keyId = AvroSchemaUtil.getFieldId(keyValue.getField("key"), nameMapping, fieldNames());
      Integer valueId = AvroSchemaUtil.getFieldId(keyValue.getField("value"), nameMapping, fieldNames());
      if (keyId == null || valueId == null) {
        if (keyId != null || valueId != null) {
          LOG.warn("Map schema {} should have both key and value ids set or both unset", array);
        }
        return null;
      }

      // if either key or value is selected, the whole map must be projected
      if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
        return complexMapWithIds(array, keyId, valueId);
      } else if (element != null) {
        Schema.Field keyProjectionField = element.getField("key");
        Schema valueProjection = element.getField("value").schema();
        // it is possible that key is not selected, and
        // key schemas can be different if new field ids were assigned to them
        if (keyProjectionField != null &&
            !Objects.equals(keyValue.getField("key").schema(), keyProjectionField.schema())) {
          Preconditions.checkState(
              SchemaNormalization.parsingFingerprint64(keyValue.getField("key").schema()) ==
                  SchemaNormalization.parsingFingerprint64(keyProjectionField.schema()),
              "Map keys should not be projected");
          return AvroSchemaUtil.createMap(keyId, keyProjectionField.schema(), valueId, valueProjection);
        } else if (!Objects.equals(keyValue.getField("value").schema(), valueProjection)) {
          return AvroSchemaUtil.createMap(keyId, keyValue.getField("key").schema(), valueId, valueProjection);
        } else {
          return complexMapWithIds(array, keyId, valueId);
        }
      }

    } else {
      Integer elementId = AvroSchemaUtil.getElementId(array, nameMapping, fieldNames());
      if (elementId == null) {
        return null;
      }

      if (selectedIds.contains(elementId)) {
        return arrayWithId(array, elementId);
      } else if (element != null) {
        if (!Objects.equals(element, array.getElementType())) {
          // the element must be a projection
          return arrayWithId(Schema.createArray(element), elementId);
        }
        return arrayWithId(array, elementId);
      }
    }

    return null;
  }

  @Override
  public Schema map(Schema map, Schema value) {
    Integer keyId = AvroSchemaUtil.getKeyId(map, nameMapping, fieldNames());
    Integer valueId = AvroSchemaUtil.getValueId(map, nameMapping, fieldNames());
    if (keyId == null || valueId == null) {
      if (keyId != null || valueId != null) {
        LOG.warn("Map schema {} should have both key and value ids set or both unset", map);
      }
      return null;
    }

    // if either key or value is selected, the whole map must be projected
    if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
      // Assign ids. Ids may not always be present in the schema,
      // e.g if we are reading data not written by Iceberg writers
      return mapWithIds(map, keyId, valueId);
    } else if (value != null) {
      if (!Objects.equals(value, map.getValueType())) {
        // the value must be a projection
        return mapWithIds(Schema.createMap(value), keyId, valueId);
      }
      return map;
    }

    return null;
  }

  private Schema arrayWithId(Schema array, Integer elementId) {
    if (!AvroSchemaUtil.hasProperty(array, AvroSchemaUtil.ELEMENT_ID_PROP)) {
      Schema result = Schema.createArray(array.getElementType());
      result.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, elementId);
      return result;
    }
    return array;
  }

  private Schema complexMapWithIds(Schema map, Integer keyId, Integer valueId) {
    Schema keyValue = map.getElementType();
    if (!AvroSchemaUtil.hasFieldId(keyValue.getField("key")) ||
        !AvroSchemaUtil.hasFieldId(keyValue.getField("value"))) {
      return AvroSchemaUtil.createMap(
          keyId, keyValue.getField("key").schema(),
          valueId, keyValue.getField("value").schema());
    }
    return map;
  }

  private Schema mapWithIds(Schema map, Integer keyId, Integer valueId) {
    if (!AvroSchemaUtil.hasProperty(map, AvroSchemaUtil.KEY_ID_PROP) ||
        !AvroSchemaUtil.hasProperty(map, AvroSchemaUtil.VALUE_ID_PROP)) {
      Schema result = Schema.createMap(map.getValueType());
      result.addProp(AvroSchemaUtil.KEY_ID_PROP, keyId);
      result.addProp(AvroSchemaUtil.VALUE_ID_PROP, valueId);
      return result;
    }
    return map;
  }

  @Override
  public Schema primitive(Schema primitive) {
    // primitives are not selected directly
    return null;
  }

  private static Schema copyRecord(Schema record, List<Schema.Field> newFields) {
    Schema copy = Schema.createRecord(record.getName(),
        record.getDoc(), record.getNamespace(), record.isError(), newFields);

    for (Map.Entry<String, Object> prop : record.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    return copy;
  }

  private static Schema.Field copyField(Schema.Field field, Schema newSchema, Integer fieldId) {
    Schema newSchemaReordered;
    // if the newSchema is an optional schema, make sure the NULL option is always the first
    if (isOptionSchemaWithNonNullFirstOption(newSchema)) {
      newSchemaReordered = AvroSchemaUtil.toOption(AvroSchemaUtil.fromOption(newSchema));
    } else {
      newSchemaReordered = newSchema;
    }
    // do not copy over default values as the file is expected to have values for fields already in the file schema
    Schema.Field copy = new Schema.Field(field.name(),
        newSchemaReordered, field.doc(),
        AvroSchemaUtil.isOptionSchema(newSchemaReordered) ? JsonProperties.NULL_VALUE : null, field.order());

    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    if (AvroSchemaUtil.hasFieldId(field)) {
      int existingFieldId = AvroSchemaUtil.getFieldId(field);
      Preconditions.checkArgument(existingFieldId == fieldId,
          "Existing field does match with that fetched from name mapping");
    } else {
      // field may not have a fieldId if the fieldId was fetched from nameMapping
      copy.addProp(AvroSchemaUtil.FIELD_ID_PROP, fieldId);
    }

    return copy;
  }

  private static boolean isOptionSchemaWithNonNullFirstOption(Schema schema) {
    return AvroSchemaUtil.isOptionSchema(schema) && schema.getTypes().get(0).getType() != Schema.Type.NULL;
  }
}
