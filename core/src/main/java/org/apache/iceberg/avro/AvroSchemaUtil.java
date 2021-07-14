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
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class AvroSchemaUtil {

  private AvroSchemaUtil() {
  }

  // Original Iceberg field name corresponding to a sanitized Avro name
  public static final String ICEBERG_FIELD_NAME_PROP = "iceberg-field-name";
  public static final String FIELD_ID_PROP = "field-id";
  public static final String KEY_ID_PROP = "key-id";
  public static final String VALUE_ID_PROP = "value-id";
  public static final String ELEMENT_ID_PROP = "element-id";
  public static final String ADJUST_TO_UTC_PROP = "adjust-to-utc";

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private static final Schema.Type MAP = Schema.Type.MAP;
  private static final Schema.Type ARRAY = Schema.Type.ARRAY;
  private static final Schema.Type UNION = Schema.Type.UNION;
  private static final Schema.Type RECORD = Schema.Type.RECORD;

  public static Schema convert(org.apache.iceberg.Schema schema,
                               String tableName) {
    return convert(schema, ImmutableMap.of(schema.asStruct(), tableName));
  }

  public static Schema convert(org.apache.iceberg.Schema schema,
                               Map<Types.StructType, String> names) {
    return TypeUtil.visit(schema, new TypeToSchema(names));
  }

  public static Schema convert(Type type) {
    return convert(type, ImmutableMap.of());
  }

  public static Schema convert(Types.StructType type, String name) {
    return convert(type, ImmutableMap.of(type, name));
  }

  public static Schema convert(Type type, String name) {
    if (type instanceof Types.StructType) {
      return convert((Types.StructType) type, name);
    } else {
      return convert(type);
    }
  }

  public static Schema convert(Type type, Map<Types.StructType, String> names) {
    return TypeUtil.visit(type, new TypeToSchema(names));
  }

  public static Type convert(Schema schema) {
    return AvroSchemaVisitor.visit(schema, new SchemaToType(schema));
  }

  public static org.apache.iceberg.Schema toIceberg(Schema schema) {
    final List<Types.NestedField> fields = convert(schema).asNestedType().asStructType().fields();
    return new org.apache.iceberg.Schema(fields);
  }

  static boolean hasIds(Schema schema) {
    return AvroCustomOrderSchemaVisitor.visit(schema, new HasIds());
  }

  public static Map<Type, Schema> convertTypes(Types.StructType type, String name) {
    TypeToSchema converter = new TypeToSchema(ImmutableMap.of(type, name));
    TypeUtil.visit(type, converter);
    return ImmutableMap.copyOf(converter.getConversionMap());
  }

  public static Schema pruneColumns(Schema schema, Set<Integer> selectedIds, NameMapping nameMapping) {
    return new PruneColumns(selectedIds, nameMapping).rootSchema(schema);
  }

  public static Schema buildAvroProjection(Schema schema, org.apache.iceberg.Schema expected,
                                           Map<String, String> renames) {
    return AvroCustomOrderSchemaVisitor.visit(schema, new BuildAvroProjection(expected, renames));
  }

  public static boolean isTimestamptz(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType instanceof LogicalTypes.TimestampMillis || logicalType instanceof LogicalTypes.TimestampMicros) {
      // timestamptz is adjusted to UTC
      Object value = schema.getObjectProp(ADJUST_TO_UTC_PROP);

      if (value == null) {
        // not all avro timestamp logical types will have the adjust_to_utc prop, default to timestamp without timezone
        return false;
      } else if (value instanceof Boolean) {
        return (Boolean) value;
      } else if (value instanceof String) {
        return Boolean.parseBoolean((String) value);
      }
    }

    return false;
  }

  public static boolean isOptionSchema(Schema schema) {
    if (schema.getType() == UNION && schema.getTypes().size() == 2) {
      if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
        return true;
      } else if (schema.getTypes().get(1).getType() == Schema.Type.NULL) {
        return true;
      }
    }
    return false;
  }

  static Schema toOption(Schema schema) {
    if (schema.getType() == UNION) {
      Preconditions.checkArgument(isOptionSchema(schema),
          "Union schemas are not supported: %s", schema);
      return schema;
    } else {
      return Schema.createUnion(NULL, schema);
    }
  }

  static Schema fromOption(Schema schema) {
    Preconditions.checkArgument(schema.getType() == UNION,
        "Expected union schema but was passed: %s", schema);
    Preconditions.checkArgument(schema.getTypes().size() == 2,
        "Expected optional schema, but was passed: %s", schema);
    if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      return schema.getTypes().get(1);
    } else {
      return schema.getTypes().get(0);
    }
  }

  static Schema fromOptions(List<Schema> options) {
    Preconditions.checkArgument(options.size() == 2,
        "Expected two schemas, but was passed: %s options", options.size());
    if (options.get(0).getType() == Schema.Type.NULL) {
      return options.get(1);
    } else {
      return options.get(0);
    }
  }

  public static boolean isKeyValueSchema(Schema schema) {
    return schema.getType() == RECORD && schema.getFields().size() == 2;
  }

  static Schema createMap(int keyId, Schema keySchema,
                          int valueId, Schema valueSchema) {
    String keyValueName = "k" + keyId + "_v" + valueId;

    Schema.Field keyField = new Schema.Field("key", keySchema, null, (Object) null);
    keyField.addProp(FIELD_ID_PROP, keyId);

    Schema.Field valueField = new Schema.Field("value", valueSchema, null,
        isOptionSchema(valueSchema) ? JsonProperties.NULL_VALUE : null);
    valueField.addProp(FIELD_ID_PROP, valueId);

    return LogicalMap.get().addToSchema(Schema.createArray(Schema.createRecord(
        keyValueName, null, null, false, ImmutableList.of(keyField, valueField))));
  }

  static Schema createProjectionMap(String recordName,
                          int keyId, String keyName, Schema keySchema,
                          int valueId, String valueName, Schema valueSchema) {
    String keyValueName = "k" + keyId + "_v" + valueId;

    Schema.Field keyField = new Schema.Field("key", keySchema, null, (Object) null);
    if (!"key".equals(keyName)) {
      keyField.addAlias(keyName);
    }
    keyField.addProp(FIELD_ID_PROP, keyId);

    Schema.Field valueField = new Schema.Field("value", valueSchema, null,
        isOptionSchema(valueSchema) ? JsonProperties.NULL_VALUE : null);
    valueField.addProp(FIELD_ID_PROP, valueId);
    if (!"value".equals(valueName)) {
      valueField.addAlias(valueName);
    }

    Schema keyValueRecord = Schema.createRecord(
        keyValueName, null, null, false, ImmutableList.of(keyField, valueField));
    if (!keyValueName.equals(recordName)) {
      keyValueRecord.addAlias(recordName);
    }

    return LogicalMap.get().addToSchema(Schema.createArray(keyValueRecord));
  }

  private static Integer getId(Schema schema, String propertyName) {
    Integer id = getId(schema, propertyName, null, null);
    Preconditions.checkNotNull(id, "Missing expected '%s' property", propertyName);
    return id;
  }

  private static Integer getId(Schema schema, String propertyName, NameMapping nameMapping, List<String> names) {
    if (schema.getType() == UNION) {
      return getId(fromOption(schema), propertyName, nameMapping, names);
    }

    Object id = schema.getObjectProp(propertyName);
    if (id != null) {
      return toInt(id);
    } else if (nameMapping != null) {
      MappedField mappedField = nameMapping.find(names);
      if (mappedField != null) {
        return mappedField.id();
      }
    }

    return null;
  }

  static boolean hasProperty(Schema schema, String propertyName) {
    if (schema.getType() == UNION) {
      return hasProperty(fromOption(schema), propertyName);
    }
    return schema.getObjectProp(propertyName) != null;
  }

  public static int getKeyId(Schema schema) {
    Preconditions.checkArgument(schema.getType() == MAP,
        "Cannot get map key id for non-map schema: %s", schema);
    return getId(schema, KEY_ID_PROP);
  }

  static Integer getKeyId(Schema schema, NameMapping nameMapping, Iterable<String> parentFieldNames) {
    Preconditions.checkArgument(schema.getType() == MAP,
        "Cannot get map key id for non-map schema: %s", schema);
    List<String> names = Lists.newArrayList(parentFieldNames);
    names.add("key");
    return getId(schema, KEY_ID_PROP, nameMapping, names);
  }

  public static int getValueId(Schema schema) {
    Preconditions.checkArgument(schema.getType() == MAP,
        "Cannot get map value id for non-map schema: %s", schema);
    return getId(schema, VALUE_ID_PROP);
  }

  static Integer getValueId(Schema schema, NameMapping nameMapping, Iterable<String> parentFieldNames) {
    Preconditions.checkArgument(schema.getType() == MAP,
        "Cannot get map value id for non-map schema: %s", schema);
    List<String> names = Lists.newArrayList(parentFieldNames);
    names.add("value");
    return getId(schema, VALUE_ID_PROP, nameMapping, names);
  }

  public static int getElementId(Schema schema) {
    Preconditions.checkArgument(schema.getType() == ARRAY,
        "Cannot get array element id for non-array schema: %s", schema);
    return getId(schema, ELEMENT_ID_PROP);
  }

  static Integer getElementId(Schema schema, NameMapping nameMapping, Iterable<String> parentFieldNames) {
    Preconditions.checkArgument(schema.getType() == ARRAY,
        "Cannot get array element id for non-array schema: %s", schema);
    List<String> names = Lists.newArrayList(parentFieldNames);
    names.add("element");
    return getId(schema, ELEMENT_ID_PROP, nameMapping, names);
  }

  public static int getFieldId(Schema.Field field) {
    Integer id = getFieldId(field, null, null);
    Preconditions.checkNotNull(id, "Missing expected '%s' property", FIELD_ID_PROP);
    return id;
  }

  static Integer getFieldId(Schema.Field field, NameMapping nameMapping, Iterable<String> parentFieldNames) {
    Object id = field.getObjectProp(FIELD_ID_PROP);
    if (id != null) {
      return toInt(id);
    } else if (nameMapping != null) {
      List<String> names = Lists.newArrayList(parentFieldNames);
      names.add(field.name());
      MappedField mappedField = nameMapping.find(names);
      if (mappedField != null) {
        return mappedField.id();
      }
    }

    return null;
  }

  public static boolean hasFieldId(Schema.Field field) {
    return field.getObjectProp(FIELD_ID_PROP) != null;
  }

  private static int toInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }

    throw new UnsupportedOperationException("Cannot coerce value to int: " + value);
  }

  static Schema copyRecord(Schema record, List<Schema.Field> newFields, String newName) {
    Schema copy;
    if (newName != null) {
      copy = Schema.createRecord(newName, record.getDoc(), null, record.isError(), newFields);
      // the namespace is defaulted to the record's namespace if it is null, which causes renames
      // without the namespace to fail. using "" instead of null changes this behavior to match the
      // original schema.
      copy.addAlias(record.getName(), record.getNamespace() == null ? "" : record.getNamespace());
    } else {
      copy = Schema.createRecord(record.getName(),
          record.getDoc(), record.getNamespace(), record.isError(), newFields);
    }

    for (Map.Entry<String, Object> prop : record.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    return copy;
  }

  static Schema.Field copyField(Schema.Field field, Schema newSchema, String newName) {
    Schema.Field copy = new Schema.Field(newName,
        newSchema, field.doc(), field.defaultVal(), field.order());

    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    if (!newName.equals(field.name())) {
      copy.addAlias(field.name());
    }

    return copy;
  }

  public static String makeCompatibleName(String name) {
    if (!validAvroName(name)) {
      return sanitize(name);
    }
    return name;
  }

  static boolean validAvroName(String name) {
    int length = name.length();
    Preconditions.checkArgument(length > 0, "Empty name");
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      return false;
    }

    for (int i = 1; i < length; i++) {
      char character = name.charAt(i);
      if (!(Character.isLetterOrDigit(character) || character == '_')) {
        return false;
      }
    }
    return true;
  }

  static String sanitize(String name) {
    int length = name.length();
    StringBuilder sb = new StringBuilder(name.length());
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      sb.append(sanitize(first));
    } else {
      sb.append(first);
    }

    for (int i = 1; i < length; i++) {
      char character = name.charAt(i);
      if (!(Character.isLetterOrDigit(character) || character == '_')) {
        sb.append(sanitize(character));
      } else {
        sb.append(character);
      }
    }
    return sb.toString();
  }

  private static String sanitize(char character) {
    if (Character.isDigit(character)) {
      return "_" + character;
    }
    return "_x" + Integer.toHexString(character).toUpperCase();
  }
}
