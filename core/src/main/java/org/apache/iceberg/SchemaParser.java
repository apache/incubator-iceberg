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

package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class SchemaParser {

  private SchemaParser() {
  }

  private static final String SCHEMA_ID = "schema-id";
  private static final String IDENTIFIER_FIELD_IDS = "identifier-field-ids";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String DOC = "doc";
  private static final String EXPR = "expr";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String REQUIRED = "required";
  private static final String ELEMENT_REQUIRED = "element-required";
  private static final String VALUE_REQUIRED = "value-required";

  private static void toJson(Types.StructType struct, JsonGenerator generator) throws IOException {
    toJson(struct, null, null, generator);
  }

  private static void toJson(Types.StructType struct, Integer schemaId, Set<Integer> identifierFieldIds,
                             JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, STRUCT);
    if (schemaId != null) {
      generator.writeNumberField(SCHEMA_ID, schemaId);
    }

    if (identifierFieldIds != null && !identifierFieldIds.isEmpty()) {
      generator.writeArrayFieldStart(IDENTIFIER_FIELD_IDS);
      for (int id : identifierFieldIds) {
        generator.writeNumber(id);
      }
      generator.writeEndArray();
    }

    generator.writeArrayFieldStart(FIELDS);
    for (Types.NestedField field : struct.fields()) {
      generator.writeStartObject();
      generator.writeNumberField(ID, field.fieldId());
      generator.writeStringField(NAME, field.name());
      generator.writeBooleanField(REQUIRED, field.isRequired());
      generator.writeFieldName(TYPE);
      toJson(field.type(), generator);
      if (field.doc() != null) {
        generator.writeStringField(DOC, field.doc());
      }

      if (field.expr() != null) {
        generator.writeStringField(EXPR, field.expr());
      }

      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  static void toJson(Types.ListType list, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, LIST);

    generator.writeNumberField(ELEMENT_ID, list.elementId());
    generator.writeFieldName(ELEMENT);
    toJson(list.elementType(), generator);
    generator.writeBooleanField(ELEMENT_REQUIRED, !list.isElementOptional());

    generator.writeEndObject();
  }

  static void toJson(Types.MapType map, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, MAP);

    generator.writeNumberField(KEY_ID, map.keyId());
    generator.writeFieldName(KEY);
    toJson(map.keyType(), generator);

    generator.writeNumberField(VALUE_ID, map.valueId());
    generator.writeFieldName(VALUE);
    toJson(map.valueType(), generator);
    generator.writeBooleanField(VALUE_REQUIRED, !map.isValueOptional());

    generator.writeEndObject();
  }

  static void toJson(Type.PrimitiveType primitive, JsonGenerator generator) throws IOException {
    generator.writeString(primitive.toString());
  }

  static void toJson(Type type, JsonGenerator generator) throws IOException {
    if (type.isPrimitiveType()) {
      toJson(type.asPrimitiveType(), generator);
    } else {
      Type.NestedType nested = type.asNestedType();
      switch (type.typeId()) {
        case STRUCT:
          toJson(nested.asStructType(), generator);
          break;
        case LIST:
          toJson(nested.asListType(), generator);
          break;
        case MAP:
          toJson(nested.asMapType(), generator);
          break;
        default:
          throw new IllegalArgumentException("Cannot write unknown type: " + type);
      }
    }
  }

  public static void toJson(Schema schema, JsonGenerator generator) throws IOException {
    toJson(schema.asStruct(), schema.schemaId(), schema.identifierFieldIds(), generator);
  }

  public static String toJson(Schema schema) {
    return toJson(schema, false);
  }

  public static String toJson(Schema schema, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(schema.asStruct(), schema.schemaId(), schema.identifierFieldIds(), generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static Type typeFromJson(JsonNode json) {
    if (json.isTextual()) {
      return Types.fromPrimitiveString(json.asText());

    } else if (json.isObject()) {
      String type = json.get(TYPE).asText();
      if (STRUCT.equals(type)) {
        return structFromJson(json);
      } else if (LIST.equals(type)) {
        return listFromJson(json);
      } else if (MAP.equals(type)) {
        return mapFromJson(json);
      }
    }

    throw new IllegalArgumentException("Cannot parse type from json: " + json);
  }

  private static Types.StructType structFromJson(JsonNode json) {
    JsonNode fieldArray = json.get(FIELDS);
    Preconditions.checkArgument(fieldArray.isArray(),
        "Cannot parse struct fields from non-array: %s", fieldArray);

    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldArray.size());
    Iterator<JsonNode> iterator = fieldArray.elements();
    while (iterator.hasNext()) {
      JsonNode field = iterator.next();
      Preconditions.checkArgument(field.isObject(),
          "Cannot parse struct field from non-object: %s", field);

      int id = JsonUtil.getInt(ID, field);
      String name = JsonUtil.getString(NAME, field);
      Type type = typeFromJson(field.get(TYPE));

      String doc = JsonUtil.getStringOrNull(DOC, field);
      String expr = JsonUtil.getStringOrNull(EXPR, field);
      boolean isRequired = JsonUtil.getBool(REQUIRED, field);
      if (isRequired) {
        fields.add(Types.NestedField.required(id, name, type, doc, expr));
      } else {
        fields.add(Types.NestedField.optional(id, name, type, doc, expr));
      }
    }

    return Types.StructType.of(fields);
  }


  private static Types.ListType listFromJson(JsonNode json) {
    int elementId = JsonUtil.getInt(ELEMENT_ID, json);
    Type elementType = typeFromJson(json.get(ELEMENT));
    boolean isRequired = JsonUtil.getBool(ELEMENT_REQUIRED, json);

    if (isRequired) {
      return Types.ListType.ofRequired(elementId, elementType);
    } else {
      return Types.ListType.ofOptional(elementId, elementType);
    }
  }

  private static Types.MapType mapFromJson(JsonNode json) {
    int keyId = JsonUtil.getInt(KEY_ID, json);
    Type keyType = typeFromJson(json.get(KEY));

    int valueId = JsonUtil.getInt(VALUE_ID, json);
    Type valueType = typeFromJson(json.get(VALUE));

    boolean isRequired = JsonUtil.getBool(VALUE_REQUIRED, json);

    if (isRequired) {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    }
  }

  public static Schema fromJson(JsonNode json) {
    Type type  = typeFromJson(json);
    Preconditions.checkArgument(type.isNestedType() && type.asNestedType().isStructType(),
        "Cannot create schema, not a struct type: %s", type);
    Integer schemaId = JsonUtil.getIntOrNull(SCHEMA_ID, json);
    Set<Integer> identifierFieldIds = JsonUtil.getIntegerSetOrNull(IDENTIFIER_FIELD_IDS, json);

    if (schemaId == null) {
      return new Schema(type.asNestedType().asStructType().fields(), identifierFieldIds);
    } else {
      return new Schema(schemaId, type.asNestedType().asStructType().fields(), identifierFieldIds);
    }
  }

  private static final Cache<String, Schema> SCHEMA_CACHE = Caffeine.newBuilder()
      .weakValues()
      .build();

  public static Schema fromJson(String json) {
    return SCHEMA_CACHE.get(json, jsonKey -> {
      try {
        return fromJson(JsonUtil.mapper().readValue(jsonKey, JsonNode.class));
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    });
  }
}
