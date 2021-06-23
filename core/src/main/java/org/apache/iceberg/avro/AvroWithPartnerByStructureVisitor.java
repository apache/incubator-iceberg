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
import org.apache.iceberg.util.Pair;

/**
 * A abstract avro schema visitor with partner type. The visitor rely on the structure matching exactly and are
 * guaranteed that because both schemas are derived from the same Iceberg schema.
 *
 * @param <P> Partner type.
 * @param <T> Return T.
 */
public abstract class AvroWithPartnerByStructureVisitor<P, T> {

  public static <P, T> T visit(P partner, Schema schema, AvroWithPartnerByStructureVisitor<P, T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        return visitRecord(partner, schema, visitor);

      case UNION:
        return visitUnion(partner, schema, visitor);

      case ARRAY:
        return visitArray(partner, schema, visitor);

      case MAP:
        P keyType = visitor.mapKeyType(partner);
        Preconditions.checkArgument(
            visitor.isStringType(keyType),
            "Invalid map: %s is not a string", keyType);

        visitor.beforeMapValue("value", schema.getValueType(), schema);
        T result = visit(visitor.mapValueType(partner), schema.getValueType(), visitor);
        visitor.afterMapValue("value", schema.getValueType(), schema);

        return visitor.map(partner, schema, result);

      default:
        return visitor.primitive(partner, schema);
    }
  }

  // ---------------------------------- Static helpers ---------------------------------------------

  private static <P, T> T visitRecord(P struct, Schema record, AvroWithPartnerByStructureVisitor<P, T> visitor) {
    // check to make sure this hasn't been visited before
    String name = record.getFullName();
    Preconditions.checkState(!visitor.recordLevels.contains(name),
        "Cannot process recursive Avro record %s", name);
    List<Schema.Field> fields = record.getFields();

    visitor.recordLevels.push(name);

    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i += 1) {
      Pair<String, P> nameAndType = visitor.fieldNameAndType(struct, i);
      String fieldName = nameAndType.first();
      Schema.Field field = fields.get(i);
      Preconditions.checkArgument(AvroSchemaUtil.makeCompatibleName(fieldName).equals(field.name()),
          "Structs do not match: field %s != %s", fieldName, field.name());

      visitor.beforeField(field.name(), field.schema(), record);
      results.add(visit(nameAndType.second(), field.schema(), visitor));
      visitor.afterField(field.name(), field.schema(), record);
      names.add(fieldName);
    }

    visitor.recordLevels.pop();

    return visitor.record(struct, record, names, results);
  }

  private static <P, T> T visitUnion(P type, Schema union, AvroWithPartnerByStructureVisitor<P, T> visitor) {
    List<Schema> types = union.getTypes();
    Preconditions.checkArgument(AvroSchemaUtil.isOptionSchema(union),
        "Cannot visit non-option union: %s", union);
    List<T> options = Lists.newArrayListWithExpectedSize(types.size());
    for (Schema branch : types) {
      if (branch.getType() == Schema.Type.NULL) {
        options.add(visit(visitor.nullType(), branch, visitor));
      } else {
        options.add(visit(type, branch, visitor));
      }
    }
    return visitor.union(type, union, options);
  }

  private static <P, T> T visitArray(P type, Schema array, AvroWithPartnerByStructureVisitor<P, T> visitor) {
    T result;

    if (array.getLogicalType() instanceof LogicalMap || visitor.isMapType(type)) {
      Preconditions.checkState(
          AvroSchemaUtil.isKeyValueSchema(array.getElementType()),
          "Cannot visit invalid logical map type: %s", array);
      List<Schema.Field> keyValueFields = array.getElementType().getFields();

      Schema mapKeySchema = keyValueFields.get(0).schema();
      visitor.beforeMapKey("key", mapKeySchema, array.getElementType());
      T key = visit(visitor.mapKeyType(type), keyValueFields.get(0).schema(), visitor);
      visitor.afterMapKey("key", mapKeySchema, array.getElementType());

      Schema mapValueSchema = keyValueFields.get(1).schema();
      visitor.beforeMapValue("value", mapValueSchema, array.getElementType());
      T value = visit(visitor.mapValueType(type), keyValueFields.get(1).schema(), visitor);
      visitor.afterMapValue("value", mapValueSchema, array.getElementType());

      result = visitor.map(type, array, key, value);
    } else {
      visitor.beforeListElement("element", array.getElementType(), array);
      T element = visit(visitor.arrayElementType(type), array.getElementType(), visitor);
      visitor.afterListElement("element", array.getElementType(), array);

      result = visitor.array(type, array, element);
    }

    return result;
  }

  /**
   * Just for checking state.
   */
  private Deque<String> recordLevels = Lists.newLinkedList();

  // ---------------------------------- Partner type methods ---------------------------------------------

  protected abstract boolean isMapType(P type);

  protected abstract boolean isStringType(P type);

  protected abstract P arrayElementType(P arrayType);

  protected abstract P mapKeyType(P mapType);
  protected abstract P mapValueType(P mapType);

  protected abstract Pair<String, P> fieldNameAndType(P structType, int pos);

  protected abstract P nullType();

  // ---------------------------------- Type visitors ---------------------------------------------

  public T record(P struct, Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(P type, Schema union, List<T> options) {
    return null;
  }

  public T array(P sArray, Schema array, T element) {
    return null;
  }

  public T map(P sMap, Schema map, T key, T value) {
    return null;
  }

  public T map(P sMap, Schema map, T value) {
    return null;
  }

  public T primitive(P type, Schema primitive) {
    return null;
  }

  // ---------------------------------- Before/After type methods --------------------------------------

  public void beforeField(String name, Schema type, Schema parentSchema) {
  }

  public void afterField(String name, Schema type, Schema parentSchema) {
  }

  public void beforeListElement(String name, Schema type, Schema parentSchema) {
    beforeField(name, type, parentSchema);
  }

  public void afterListElement(String name, Schema type, Schema parentSchema) {
    afterField(name, type, parentSchema);
  }

  public void beforeMapKey(String name, Schema type, Schema parentSchema) {
    beforeField(name, type, parentSchema);
  }

  public void afterMapKey(String name, Schema type, Schema parentSchema) {
    afterField(name, type, parentSchema);
  }

  public void beforeMapValue(String name, Schema type, Schema parentSchema) {
    beforeField(name, type, parentSchema);
  }

  public void afterMapValue(String name, Schema type, Schema parentSchema) {
    afterField(name, type, parentSchema);
  }
}
