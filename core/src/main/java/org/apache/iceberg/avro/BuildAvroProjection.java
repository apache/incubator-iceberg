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
import java.util.function.Supplier;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Renames and aliases fields in an Avro schema based on the current table schema.
 * <p>
 * This class creates a read schema based on an Avro file's schema that will correctly translate
 * from the file's field names to the current table schema.
 * <p>
 * This will also rename records in the file's Avro schema to support custom read classes.
 */
class BuildAvroProjection extends AvroCustomOrderSchemaVisitor<Schema, Schema.Field> {
  private final Map<String, String> renames;
  private Type current = null;

  BuildAvroProjection(org.apache.iceberg.Schema expectedSchema, Map<String, String> renames) {
    this.renames = renames;
    this.current = expectedSchema.asStruct();
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Schema record(Schema record, List<String> names, Iterable<Schema.Field> schemaIterable) {
    Preconditions.checkArgument(
        current.isNestedType() && current.asNestedType().isStructType(),
        "Cannot project non-struct: %s", current);

    Types.StructType struct = current.asNestedType().asStructType();

    boolean hasChange = false;
    List<Schema.Field> fields = record.getFields();
    List<Schema.Field> fieldResults = Lists.newArrayList(schemaIterable);

    Map<String, Schema.Field> updateMap = Maps.newHashMap();
    for (int i = 0; i < fields.size(); i += 1) {
      Schema.Field field = fields.get(i);
      Schema.Field updatedField = fieldResults.get(i);

      if (updatedField != null) {
        updateMap.put(updatedField.name(), updatedField);

        if (!updatedField.schema().equals(field.schema()) ||
            !updatedField.name().equals(field.name())) {
          hasChange = true;
        }
      } else {
        hasChange = true; // column was not projected
      }
    }

    // construct the schema using the expected order
    List<Schema.Field> updatedFields = Lists.newArrayListWithExpectedSize(struct.fields().size());
    List<Types.NestedField> expectedFields = struct.fields();
    for (int i = 0; i < expectedFields.size(); i += 1) {
      Types.NestedField field = expectedFields.get(i);

      // detect reordering
      if (i < fields.size() && !field.name().equals(fields.get(i).name())) {
        hasChange = true;
      }

      Schema.Field avroField = updateMap.get(AvroSchemaUtil.makeCompatibleName(field.name()));

      if (avroField != null) {
        updatedFields.add(avroField);

      } else {
        Preconditions.checkArgument(
            field.isOptional() || MetadataColumns.metadataFieldIds().contains(field.fieldId()),
            "Missing required field: %s", field.name());
        // Create a field that will be defaulted to null. We assign a unique suffix to the field
        // to make sure that even if records in the file have the field it is not projected.
        Schema.Field newField = new Schema.Field(
            field.name() + "_r" + field.fieldId(),
            AvroSchemaUtil.toOption(AvroSchemaUtil.convert(field.type())), null, JsonProperties.NULL_VALUE);
        newField.addProp(AvroSchemaUtil.FIELD_ID_PROP, field.fieldId());
        updatedFields.add(newField);
        hasChange = true;
      }
    }

    if (hasChange || renames.containsKey(record.getFullName())) {
      return AvroSchemaUtil.copyRecord(record, updatedFields, renames.get(record.getFullName()));
    }

    return record;
  }

  @Override
  public Schema.Field field(Schema.Field field, Supplier<Schema> fieldResult) {
    Types.StructType struct = current.asNestedType().asStructType();
    int fieldId = AvroSchemaUtil.getFieldId(field);
    Types.NestedField expectedField = struct.field(fieldId);

    // if the field isn't present, it was not selected
    if (expectedField == null) {
      return null;
    }

    String expectedName = expectedField.name();

    this.current = expectedField.type();
    try {
      Schema schema = fieldResult.get();

      if (!Objects.equals(schema, field.schema()) || !expectedName.equals(field.name())) {
        // add an alias for the field
        return AvroSchemaUtil.copyField(field, schema, AvroSchemaUtil.makeCompatibleName(expectedName));
      } else {
        // always copy because fields can't be reused
        return AvroSchemaUtil.copyField(field, field.schema(), field.name());
      }

    } finally {
      this.current = struct;
    }
  }

  @Override
  public Schema union(Schema union, Iterable<Schema> options) {
    Preconditions.checkState(AvroSchemaUtil.isOptionSchema(union),
        "Invalid schema: non-option unions are not supported: %s", union);
    Schema nonNullOriginal = AvroSchemaUtil.fromOption(union);
    Schema nonNullResult = AvroSchemaUtil.fromOptions(Lists.newArrayList(options));

    if (!Objects.equals(nonNullOriginal, nonNullResult)) {
      return AvroSchemaUtil.toOption(nonNullResult);
    }

    return union;
  }

  @Override
  public Schema array(Schema array, Supplier<Schema> element) {
    if (array.getLogicalType() instanceof LogicalMap ||
        current.isMapType() && AvroSchemaUtil.isKeyValueSchema(array.getElementType())) {
      Preconditions.checkArgument(current.isMapType(), "Incompatible projected type: %s", current);
      Types.MapType asMapType = current.asNestedType().asMapType();
      this.current = Types.StructType.of(asMapType.fields()); // create a struct to correspond to element
      try {
        Schema keyValueSchema = array.getElementType();
        Schema.Field keyField = keyValueSchema.getFields().get(0);
        Schema.Field valueField = keyValueSchema.getFields().get(1);
        Schema.Field valueProjection = element.get().getField("value");

        // element was changed, create a new array
        if (!Objects.equals(valueProjection.schema(), valueField.schema())) {
          return AvroSchemaUtil.createProjectionMap(keyValueSchema.getFullName(),
              AvroSchemaUtil.getFieldId(keyField), keyField.name(), keyField.schema(),
              AvroSchemaUtil.getFieldId(valueField), valueField.name(), valueProjection.schema());
        } else if (!(array.getLogicalType() instanceof LogicalMap)) {
          return AvroSchemaUtil.createProjectionMap(keyValueSchema.getFullName(),
              AvroSchemaUtil.getFieldId(keyField), keyField.name(), keyField.schema(),
              AvroSchemaUtil.getFieldId(valueField), valueField.name(), valueField.schema());
        }

        return array;

      } finally {
        this.current = asMapType;
      }

    } else {
      Preconditions.checkArgument(current.isListType(),
          "Incompatible projected type: %s", current);
      Types.ListType list = current.asNestedType().asListType();
      this.current = list.elementType();
      try {
        Schema elementSchema = element.get();

        // element was changed, create a new array
        if (!Objects.equals(elementSchema, array.getElementType())) {
          return Schema.createArray(elementSchema);
        }

        return array;

      } finally {
        this.current = list;
      }
    }
  }

  @Override
  public Schema map(Schema map, Supplier<Schema> value) {
    Preconditions.checkArgument(current.isNestedType() && current.asNestedType().isMapType(),
        "Incompatible projected type: %s", current);
    Types.MapType asMapType = current.asNestedType().asMapType();
    Preconditions.checkArgument(asMapType.keyType() == Types.StringType.get(),
        "Incompatible projected type: key type %s is not string", asMapType.keyType());
    this.current = asMapType.valueType();
    try {
      Schema valueSchema = value.get();

      // element was changed, create a new map
      if (!Objects.equals(valueSchema, map.getValueType())) {
        return Schema.createMap(valueSchema);
      }

      return map;

    } finally {
      this.current = asMapType;
    }
  }

  @Override
  public Schema primitive(Schema primitive) {
    // check for type promotion
    switch (primitive.getType()) {
      case INT:
        if (current.typeId() == Type.TypeID.LONG) {
          return Schema.create(Schema.Type.LONG);
        }
        return primitive;

      case FLOAT:
        if (current.typeId() == Type.TypeID.DOUBLE) {
          return Schema.create(Schema.Type.DOUBLE);
        }
        return primitive;

      default:
        return primitive;
    }
  }

}
