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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type.NestedType;
import org.apache.iceberg.types.Type.PrimitiveType;

public class Types {

  private Types() {
  }

  private static final ImmutableMap<String, PrimitiveType> TYPES = ImmutableMap
      .<String, PrimitiveType>builder()
      .put(BooleanType.get().toString(), BooleanType.get())
      .put(IntegerType.get().toString(), IntegerType.get())
      .put(LongType.get().toString(), LongType.get())
      .put(FloatType.get().toString(), FloatType.get())
      .put(DoubleType.get().toString(), DoubleType.get())
      .put(DateType.get().toString(), DateType.get())
      .put(TimeType.get().toString(), TimeType.get())
      .put(TimestampType.withZone().toString(), TimestampType.withZone())
      .put(TimestampType.withoutZone().toString(), TimestampType.withoutZone())
      .put(StringType.get().toString(), StringType.get())
      .put(UUIDType.get().toString(), UUIDType.get())
      .put(BinaryType.get().toString(), BinaryType.get())
      .build();

  private static final Pattern FIXED = Pattern.compile("fixed\\[(\\d+)\\]");
  private static final Pattern DECIMAL = Pattern.compile("decimal\\((\\d+),\\s+(\\d+)\\)");

  public static PrimitiveType fromPrimitiveString(String typeString) {
    String lowerTypeString = typeString.toLowerCase(Locale.ROOT);
    if (TYPES.containsKey(lowerTypeString)) {
      return TYPES.get(lowerTypeString);
    }

    Matcher fixed = FIXED.matcher(lowerTypeString);
    if (fixed.matches()) {
      return FixedType.ofLength(Integer.parseInt(fixed.group(1)));
    }

    Matcher decimal = DECIMAL.matcher(lowerTypeString);
    if (decimal.matches()) {
      return DecimalType.of(
          Integer.parseInt(decimal.group(1)),
          Integer.parseInt(decimal.group(2)));
    }

    throw new IllegalArgumentException("Cannot parse type string to primitive: " + typeString);
  }

  public static class BooleanType extends PrimitiveType {
    private static final BooleanType INSTANCE = new BooleanType();

    public static BooleanType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.BOOLEAN;
    }

    @Override
    public String toString() {
      return "boolean";
    }
  }

  public static class IntegerType extends PrimitiveType {
    private static final IntegerType INSTANCE = new IntegerType();

    public static IntegerType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.INTEGER;
    }

    @Override
    public String toString() {
      return "int";
    }
  }

  public static class LongType extends PrimitiveType {
    private static final LongType INSTANCE = new LongType();

    public static LongType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LONG;
    }

    @Override
    public String toString() {
      return "long";
    }
  }

  public static class FloatType extends PrimitiveType {
    private static final FloatType INSTANCE = new FloatType();

    public static FloatType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.FLOAT;
    }

    @Override
    public String toString() {
      return "float";
    }
  }

  public static class DoubleType extends PrimitiveType {
    private static final DoubleType INSTANCE = new DoubleType();

    public static DoubleType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DOUBLE;
    }

    @Override
    public String toString() {
      return "double";
    }
  }

  public static class DateType extends PrimitiveType {
    private static final DateType INSTANCE = new DateType();

    public static DateType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DATE;
    }

    @Override
    public String toString() {
      return "date";
    }
  }

  public static class TimeType extends PrimitiveType {
    private static final TimeType INSTANCE = new TimeType();

    public static TimeType get() {
      return INSTANCE;
    }

    private TimeType() {
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIME;
    }

    @Override
    public String toString() {
      return "time";
    }
  }

  public static class TimestampType extends PrimitiveType {
    private static final TimestampType INSTANCE_WITH_ZONE = new TimestampType(true);
    private static final TimestampType INSTANCE_WITHOUT_ZONE = new TimestampType(false);

    public static TimestampType withZone() {
      return INSTANCE_WITH_ZONE;
    }

    public static TimestampType withoutZone() {
      return INSTANCE_WITHOUT_ZONE;
    }

    private final boolean adjustToUTC;

    private TimestampType(boolean adjustToUTC) {
      this.adjustToUTC = adjustToUTC;
    }

    public boolean shouldAdjustToUTC() {
      return adjustToUTC;
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIMESTAMP;
    }

    @Override
    public String toString() {
      if (shouldAdjustToUTC()) {
        return "timestamptz";
      } else {
        return "timestamp";
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof TimestampType)) {
        return false;
      }

      TimestampType that = (TimestampType) o;
      return adjustToUTC == that.adjustToUTC;
    }

    @Override
    public int hashCode() {
      return Objects.hash(TimestampType.class, adjustToUTC);
    }
  }

  public static class StringType extends PrimitiveType {
    private static final StringType INSTANCE = new StringType();

    public static StringType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.STRING;
    }

    @Override
    public String toString() {
      return "string";
    }
  }

  public static class UUIDType extends PrimitiveType {
    private static final UUIDType INSTANCE = new UUIDType();

    public static UUIDType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.UUID;
    }

    @Override
    public String toString() {
      return "uuid";
    }
  }

  public static class FixedType extends PrimitiveType {
    public static FixedType ofLength(int length) {
      return new FixedType(length);
    }

    private final int length;

    private FixedType(int length) {
      this.length = length;
    }

    public int length() {
      return length;
    }

    @Override
    public TypeID typeId() {
      return TypeID.FIXED;
    }

    @Override
    public String toString() {
      return String.format("fixed[%d]", length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof FixedType)) {
        return false;
      }

      FixedType that = (FixedType) o;
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(FixedType.class, length);
    }
  }

  public static class BinaryType extends PrimitiveType {
    private static final BinaryType INSTANCE = new BinaryType();

    public static BinaryType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.BINARY;
    }

    @Override
    public String toString() {
      return "binary";
    }
  }

  public static class DecimalType extends PrimitiveType {
    public static DecimalType of(int precision, int scale) {
      return new DecimalType(precision, scale);
    }

    private final int scale;
    private final int precision;

    private DecimalType(int precision, int scale) {
      Preconditions.checkArgument(precision <= 38,
          "Decimals with precision larger than 38 are not supported: %s", precision);
      this.scale = scale;
      this.precision = precision;
    }

    public int scale() {
      return scale;
    }

    public int precision() {
      return precision;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DECIMAL;
    }

    @Override
    public String toString() {
      return String.format("decimal(%d, %d)", precision, scale);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof DecimalType)) {
        return false;
      }

      DecimalType that = (DecimalType) o;
      if (scale != that.scale) {
        return false;
      }
      return precision == that.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(DecimalType.class, scale, precision);
    }
  }

  public static class NestedField implements Serializable {
    public static NestedField optional(int id, String name, Type type) {
      return new NestedField(true, id, name, type, null, null);
    }

    public static NestedField optional(int id, String name, Type type, String doc) {
      return new NestedField(true, id, name, type, null, doc);
    }

    public static NestedField optional(int id, String name, Type type, Object defaultValue, String doc) {
      return new NestedField(true, id, name, type, defaultValue, doc);
    }

    public static NestedField required(int id, String name, Type type) {
      return new NestedField(false, id, name, type, null, null);
    }

    public static NestedField required(int id, String name, Type type, String doc) {
      return new NestedField(false, id, name, type, null, doc);
    }

    public static NestedField required(int id, String name, Type type, Object defaultValue, String doc) {
      validateDefaultValueForRequiredField(defaultValue, name);
      return new NestedField(false, id, name, type, defaultValue, doc);
    }

    public static NestedField of(int id, boolean isOptional, String name, Type type) {
      return new NestedField(isOptional, id, name, type, null, null);
    }

    public static NestedField of(int id, boolean isOptional, String name, Type type, String doc) {
      return new NestedField(isOptional, id, name, type, null, doc);
    }

    public static NestedField of(int id, boolean isOptional, String name, Type type, Object defaultValue, String doc) {
      return new NestedField(isOptional, id, name, type, defaultValue, doc);
    }

    private static void validateDefaultValueForRequiredField(Object defaultValue, String fieldName) {
      Preconditions.checkArgument(defaultValue != null,
          "Cannot create NestedField with a null default for the required field: " +  fieldName);
    }

    private static void validateDefaultValue(Object defaultValue, Type type) {
      if (defaultValue == null) {
        return;
      }
      switch (type.typeId()) {
        case STRUCT:
          Preconditions.checkArgument(List.class.isInstance(defaultValue),
              "defaultValue should be a List of Objects for StructType");
          if (defaultValue == null) {
            return;
          }
          List<Object> defaultList = (List) defaultValue;
          Preconditions.checkArgument(defaultList.size() == type.asStructType().fields().size());
          for (int i = 0; i < defaultList.size(); i++) {
            NestedField.validateDefaultValue(defaultList.get(i), type.asStructType().fields().get(i).type);
          }
          break;
        case LIST:
          Preconditions.checkArgument(defaultValue instanceof ArrayList,
              "defaultValue should be an ArrayList of Objects, for ListType");
          List<Object> defaultArrayList = (ArrayList<Object>) defaultValue;
          if (defaultArrayList == null || defaultArrayList.size() == 0) {
            return;
          }
          defaultArrayList.forEach(dv -> NestedField.validateDefaultValue(dv, type.asListType().elementField.type));
          break;
        case MAP:
          Preconditions.checkArgument(Map.class.isInstance(defaultValue),
              "defaultValue should be an instance of Map for MapType");
          Map<Object, Object> defaultMap = (Map<Object, Object>) defaultValue;
          if (defaultMap == null || defaultMap.isEmpty()) {
            return;
          }
          for (Map.Entry e : defaultMap.entrySet()) {
            NestedField.validateDefaultValue(e.getKey(), type.asMapType().keyField.type);
            NestedField.validateDefaultValue(e.getValue(), type.asMapType().valueField.type);
          }
          break;
        default:
          Preconditions.checkArgument(type.typeId().javaClass().isInstance(defaultValue),
              "defaultValue should be of same java class of the type, defaultValue class: " + defaultValue.getClass() +
              ", type class: " + type.typeId().javaClass());
      }
    }

    private final boolean isOptional;
    private final int id;
    private final String name;
    private final Type type;
    private final Object defaultValue;
    private final String doc;

    private NestedField(boolean isOptional, int id, String name, Type type, Object defaultValue, String doc) {
      Preconditions.checkNotNull(name, "Name cannot be null");
      Preconditions.checkNotNull(type, "Type cannot be null");
      validateDefaultValue(defaultValue, type);
      this.isOptional = isOptional;
      this.id = id;
      this.name = name;
      this.type = type;
      this.defaultValue = defaultValue;
      this.doc = doc;
    }

    public boolean isOptional() {
      return isOptional;
    }

    public NestedField asOptional() {
      if (isOptional) {
        return this;
      }
      return new NestedField(true, id, name, type, defaultValue, doc);
    }

    public boolean isRequired() {
      return !isOptional;
    }

    public NestedField asRequired() {
      if (!isOptional) {
        return this;
      }
      return new NestedField(false, id, name, type, defaultValue, doc);
    }

    public boolean hasDefaultValue() {
      return defaultValue != null;
    }

    public Object getDefaultValue() {
      return defaultValue;
    }

    public int fieldId() {
      return id;
    }

    public String name() {
      return name;
    }

    public Type type() {
      return type;
    }

    public String doc() {
      return doc;
    }

    @Override
    public String toString() {
      return String.format("%d: %s: %s %s",
          id, name, isOptional ? "optional" : "required", type) +
          (defaultValue != null ? ", default value: " + defaultValue + ", " : "") +
          (doc != null ? " (" + doc + ")" : "");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof NestedField)) {
        return false;
      }

      NestedField that = (NestedField) o;
      if (isOptional != that.isOptional) {
        return false;
      } else if (id != that.id) {
        return false;
      } else if (!name.equals(that.name)) {
        return false;
      } else if (!Objects.equals(defaultValue, that.defaultValue)) {
        return false;
      } else if (!Objects.equals(doc, that.doc)) {
        return false;
      }
      return type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return hasDefaultValue() ? Objects.hash(NestedField.class, id, isOptional, name, type, defaultValue) :
          Objects.hash(NestedField.class, id, isOptional, name, type);
    }
  }

  public static class StructType extends NestedType {
    private static final Joiner FIELD_SEP = Joiner.on(", ");

    public static StructType of(NestedField... fields) {
      return of(Arrays.asList(fields));
    }

    public static StructType of(List<NestedField> fields) {
      return new StructType(fields);
    }

    private final NestedField[] fields;

    // lazy values
    private transient List<NestedField> fieldList = null;
    private transient Map<String, NestedField> fieldsByName = null;
    private transient Map<String, NestedField> fieldsByLowerCaseName = null;
    private transient Map<Integer, NestedField> fieldsById = null;

    private StructType(List<NestedField> fields) {
      Preconditions.checkNotNull(fields, "Field list cannot be null");
      this.fields = new NestedField[fields.size()];
      for (int i = 0; i < this.fields.length; i += 1) {
        this.fields[i] = fields.get(i);
      }
    }

    @Override
    public List<NestedField> fields() {
      return lazyFieldList();
    }

    public NestedField field(String name) {
      return lazyFieldsByName().get(name);
    }

    @Override
    public NestedField field(int id) {
      return lazyFieldsById().get(id);
    }

    public NestedField caseInsensitiveField(String name) {
      return lazyFieldsByLowerCaseName().get(name.toLowerCase(Locale.ROOT));
    }

    @Override
    public Type fieldType(String name) {
      NestedField field = field(name);
      if (field != null) {
        return field.type();
      }
      return null;
    }

    @Override
    public TypeID typeId() {
      return TypeID.STRUCT;
    }

    @Override
    public boolean isStructType() {
      return true;
    }

    @Override
    public Types.StructType asStructType() {
      return this;
    }

    @Override
    public String toString() {
      return String.format("struct<%s>", FIELD_SEP.join(fields));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof StructType)) {
        return false;
      }

      StructType that = (StructType) o;
      return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(NestedField.class, Arrays.hashCode(fields));
    }

    private List<NestedField> lazyFieldList() {
      if (fieldList == null) {
        this.fieldList = ImmutableList.copyOf(fields);
      }
      return fieldList;
    }

    private Map<String, NestedField> lazyFieldsByName() {
      if (fieldsByName == null) {
        ImmutableMap.Builder<String, NestedField> byNameBuilder = ImmutableMap.builder();
        for (NestedField field : fields) {
          byNameBuilder.put(field.name(), field);
        }
        fieldsByName = byNameBuilder.build();
      }
      return fieldsByName;
    }

    private Map<String, NestedField> lazyFieldsByLowerCaseName() {
      if (fieldsByLowerCaseName == null) {
        ImmutableMap.Builder<String, NestedField> byLowerCaseNameBuilder = ImmutableMap.builder();
        for (NestedField field : fields) {
          byLowerCaseNameBuilder.put(field.name().toLowerCase(Locale.ROOT), field);
        }
        fieldsByLowerCaseName = byLowerCaseNameBuilder.build();
      }
      return fieldsByLowerCaseName;
    }

    private Map<Integer, NestedField> lazyFieldsById() {
      if (fieldsById == null) {
        ImmutableMap.Builder<Integer, NestedField> byIdBuilder = ImmutableMap.builder();
        for (NestedField field : fields) {
          byIdBuilder.put(field.fieldId(), field);
        }
        this.fieldsById = byIdBuilder.build();
      }
      return fieldsById;
    }
  }

  public static class ListType extends NestedType {
    public static ListType ofOptional(int elementId, Type elementType) {
      Preconditions.checkNotNull(elementType, "Element type cannot be null");
      return new ListType(NestedField.optional(elementId, "element", elementType));
    }

    public static ListType ofRequired(int elementId, Type elementType) {
      Preconditions.checkNotNull(elementType, "Element type cannot be null");
      return new ListType(NestedField.required(elementId, "element", elementType));
    }

    private final NestedField elementField;
    private transient List<NestedField> fields = null;

    private ListType(NestedField elementField) {
      this.elementField = elementField;
    }

    public Type elementType() {
      return elementField.type();
    }

    @Override
    public Type fieldType(String name) {
      if ("element".equals(name)) {
        return elementType();
      }
      return null;
    }

    @Override
    public NestedField field(int id) {
      if (elementField.fieldId() == id) {
        return elementField;
      }
      return null;
    }

    @Override
    public List<NestedField> fields() {
      return lazyFieldList();
    }

    public int elementId() {
      return elementField.fieldId();
    }

    public boolean isElementRequired() {
      return !elementField.isOptional;
    }

    public boolean isElementOptional() {
      return elementField.isOptional;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LIST;
    }

    @Override
    public boolean isListType() {
      return true;
    }

    @Override
    public Types.ListType asListType() {
      return this;
    }

    @Override
    public String toString() {
      return String.format("list<%s>", elementField.type());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof ListType)) {
        return false;
      }

      ListType that = (ListType) o;
      return elementField.equals(that.elementField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ListType.class, elementField);
    }

    private List<NestedField> lazyFieldList() {
      if (fields == null) {
        this.fields = ImmutableList.of(elementField);
      }
      return fields;
    }
  }

  public static class MapType extends NestedType {
    public static MapType ofOptional(int keyId, int valueId, Type keyType, Type valueType) {
      Preconditions.checkNotNull(valueType, "Value type cannot be null");
      return new MapType(
          NestedField.required(keyId, "key", keyType),
          NestedField.optional(valueId, "value", valueType));
    }

    public static MapType ofRequired(int keyId, int valueId, Type keyType, Type valueType) {
      Preconditions.checkNotNull(valueType, "Value type cannot be null");
      return new MapType(
          NestedField.required(keyId, "key", keyType),
          NestedField.required(valueId, "value", valueType));
    }

    private final NestedField keyField;
    private final NestedField valueField;
    private transient List<NestedField> fields = null;

    private MapType(NestedField keyField, NestedField valueField) {
      this.keyField = keyField;
      this.valueField = valueField;
    }

    public Type keyType() {
      return keyField.type();
    }

    public Type valueType() {
      return valueField.type();
    }

    @Override
    public Type fieldType(String name) {
      if ("key".equals(name)) {
        return keyField.type();
      } else if ("value".equals(name)) {
        return valueField.type();
      }
      return null;
    }

    @Override
    public NestedField field(int id) {
      if (keyField.fieldId() == id) {
        return keyField;
      } else if (valueField.fieldId() == id) {
        return valueField;
      }
      return null;
    }

    @Override
    public List<NestedField> fields() {
      return lazyFieldList();
    }

    public int keyId() {
      return keyField.fieldId();
    }

    public int valueId() {
      return valueField.fieldId();
    }

    public boolean isValueRequired() {
      return !valueField.isOptional;
    }

    public boolean isValueOptional() {
      return valueField.isOptional;
    }

    @Override
    public TypeID typeId() {
      return TypeID.MAP;
    }

    @Override
    public boolean isMapType() {
      return true;
    }

    @Override
    public Types.MapType asMapType() {
      return this;
    }

    @Override
    public String toString() {
      return String.format("map<%s, %s>", keyField.type(), valueField.type());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof MapType)) {
        return false;
      }

      MapType that = (MapType) o;
      if (!keyField.equals(that.keyField)) {
        return false;
      }
      return valueField.equals(that.valueField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(MapType.class, keyField, valueField);
    }

    private List<NestedField> lazyFieldList() {
      if (fields == null) {
        this.fields = ImmutableList.of(keyField, valueField);
      }
      return fields;
    }
  }
}
