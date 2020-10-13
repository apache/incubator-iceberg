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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestIcebergObjectInspector {

  private final Schema schema = new Schema(
          required(1, "binary_field", Types.BinaryType.get(), "binary comment"),
          required(2, "boolean_field", Types.BooleanType.get(), "boolean comment"),
          required(3, "date_field", Types.DateType.get(), "date comment"),
          required(4, "decimal_field", Types.DecimalType.of(38, 18), "decimal comment"),
          required(5, "double_field", Types.DoubleType.get(), "double comment"),
          required(6, "fixed_field", Types.FixedType.ofLength(3), "fixed comment"),
          required(7, "float_field", Types.FloatType.get(), "float comment"),
          required(8, "integer_field", Types.IntegerType.get(), "integer comment"),
          required(9, "long_field", Types.LongType.get(), "long comment"),
          required(10, "string_field", Types.StringType.get(), "string comment"),
          required(11, "timestamp_field", Types.TimestampType.withoutZone(), "timestamp comment"),
          required(12, "timestamptz_field", Types.TimestampType.withZone(), "timestamptz comment"),
          required(13, "uuid_field", Types.UUIDType.get(), "uuid comment"),
          required(14, "list_field",
                  Types.ListType.ofRequired(15, Types.StringType.get()), "list comment"),
          required(16, "map_field",
                  Types.MapType.ofRequired(17, 18, Types.StringType.get(), Types.IntegerType.get()),
                  "map comment"),
          required(19, "struct_field", Types.StructType.of(
                  Types.NestedField.required(20, "nested_field", Types.StringType.get(), "nested field comment")),
                  "struct comment"
          )
  );

  @Test
  public void testIcebergObjectInspector() {
    ObjectInspector oi = IcebergObjectInspector.create(schema);
    Assert.assertNotNull(oi);
    Assert.assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

    StructObjectInspector soi = (StructObjectInspector) oi;

    // binary
    StructField binaryField = soi.getStructFieldRef("binary_field");
    Assert.assertEquals(1, binaryField.getFieldID());
    Assert.assertEquals("binary_field", binaryField.getFieldName());
    Assert.assertEquals("binary comment", binaryField.getFieldComment());
    Assert.assertEquals(IcebergBinaryObjectInspector.byteBuffer(), binaryField.getFieldObjectInspector());

    // boolean
    StructField booleanField = soi.getStructFieldRef("boolean_field");
    Assert.assertEquals(2, booleanField.getFieldID());
    Assert.assertEquals("boolean_field", booleanField.getFieldName());
    Assert.assertEquals("boolean comment", booleanField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(boolean.class), booleanField.getFieldObjectInspector());

    // date
    StructField dateField = soi.getStructFieldRef("date_field");
    Assert.assertEquals(3, dateField.getFieldID());
    Assert.assertEquals("date_field", dateField.getFieldName());
    Assert.assertEquals("date comment", dateField.getFieldComment());
    if (MetastoreUtil.hive3PresentOnClasspath()) {
      Assert.assertEquals("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspectorHive3",
              dateField.getFieldObjectInspector().getClass().getName());
    } else {
      Assert.assertEquals("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspector",
              dateField.getFieldObjectInspector().getClass().getName());
    }

    // decimal
    StructField decimalField = soi.getStructFieldRef("decimal_field");
    Assert.assertEquals(4, decimalField.getFieldID());
    Assert.assertEquals("decimal_field", decimalField.getFieldName());
    Assert.assertEquals("decimal comment", decimalField.getFieldComment());
    Assert.assertEquals(IcebergDecimalObjectInspector.get(38, 18), decimalField.getFieldObjectInspector());

    // double
    StructField doubleField = soi.getStructFieldRef("double_field");
    Assert.assertEquals(5, doubleField.getFieldID());
    Assert.assertEquals("double_field", doubleField.getFieldName());
    Assert.assertEquals("double comment", doubleField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(double.class), doubleField.getFieldObjectInspector());

    // fixed
    StructField fixedField = soi.getStructFieldRef("fixed_field");
    Assert.assertEquals(6, fixedField.getFieldID());
    Assert.assertEquals("fixed_field", fixedField.getFieldName());
    Assert.assertEquals("fixed comment", fixedField.getFieldComment());
    Assert.assertEquals(IcebergBinaryObjectInspector.byteArray(), fixedField.getFieldObjectInspector());

    // float
    StructField floatField = soi.getStructFieldRef("float_field");
    Assert.assertEquals(7, floatField.getFieldID());
    Assert.assertEquals("float_field", floatField.getFieldName());
    Assert.assertEquals("float comment", floatField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(float.class), floatField.getFieldObjectInspector());

    // integer
    StructField integerField = soi.getStructFieldRef("integer_field");
    Assert.assertEquals(8, integerField.getFieldID());
    Assert.assertEquals("integer_field", integerField.getFieldName());
    Assert.assertEquals("integer comment", integerField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(int.class), integerField.getFieldObjectInspector());

    // long
    StructField longField = soi.getStructFieldRef("long_field");
    Assert.assertEquals(9, longField.getFieldID());
    Assert.assertEquals("long_field", longField.getFieldName());
    Assert.assertEquals("long comment", longField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(long.class), longField.getFieldObjectInspector());

    // string
    StructField stringField = soi.getStructFieldRef("string_field");
    Assert.assertEquals(10, stringField.getFieldID());
    Assert.assertEquals("string_field", stringField.getFieldName());
    Assert.assertEquals("string comment", stringField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(String.class), stringField.getFieldObjectInspector());

    // timestamp without tz
    StructField timestampField = soi.getStructFieldRef("timestamp_field");
    Assert.assertEquals(11, timestampField.getFieldID());
    Assert.assertEquals("timestamp_field", timestampField.getFieldName());
    Assert.assertEquals("timestamp comment", timestampField.getFieldComment());
    if (MetastoreUtil.hive3PresentOnClasspath()) {
      Assert.assertTrue(timestampField.getFieldObjectInspector().getClass().getName()
              .startsWith("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspectorHive3"));
    } else {
      Assert.assertEquals(IcebergTimestampObjectInspector.get(false), timestampField.getFieldObjectInspector());
    }

    // timestamp with tz
    StructField timestampTzField = soi.getStructFieldRef("timestamptz_field");
    Assert.assertEquals(12, timestampTzField.getFieldID());
    Assert.assertEquals("timestamptz_field", timestampTzField.getFieldName());
    Assert.assertEquals("timestamptz comment", timestampTzField.getFieldComment());
    if (MetastoreUtil.hive3PresentOnClasspath()) {
      Assert.assertTrue(timestampTzField.getFieldObjectInspector().getClass().getName()
              .startsWith("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspectorHive3"));
    } else {
      Assert.assertEquals(IcebergTimestampObjectInspector.get(true), timestampTzField.getFieldObjectInspector());
    }

    // UUID
    StructField uuidField = soi.getStructFieldRef("uuid_field");
    Assert.assertEquals(13, uuidField.getFieldID());
    Assert.assertEquals("uuid_field", uuidField.getFieldName());
    Assert.assertEquals("uuid comment", uuidField.getFieldComment());
    Assert.assertEquals(getPrimitiveObjectInspector(String.class), uuidField.getFieldObjectInspector());

    // list
    StructField listField = soi.getStructFieldRef("list_field");
    Assert.assertEquals(14, listField.getFieldID());
    Assert.assertEquals("list_field", listField.getFieldName());
    Assert.assertEquals("list comment", listField.getFieldComment());
    Assert.assertEquals(getListObjectInspector(String.class), listField.getFieldObjectInspector());

    // map
    StructField mapField = soi.getStructFieldRef("map_field");
    Assert.assertEquals(16, mapField.getFieldID());
    Assert.assertEquals("map_field", mapField.getFieldName());
    Assert.assertEquals("map comment", mapField.getFieldComment());
    Assert.assertEquals(getMapObjectInspector(String.class, int.class), mapField.getFieldObjectInspector());

    // struct
    StructField structField = soi.getStructFieldRef("struct_field");
    Assert.assertEquals(19, structField.getFieldID());
    Assert.assertEquals("struct_field", structField.getFieldName());
    Assert.assertEquals("struct comment", structField.getFieldComment());

    ObjectInspector expectedObjectInspector = new IcebergRecordObjectInspector(
            (Types.StructType) schema.findType(19), ImmutableList.of(getPrimitiveObjectInspector(String.class)));
    Assert.assertEquals(expectedObjectInspector, structField.getFieldObjectInspector());
  }

  @Test
  public void testIcebergObjectInspectorUnsupportedTypes() {
    AssertHelpers.assertThrows(
        "Hive does not support time type", IllegalArgumentException.class, "TIME type is not supported",
        () -> IcebergObjectInspector.create(required(1, "time_field", Types.TimeType.get())));
  }

  private static ObjectInspector getPrimitiveObjectInspector(Class<?> clazz) {
    PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(clazz);
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo);
  }

  private static ObjectInspector getListObjectInspector(Class<?> clazz) {
    return ObjectInspectorFactory.getStandardListObjectInspector(getPrimitiveObjectInspector(clazz));
  }

  private static ObjectInspector getMapObjectInspector(Class<?> keyClazz, Class<?> valueClazz) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(
            getPrimitiveObjectInspector(keyClazz), getPrimitiveObjectInspector(valueClazz));
  }

}
