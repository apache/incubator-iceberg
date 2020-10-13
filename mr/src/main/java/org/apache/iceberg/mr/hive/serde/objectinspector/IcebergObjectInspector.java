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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public final class IcebergObjectInspector extends TypeUtil.SchemaVisitor<ObjectInspector> {

  // get the correct inspectors depending on whether we're working with Hive2 or Hive3 dependencies
  // we need to do this because there is a breaking API change in Date/TimestampObjectInspector between Hive2 and Hive3
  private static final ObjectInspector DATE_INSPECTOR = DynMethods.builder("get")
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspectorHive3")
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspector")
      .buildStatic()
      .invoke();

  private static final ObjectInspector TIMESTAMP_INSPECTOR = DynMethods.builder("get")
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspectorHive3", boolean.class)
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspector", boolean.class)
      .buildStatic()
      .invoke(false);

  private static final ObjectInspector TIMESTAMP_INSPECTOR_WITH_TZ = DynMethods.builder("get")
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspectorHive3", boolean.class)
      .impl("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergTimestampObjectInspector", boolean.class)
      .buildStatic()
      .invoke(true);

  public static ObjectInspector create(@Nullable Schema schema) {
    if (schema == null) {
      return IcebergRecordObjectInspector.empty();
    }

    return TypeUtil.visit(schema, new IcebergObjectInspector());
  }

  public static ObjectInspector create(Types.NestedField... fields) {
    return create(new Schema(fields));
  }

  @Override
  public ObjectInspector field(Types.NestedField field, ObjectInspector fieldObjectInspector) {
    return fieldObjectInspector;
  }

  @Override
  public ObjectInspector list(Types.ListType listTypeInfo, ObjectInspector listObjectInspector) {
    return ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector);
  }

  @Override
  public ObjectInspector map(Types.MapType mapType,
                             ObjectInspector keyObjectInspector, ObjectInspector valueObjectInspector) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
  }

  @Override
  public ObjectInspector primitive(Type.PrimitiveType primitiveType) {
    final PrimitiveTypeInfo primitiveTypeInfo;

    switch (primitiveType.typeId()) {
      case BINARY:
        return IcebergBinaryObjectInspector.byteBuffer();
      case BOOLEAN:
        primitiveTypeInfo = TypeInfoFactory.booleanTypeInfo;
        break;
      case DATE:
        return DATE_INSPECTOR;
      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitiveType;
        return IcebergDecimalObjectInspector.get(type.precision(), type.scale());
      case DOUBLE:
        primitiveTypeInfo = TypeInfoFactory.doubleTypeInfo;
        break;
      case FIXED:
        return IcebergBinaryObjectInspector.byteArray();
      case FLOAT:
        primitiveTypeInfo = TypeInfoFactory.floatTypeInfo;
        break;
      case INTEGER:
        primitiveTypeInfo = TypeInfoFactory.intTypeInfo;
        break;
      case LONG:
        primitiveTypeInfo = TypeInfoFactory.longTypeInfo;
        break;
      case STRING:
      case UUID:
        primitiveTypeInfo = TypeInfoFactory.stringTypeInfo;
        break;
      case TIMESTAMP:
        boolean adjustToUTC = ((Types.TimestampType) primitiveType).shouldAdjustToUTC();
        return adjustToUTC ? TIMESTAMP_INSPECTOR_WITH_TZ : TIMESTAMP_INSPECTOR;

      case TIME:
      default:
        throw new IllegalArgumentException(primitiveType.typeId() + " type is not supported");
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveTypeInfo);
  }

  @Override
  public ObjectInspector schema(Schema schema, ObjectInspector structObjectInspector) {
    return structObjectInspector;
  }

  @Override
  public ObjectInspector struct(Types.StructType structType, List<ObjectInspector> fieldObjectInspectors) {
    return new IcebergRecordObjectInspector(structType, fieldObjectInspectors);
  }

}
