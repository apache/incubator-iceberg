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

package org.apache.iceberg.arrow;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class ArrowSchemaUtil {
  private static final String ORIGINAL_TYPE = "originalType";
  private static final String MAP_TYPE = "mapType";

  private ArrowSchemaUtil() {
  }

  /**
   * Convert Iceberg schema to Arrow Schema.
   *
   * @param schema iceberg schema
   * @return arrow schema
   */
  public static Schema convert(final org.apache.iceberg.Schema schema) {
    ImmutableList.Builder<Field> fields = ImmutableList.builder();

    for (NestedField f : schema.columns()) {
      fields.add(convert(f));
    }

    return new Schema(fields.build());
  }

  public static Field convert(final NestedField field) {
    final ArrowType arrowType;

    final List<Field> children = Lists.newArrayList();
    Map<String, String> metadata = null;

    switch (field.type().typeId()) {
      case BINARY:
        // Spark doesn't support BYTE(fixed_size) type, so cast it to VarBinary
      case FIXED:
        arrowType = ArrowType.Binary.INSTANCE;
        break;
      case BOOLEAN:
        arrowType = ArrowType.Bool.INSTANCE;
        break;
      case INTEGER:
        arrowType = new ArrowType.Int(Integer.SIZE, true /* signed */);
        break;
      case LONG:
        arrowType = new ArrowType.Int(Long.SIZE, true /* signed */);
        break;
      case FLOAT:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        break;
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) field.type();
        arrowType = new ArrowType.Decimal(decimalType.precision(), decimalType.scale());
        break;
      case STRING:
        arrowType = ArrowType.Utf8.INSTANCE;
        break;
      case TIME:
        arrowType = new ArrowType.Time(TimeUnit.MICROSECOND, Long.SIZE);
        break;
      case UUID:
        arrowType = new ArrowType.FixedSizeBinary(16);
        break;
      case TIMESTAMP:
        arrowType = new ArrowType.Timestamp(
            TimeUnit.MICROSECOND,
            ((Types.TimestampType) field.type()).shouldAdjustToUTC() ? "UTC" : null);
        break;
      case DATE:
        arrowType = new ArrowType.Date(DateUnit.DAY);
        break;
      case STRUCT:
        final StructType struct = field.type().asStructType();
        arrowType = ArrowType.Struct.INSTANCE;

        for (NestedField nested : struct.fields()) {
          children.add(convert(nested));
        }
        break;
      case LIST:
        final ListType listType = field.type().asListType();
        arrowType = ArrowType.List.INSTANCE;

        for (NestedField nested : listType.fields()) {
          children.add(convert(nested));
        }
        break;
      case MAP:
        metadata = ImmutableMap.of(ORIGINAL_TYPE, MAP_TYPE);
        final MapType mapType = field.type().asMapType();
        arrowType = new ArrowType.Map(false);
        List<Field> entryFields = Lists.transform(mapType.fields(), ArrowSchemaUtil::convert);
        Field entry = new Field("",
            new FieldType(field.isOptional(), arrowType, null), entryFields);
        children.add(entry);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + field);
    }

    return new Field(field.name(), new FieldType(field.isOptional(), arrowType, null, metadata), children);
  }
}
