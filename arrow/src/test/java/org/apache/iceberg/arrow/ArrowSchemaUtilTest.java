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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.junit.Assert;
import org.junit.Test;

public class ArrowSchemaUtilTest {

  private static final String INTEGER_FIELD = "i";
  private static final String BOOLEAN_FIELD = "b";
  private static final String DOUBLE_FIELD = "d";
  private static final String STRING_FIELD = "s";
  private static final String DATE_FIELD = "d2";
  private static final String TIMESTAMP_FIELD = "ts";
  private static final String LONG_FIELD = "l";
  private static final String FLOAT_FIELD = "f";
  private static final String TIME_FIELD = "tt";
  private static final String FIXED_WIDTH_BINARY_FIELD = "fbt";
  private static final String BINARY_FIELD = "bt";
  private static final String DECIMAL_FIELD = "dt";
  private static final String STRUCT_FIELD = "st";
  private static final String LIST_FIELD = "lt";
  private static final String MAP_FIELD = "mt";
  private static final String UUID_FIELD = "uu";

  @Test
  public void convertPrimitive() {
    Schema iceberg = new Schema(
        Types.NestedField.optional(0, INTEGER_FIELD, IntegerType.get()),
        Types.NestedField.optional(1, BOOLEAN_FIELD, BooleanType.get()),
        Types.NestedField.required(2, DOUBLE_FIELD, DoubleType.get()),
        Types.NestedField.required(3, STRING_FIELD, StringType.get()),
        Types.NestedField.optional(4, DATE_FIELD, DateType.get()),
        Types.NestedField.optional(5, TIMESTAMP_FIELD, TimestampType.withZone()),
        Types.NestedField.optional(6, LONG_FIELD, LongType.get()),
        Types.NestedField.optional(7, FLOAT_FIELD, FloatType.get()),
        Types.NestedField.optional(8, TIME_FIELD, TimeType.get()),
        Types.NestedField.optional(9, BINARY_FIELD, Types.BinaryType.get()),
        Types.NestedField.optional(10, DECIMAL_FIELD, Types.DecimalType.of(1, 1)),
        Types.NestedField.optional(12, LIST_FIELD, Types.ListType.ofOptional(13, Types.IntegerType.get())),
        Types.NestedField.required(14, MAP_FIELD, Types.MapType.ofOptional(15, 16,
            StringType.get(), IntegerType.get())),
        Types.NestedField.optional(17, FIXED_WIDTH_BINARY_FIELD, Types.FixedType.ofLength(10)),
        Types.NestedField.optional(18, UUID_FIELD, Types.UUIDType.get()));

    org.apache.arrow.vector.types.pojo.Schema arrow = ArrowSchemaUtil.convert(iceberg);

    validate(iceberg, arrow);
  }

  @Test
  public void convertComplex() {
    Schema iceberg = new Schema(
        Types.NestedField.optional(0, "m", MapType.ofOptional(
            1, 2, StringType.get(),
            LongType.get())
        ),
        Types.NestedField.required(3, "m2", MapType.ofOptional(
            4, 5, StringType.get(),
            ListType.ofOptional(6, TimestampType.withoutZone()))
        )
    );
    org.apache.arrow.vector.types.pojo.Schema arrow = ArrowSchemaUtil.convert(iceberg);
    Assert.assertEquals(iceberg.columns().size(), arrow.getFields().size());
  }

  private void validate(Schema iceberg, org.apache.arrow.vector.types.pojo.Schema arrow) {
    Assert.assertEquals(iceberg.columns().size(), arrow.getFields().size());

    for (Types.NestedField nf : iceberg.columns()) {
      Field field = arrow.findField(nf.name());
      Assert.assertNotNull("Missing filed: " + nf, field);
      validate(nf.type(), field, nf.isOptional());
    }
  }

  private void validate(Type iceberg, Field field, boolean optional) {
    ArrowType arrowType = field.getType();
    Assert.assertEquals(optional, field.isNullable());
    switch (iceberg.typeId()) {
      case BOOLEAN:
        Assert.assertEquals(BOOLEAN_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Bool, arrowType.getTypeID());
        break;
      case INTEGER:
        Assert.assertEquals(INTEGER_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Int, arrowType.getTypeID());
        break;
      case LONG:
        Assert.assertEquals(LONG_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Int, arrowType.getTypeID());
        break;
      case FLOAT:
        Assert.assertEquals(FLOAT_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.FloatingPoint, arrowType.getTypeID());
        break;
      case DOUBLE:
        Assert.assertEquals(DOUBLE_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.FloatingPoint, arrowType.getTypeID());
        break;
      case DATE:
        Assert.assertEquals(DATE_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Date, arrowType.getTypeID());
        break;
      case TIME:
        Assert.assertEquals(TIME_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Time, arrowType.getTypeID());
        break;
      case TIMESTAMP:
        Assert.assertEquals(TIMESTAMP_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Timestamp, arrowType.getTypeID());
        break;
      case STRING:
        Assert.assertEquals(STRING_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Utf8, arrowType.getTypeID());
        break;
      case FIXED:
        Assert.assertEquals(FIXED_WIDTH_BINARY_FIELD, field.getName());
        Assert.assertEquals(ArrowType.Binary.TYPE_TYPE, arrowType.getTypeID());
        break;
      case BINARY:
        Assert.assertEquals(BINARY_FIELD, field.getName());
        Assert.assertEquals(ArrowType.Binary.TYPE_TYPE, arrowType.getTypeID());
        break;
      case DECIMAL:
        Assert.assertEquals(DECIMAL_FIELD, field.getName());
        Assert.assertEquals(ArrowType.Decimal.TYPE_TYPE, arrowType.getTypeID());
        break;
      case STRUCT:
        Assert.assertEquals(STRUCT_FIELD, field.getName());
        Assert.assertEquals(ArrowType.Struct.TYPE_TYPE, arrowType.getTypeID());
        break;
      case LIST:
        Assert.assertEquals(LIST_FIELD, field.getName());
        Assert.assertEquals(ArrowType.List.TYPE_TYPE, arrowType.getTypeID());
        break;
      case MAP:
        Assert.assertEquals(MAP_FIELD, field.getName());
        Assert.assertEquals(ArrowType.ArrowTypeID.Map, arrowType.getTypeID());
        break;
      case UUID:
        Assert.assertEquals(UUID_FIELD, field.getName());
        Assert.assertEquals(ArrowType.FixedSizeBinary.TYPE_TYPE, arrowType.getTypeID());
        break;
      default:
        throw new UnsupportedOperationException("Check not implemented for type: " + iceberg);
    }
  }
}
