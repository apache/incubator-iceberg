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

package org.apache.iceberg.spark;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructField$;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSparkTableUtil {

  Schema icebergSchema = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Test
  public void testValidateSchema() {
    StructType sparkSchema =
        new StructType().add("c1", DataTypes.IntegerType, true)
            .add("c2", DataTypes.StringType, true)
            .add("c3", DataTypes.StringType, true);
    Assert.assertTrue(SparkTableUtil.validateSchema(sparkSchema, icebergSchema));
  }

  @Test
  public void testValidateSchemaForNestedSchemas() {
    Types.StructType structType = Types.StructType.of(
        optional(0, "c0", Types.IntegerType.get()),
        optional(1, "c1", Types.StringType.get()));
    Schema nestedIcebergSchema = new Schema(optional(3, "c3", structType),
        optional(4, "c4", Types.StringType.get()));

    StructField field1 = StructField$.MODULE$.apply("c0", DataTypes.IntegerType, true, Metadata.empty());
    StructField field2 = StructField$.MODULE$.apply("c1", DataTypes.StringType, true, Metadata.empty());
    List<StructField> fields = Lists.newArrayList(field1, field2);
    StructType nestedStruct = DataTypes.createStructType(fields);
    StructType sparkSchema =
        new StructType().add("c3", nestedStruct, true)
            .add("c4", DataTypes.StringType, true);
    Assert.assertTrue(SparkTableUtil.validateSchema(sparkSchema, nestedIcebergSchema));
  }

  @Test
  public void testColumnCountMismatch() {
    StructType sparkSchema =
        new StructType().add(StructField$.MODULE$.apply("c1", DataTypes.IntegerType, true, Metadata.empty()))
            .add(StructField$.MODULE$.apply("c3", DataTypes.StringType, true, Metadata.empty()));
    Assert.assertFalse(SparkTableUtil.validateSchema(sparkSchema, icebergSchema));
  }

  @Test
  public void testDatatypeMismatch() {
    StructType sparkSchema = new StructType().add("c1", DataTypes.IntegerType, true)
            .add("c2", DataTypes.IntegerType, true) // Different type
            .add("c3", DataTypes.StringType, true);
    Assert.assertFalse(SparkTableUtil.validateSchema(sparkSchema, icebergSchema));
  }

  @Test
  public void testColumnNameMismatch() {
    StructType sparkSchema = new StructType().add("c1", DataTypes.IntegerType, true)
            .add("c2", DataTypes.StringType, true)
            .add("c4", DataTypes.StringType, true); // different col name
    Assert.assertFalse(SparkTableUtil.validateSchema(sparkSchema, icebergSchema));
  }

  @Test
  public void testNullabilityMismatch() {
    StructType sparkSchema = new StructType().add("c1", DataTypes.IntegerType, false) // Nullability false
            .add("c2", DataTypes.StringType, true)
            .add("c4", DataTypes.StringType, true);
    Assert.assertFalse(SparkTableUtil.validateSchema(sparkSchema, icebergSchema));
  }
}
