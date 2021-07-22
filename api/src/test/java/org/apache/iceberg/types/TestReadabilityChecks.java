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

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestReadabilityChecks {
  private static final Type.PrimitiveType[] PRIMITIVES = new Type.PrimitiveType[] {
      Types.BooleanType.get(),
      Types.IntegerType.get(),
      Types.LongType.get(),
      Types.FloatType.get(),
      Types.DoubleType.get(),
      Types.DateType.get(),
      Types.TimeType.get(),
      Types.TimestampType.withoutZone(),
      Types.TimestampType.withZone(),
      Types.StringType.get(),
      Types.UUIDType.get(),
      Types.FixedType.ofLength(3),
      Types.FixedType.ofLength(4),
      Types.BinaryType.get(),
      Types.DecimalType.of(9, 2),
      Types.DecimalType.of(11, 2),
      Types.DecimalType.of(9, 3)
  };

  @Test
  public void testPrimitiveTypes() {
    for (Type.PrimitiveType from : PRIMITIVES) {
      Schema fromSchema = new Schema(required(1, "from_field", from));
      for (Type.PrimitiveType to : PRIMITIVES) {
        List<String> errors = CheckCompatibility.writeCompatibilityErrors(
            new Schema(required(1, "to_field", to)), fromSchema);

        if (TypeUtil.isPromotionAllowed(from, to)) {
          Assert.assertEquals("Should produce 0 error messages", 0, errors.size());
        } else {
          Assert.assertEquals("Should produce 1 error message", 1, errors.size());

          Assert.assertTrue("Should complain that promotion is not allowed",
              errors.get(0).contains("cannot be promoted to"));
        }
      }

      testDisallowPrimitiveToStruct(from, fromSchema);
      testDisallowPrimitiveToList(from, fromSchema);
      testDisallowPrimitiveToMap(from, fromSchema);
    }
  }

  private void testDisallowPrimitiveToMap(PrimitiveType from, Schema fromSchema) {
    Schema mapSchema = new Schema(required(1, "map_field",
        Types.MapType.ofRequired(2, 3, Types.StringType.get(), from)));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(mapSchema, fromSchema);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that primitive to map is not allowed",
        errors.get(0).contains("cannot be read as a map"));
  }

  private void testDisallowPrimitiveToList(PrimitiveType from, Schema fromSchema) {

    Schema listSchema = new Schema(required(1, "list_field", Types.ListType.ofRequired(2, from)));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(listSchema, fromSchema);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());
    Assert.assertTrue("Should complain that primitive to list is not allowed",
        errors.get(0).contains("cannot be read as a list"));
  }

  private void testDisallowPrimitiveToStruct(PrimitiveType from, Schema fromSchema) {
    Schema structSchema = new Schema(required(1, "struct_field", Types.StructType.of(
        required(2, "from", from))
    ));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(structSchema, fromSchema);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());
    Assert.assertTrue("Should complain that primitive to struct is not allowed",
        errors.get(0).contains("cannot be read as a struct"));
  }

  @Test
  public void testRequiredSchemaField() {
    Schema write = new Schema(optional(1, "from_field", Types.IntegerType.get()));
    Schema read = new Schema(required(1, "to_field", Types.IntegerType.get()));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that a required column is optional",
        errors.get(0).contains("should be required, but is optional"));
  }

  @Test
  public void testMissingSchemaField() {
    Schema write = new Schema(required(0, "other_field", Types.IntegerType.get()));
    Schema read = new Schema(required(1, "to_field", Types.IntegerType.get()));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that a required column is missing",
        errors.get(0).contains("is required, but is missing"));
  }

  @Test
  public void testRequiredStructField() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        optional(1, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "to_field", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that a required field is optional",
        errors.get(0).contains("should be required, but is optional"));
  }

  @Test
  public void testMissingRequiredStructField() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        optional(2, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "to_field", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that a required field is missing",
        errors.get(0).contains("is required, but is missing"));
  }

  @Test
  public void testMissingOptionalStructField() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        required(2, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        optional(1, "to_field", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce no error messages", 0, errors.size());
  }

  @Test
  public void testIncompatibleStructField() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "to_field", Types.BooleanType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("cannot be promoted to boolean"));
  }

  @Test
  public void testIncompatibleStructAndPrimitive() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "from_field", Types.StringType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StringType.get()));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("struct cannot be read as a string"));
  }

  @Test
  public void testMultipleErrors() {
    // required field is optional and cannot be promoted to the read type
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        optional(1, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "to_field", Types.BooleanType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 2, errors.size());

    Assert.assertTrue("Should complain that a required field is optional",
        errors.get(0).contains("should be required, but is optional"));
    Assert.assertTrue("Should complain about incompatible types",
        errors.get(1).contains("cannot be promoted to boolean"));
  }

  @Test
  public void testRequiredMapValue() {
    Schema write = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.StringType.get(), Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "map_field", Types.MapType.ofRequired(
        1, 2, Types.StringType.get(), Types.IntegerType.get()
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that values are optional",
        errors.get(0).contains("values should be required, but are optional"));
  }

  @Test
  public void testIncompatibleMapKey() {
    Schema write = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.IntegerType.get(), Types.StringType.get()
    )));
    Schema read = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.BooleanType.get(), Types.StringType.get()
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("cannot be promoted to boolean"));
  }

  @Test
  public void testIncompatibleMapValue() {
    Schema write = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.StringType.get(), Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.StringType.get(), Types.BooleanType.get()
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("cannot be promoted to boolean"));
  }

  @Test
  public void testIncompatibleMapAndPrimitive() {
    Schema write = new Schema(required(0, "map_field", Types.MapType.ofOptional(
        1, 2, Types.StringType.get(), Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "map_field", Types.StringType.get()));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("map cannot be read as a string"));
  }

  @Test
  public void testRequiredListElement() {
    Schema write = new Schema(required(0, "list_field", Types.ListType.ofOptional(
        1, Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "list_field", Types.ListType.ofRequired(
        1, Types.IntegerType.get()
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain that elements are optional",
        errors.get(0).contains("elements should be required, but are optional"));
  }

  @Test
  public void testIncompatibleListElement() {
    Schema write = new Schema(required(0, "list_field", Types.ListType.ofOptional(
        1, Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "list_field", Types.ListType.ofOptional(
        1, Types.BooleanType.get()
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("cannot be promoted to boolean"));
  }

  @Test
  public void testIncompatibleListAndPrimitive() {
    Schema write = new Schema(required(0, "list_field", Types.ListType.ofOptional(
        1, Types.IntegerType.get()
    )));
    Schema read = new Schema(required(0, "list_field", Types.StringType.get()));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    Assert.assertTrue("Should complain about incompatible types",
        errors.get(0).contains("list cannot be read as a string"));
  }

  @Test
  public void testDifferentFieldOrdering() {
    // writes should not reorder fields
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
            required(1, "field_a", Types.IntegerType.get()),
            required(2, "field_b", Types.IntegerType.get())
    )));
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
            required(2, "field_b", Types.IntegerType.get()),
            required(1, "field_a", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write, false);
    Assert.assertEquals("Should produce 0 error message", 0, errors.size());
  }

  @Test
  public void testStructWriteReordering() {
    // writes should not reorder fields
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "field_a", Types.IntegerType.get()),
        required(2, "field_b", Types.IntegerType.get())
    )));
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        required(2, "field_b", Types.IntegerType.get()),
        required(1, "field_a", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.writeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce 1 error message", 1, errors.size());

    System.err.println(errors);
    Assert.assertTrue("Should complain about field_b before field_a",
        errors.get(0).contains("field_b is out of order, before field_a"));
  }

  @Test
  public void testStructReadReordering() {
    // reads should allow reordering
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "field_a", Types.IntegerType.get()),
        required(2, "field_b", Types.IntegerType.get())
    )));
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        required(2, "field_b", Types.IntegerType.get()),
        required(1, "field_a", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.readCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce no error messages", 0, errors.size());
  }

  @Test
  public void testCaseInsensitiveSchemaProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.required(2, "long", Types.FloatType.get())
            )
        ))
    );

    Assert.assertNotNull(schema.caseInsensitiveSelect("ID").findField(0));
    Assert.assertNotNull(schema.caseInsensitiveSelect("loCATIONs").findField(5));
    Assert.assertNotNull(schema.caseInsensitiveSelect("LoCaTiOnS.LaT").findField(1));
    Assert.assertNotNull(schema.caseInsensitiveSelect("locations.LONG").findField(2));
  }

  @Test
  public void testCheckNullabilityRequiredSchemaField() {
    Schema write = new Schema(optional(1, "from_field", Types.IntegerType.get()));
    Schema read = new Schema(required(1, "to_field", Types.IntegerType.get()));

    List<String> errors = CheckCompatibility.typeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce no error messages", 0, errors.size());
  }

  @Test
  public void testCheckNullabilityRequiredStructField() {
    Schema write = new Schema(required(0, "nested", Types.StructType.of(
        optional(1, "from_field", Types.IntegerType.get())
    )));
    Schema read = new Schema(required(0, "nested", Types.StructType.of(
        required(1, "to_field", Types.IntegerType.get())
    )));

    List<String> errors = CheckCompatibility.typeCompatibilityErrors(read, write);
    Assert.assertEquals("Should produce no error messages", 0, errors.size());
  }
}
