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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSchemaUpdate {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      optional(3, "preferences", Types.StructType.of(
          required(8, "feature1", Types.BooleanType.get()),
          optional(9, "feature2", Types.BooleanType.get())
      ), "struct of named boolean options"),
      required(4, "locations", Types.MapType.ofRequired(10, 11,
          Types.StructType.of(
              required(20, "address", Types.StringType.get()),
              required(21, "city", Types.StringType.get()),
              required(22, "state", Types.StringType.get()),
              required(23, "zip", Types.IntegerType.get())
          ),
          Types.StructType.of(
              required(12, "lat", Types.FloatType.get()),
              required(13, "long", Types.FloatType.get())
          )), "map of address to coordinate"),
      optional(5, "points", Types.ListType.ofOptional(14,
          Types.StructType.of(
              required(15, "x", Types.LongType.get()),
              required(16, "y", Types.LongType.get())
          )), "2-D cartesian points"),
      required(6, "doubles", Types.ListType.ofRequired(17,
          Types.DoubleType.get()
      )),
      optional(7, "properties", Types.MapType.ofOptional(18, 19,
          Types.StringType.get(),
          Types.StringType.get()
      ), "string map of properties")
  );

  private static final Set<Integer> ALL_IDS = ImmutableSet.copyOf(TypeUtil.getProjectedIds(SCHEMA));

  private static final int SCHEMA_LAST_COLUMN_ID = 23;

  @Test
  public void testNoChanges() {
    Schema identical = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).apply();
    Assert.assertEquals("Should not include any changes", SCHEMA.asStruct(), identical.asStruct());
  }

  @Test
  public void testDeleteFields() {
    // use schema projection to test column deletes
    List<String> columns = Lists.newArrayList("id", "data", "preferences", "preferences.feature1",
        "preferences.feature2", "locations", "locations.lat", "locations.long", "points",
        "points.x", "points.y", "doubles", "properties");
    for (String name : columns) {
      Set<Integer> selected = Sets.newHashSet(ALL_IDS);
      // remove the id and any nested fields from the projection
      Types.NestedField nested = SCHEMA.findField(name);
      selected.remove(nested.fieldId());
      selected.removeAll(TypeUtil.getProjectedIds(nested.type()));

      Schema del = new SchemaUpdate(SCHEMA, 19).deleteColumn(name).apply();

      Assert.assertEquals("Should match projection with '" + name + "' removed",
          TypeUtil.select(SCHEMA, selected).asStruct(), del.asStruct());
    }
  }

  @Test
  public void testUpdateTypes() {
    Types.StructType expected = Types.StructType.of(
        required(1, "id", Types.LongType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "feature2", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "lat", Types.DoubleType.get()),
                required(13, "long", Types.DoubleType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "x", Types.LongType.get()),
                required(16, "y", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );

    Schema updated = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .updateColumn("id", Types.LongType.get())
        .updateColumn("locations.lat", Types.DoubleType.get())
        .updateColumn("locations.long", Types.DoubleType.get())
        .apply();

    Assert.assertEquals("Should convert types", expected, updated.asStruct());
  }

  @Test
  public void testUpdateFailure() {
    Set<Pair<Type.PrimitiveType, Type.PrimitiveType>> allowedUpdates = Sets.newHashSet(
        // Integer Type
        Pair.of(Types.IntegerType.get(), Types.LongType.get()),
        Pair.of(Types.IntegerType.get(), Types.FloatType.get()),
        Pair.of(Types.IntegerType.get(), Types.DoubleType.get()),
        Pair.of(Types.IntegerType.get(), Types.DecimalType.of(9, 2)),
        Pair.of(Types.IntegerType.get(), Types.StringType.get()),
        // Long Type
        Pair.of(Types.LongType.get(), Types.FloatType.get()),
        Pair.of(Types.LongType.get(), Types.DoubleType.get()),
        Pair.of(Types.LongType.get(), Types.DecimalType.of(9, 2)),
        Pair.of(Types.LongType.get(), Types.StringType.get()),
        // Float Type
        Pair.of(Types.FloatType.get(), Types.DoubleType.get()),
        Pair.of(Types.LongType.get(), Types.DecimalType.of(9, 2)),
        Pair.of(Types.LongType.get(), Types.StringType.get()),
        // Double Type
        Pair.of(Types.DoubleType.get(), Types.DecimalType.of(9, 2)),
        Pair.of(Types.DoubleType.get(), Types.StringType.get()),
        // Decimal Type
        Pair.of(Types.DecimalType.of(9, 2), Types.StringType.get()),
        // String Type
        Pair.of(Types.StringType.get(), Types.DoubleType.get()),
        Pair.of(Types.StringType.get(), Types.DecimalType.of(9, 2)),
        // Time/Date/Timestamp Type
        Pair.of(Types.TimeType.get(), Types.StringType.get()),
        Pair.of(Types.DateType.get(), Types.StringType.get()),
        Pair.of(Types.TimestampType.withZone(), Types.StringType.get()),
        Pair.of(Types.TimestampType.withoutZone(), Types.StringType.get()),
        // DecimalType -> DecimalType
        Pair.of(Types.DecimalType.of(9, 2), Types.DecimalType.of(18, 2))
    );

    List<Type.PrimitiveType> primitives = Lists.newArrayList(
        Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
        Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimeType.get(),
        Types.TimestampType.withZone(), Types.TimestampType.withoutZone(),
        Types.StringType.get(), Types.UUIDType.get(), Types.BinaryType.get(),
        Types.FixedType.ofLength(3), Types.FixedType.ofLength(4),
        Types.DecimalType.of(9, 2), Types.DecimalType.of(9, 3),
        Types.DecimalType.of(18, 2)
    );

    for (Type.PrimitiveType fromType : primitives) {
      for (Type.PrimitiveType toType : primitives) {
        Schema fromSchema = new Schema(required(1, "col", fromType));

        if (fromType.equals(toType) ||
            allowedUpdates.contains(Pair.of(fromType, toType))) {
          Schema expected = new Schema(required(1, "col", toType));
          Schema result = new SchemaUpdate(fromSchema, 1).updateColumn("col", toType).apply();
          Assert.assertEquals("Should allow update", expected.asStruct(), result.asStruct());
          continue;
        }

        String typeChange = fromType.toString() + " -> " + toType.toString();
        if (!TypeUtil.isPromotionAllowed(fromType, toType)) {
          AssertHelpers.assertThrows("Should reject update: " + typeChange,
              IllegalArgumentException.class, "change column type: col: " + typeChange,
              () -> new SchemaUpdate(fromSchema, 1).updateColumn("col", toType));
        }
      }
    }
  }

  @Test
  public void testRename() {
    Types.StructType expected = Types.StructType.of(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "json", Types.StringType.get()),
        optional(3, "options", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "newfeature", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "latitude", Types.FloatType.get()),
                required(13, "long", Types.FloatType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "X", Types.LongType.get()),
                required(16, "y.y", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );

    Schema renamed = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .renameColumn("data", "json")
        .renameColumn("preferences", "options")
        .renameColumn("preferences.feature2", "newfeature") // inside a renamed column
        .renameColumn("locations.lat", "latitude")
        .renameColumn("points.x", "X")
        .renameColumn("points.y", "y.y") // has a '.' in the field name
        .apply();

    Assert.assertEquals("Should rename all fields", expected, renamed.asStruct());
  }

  @Test
  public void testAddFields() {
    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "feature2", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "lat", Types.FloatType.get()),
                required(13, "long", Types.FloatType.get()),
                optional(25, "alt", Types.FloatType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "x", Types.LongType.get()),
                required(16, "y", Types.LongType.get()),
                optional(26, "z", Types.LongType.get()),
                optional(27, "t.t", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties"),
        optional(24, "toplevel", Types.DecimalType.of(9, 2))
    );

    Schema added = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .addColumn("toplevel", Types.DecimalType.of(9, 2))
        .addColumn("locations", "alt", Types.FloatType.get()) // map of structs
        .addColumn("points", "z", Types.LongType.get()) // list of structs
        .addColumn("points", "t.t", Types.LongType.get()) // name with '.'
        .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), added.asStruct());
  }

  @Test
  public void testAddNestedStruct() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.StructType struct = Types.StructType.of(
        required(1, "lat", Types.IntegerType.get()), // conflicts with id
        optional(2, "long", Types.IntegerType.get())
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "location", Types.StructType.of(
            required(3, "lat", Types.IntegerType.get()),
            optional(4, "long", Types.IntegerType.get())
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("location", struct)
        .apply();

    Assert.assertEquals("Should add struct and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedMapOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.MapType map = Types.MapType.ofOptional(1, 2,
        Types.StructType.of(
            required(20, "address", Types.StringType.get()),
            required(21, "city", Types.StringType.get()),
            required(22, "state", Types.StringType.get()),
            required(23, "zip", Types.IntegerType.get())
        ),
        Types.StructType.of(
            required(9, "lat", Types.IntegerType.get()),
            optional(8, "long", Types.IntegerType.get())
        )
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "locations", Types.MapType.ofOptional(3, 4,
            Types.StructType.of(
                required(5, "address", Types.StringType.get()),
                required(6, "city", Types.StringType.get()),
                required(7, "state", Types.StringType.get()),
                required(8, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(9, "lat", Types.IntegerType.get()),
                optional(10, "long", Types.IntegerType.get())
            )
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("locations", map)
        .apply();

    Assert.assertEquals("Should add map and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedListOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.ListType list = Types.ListType.ofOptional(1,
        Types.StructType.of(
            required(9, "lat", Types.IntegerType.get()),
            optional(8, "long", Types.IntegerType.get())
        )
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "locations", Types.ListType.ofOptional(3,
            Types.StructType.of(
                required(4, "lat", Types.IntegerType.get()),
                optional(5, "long", Types.IntegerType.get())
            )
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("locations", list)
        .apply();

    Assert.assertEquals("Should add map and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddRequiredColumn() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should reject add required column if incompatible changes are not allowed",
        IllegalArgumentException.class, "Incompatible change: cannot add required column: data",
        () -> new SchemaUpdate(schema, 1).addRequiredColumn("data", Types.StringType.get()));

    Schema result = new SchemaUpdate(schema, 1)
        .allowIncompatibleChanges()
        .addRequiredColumn("data", Types.StringType.get())
        .apply();

    Assert.assertEquals("Should add required column", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testMakeColumnOptional() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(optional(1, "id", Types.IntegerType.get()));

    Schema result = new SchemaUpdate(schema, 1)
        .makeColumnOptional("id")
        .apply();

    Assert.assertEquals("Should update column to be optional", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testRequireColumn() {
    Schema schema = new Schema(optional(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(required(1, "id", Types.IntegerType.get()));

    AssertHelpers.assertThrows("Should reject change to required if incompatible changes are not allowed",
        IllegalArgumentException.class, "Cannot change column nullability: id: optional -> required",
        () -> new SchemaUpdate(schema, 1).requireColumn("id"));

    // required to required is not an incompatible change
    new SchemaUpdate(expected, 1).requireColumn("id").apply();

    Schema result = new SchemaUpdate(schema, 1)
        .allowIncompatibleChanges()
        .requireColumn("id")
        .apply();

    Assert.assertEquals("Should update column to be required", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testMixedChanges() {
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get(), "unique id"),
        required(2, "json", Types.StringType.get()),
        optional(3, "options", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "newfeature", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "latitude", Types.DoubleType.get(), "latitude"),
                optional(25, "alt", Types.FloatType.get()),
                required(28, "description", Types.StringType.get(), "Location description")
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                optional(15, "X", Types.LongType.get()),
                required(16, "y.y", Types.LongType.get()),
                optional(26, "z", Types.LongType.get()),
                optional(27, "t.t", Types.LongType.get(), "name with '.'")
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(24, "toplevel", Types.DecimalType.of(9, 2))
    );

    Schema updated = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .addColumn("toplevel", Types.DecimalType.of(9, 2))
        .addColumn("locations", "alt", Types.FloatType.get()) // map of structs
        .addColumn("points", "z", Types.LongType.get()) // list of structs
        .addColumn("points", "t.t", Types.LongType.get(), "name with '.'")
        .renameColumn("data", "json")
        .renameColumn("preferences", "options")
        .renameColumn("preferences.feature2", "newfeature") // inside a renamed column
        .renameColumn("locations.lat", "latitude")
        .renameColumn("points.x", "X")
        .renameColumn("points.y", "y.y") // has a '.' in the field name
        .updateColumn("id", Types.LongType.get(), "unique id")
        .updateColumn("locations.lat", Types.DoubleType.get()) // use the original name
        .updateColumnDoc("locations.lat", "latitude")
        .deleteColumn("locations.long")
        .deleteColumn("properties")
        .makeColumnOptional("points.x")
        .allowIncompatibleChanges()
        .requireColumn("data")
        .addRequiredColumn("locations", "description", Types.StringType.get(), "Location description")
        .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), updated.asStruct());
  }

  @Test
  public void testAmbiguousAdd() {
    // preferences.booleans could be top-level or a field of preferences
    AssertHelpers.assertThrows("Should reject ambiguous column name",
        IllegalArgumentException.class, "ambiguous name: preferences.booleans", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences.booleans", Types.BooleanType.get());
        }
    );
  }

  @Test
  public void testAddAlreadyExists() {
    AssertHelpers.assertThrows("Should reject column name that already exists",
        IllegalArgumentException.class, "already exists: preferences.feature1", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", "feature1", Types.BooleanType.get());
        }
    );
    AssertHelpers.assertThrows("Should reject column name that already exists",
        IllegalArgumentException.class, "already exists: preferences", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", Types.BooleanType.get());
        }
    );
  }

  @Test
  public void testDeleteThenAdd() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(optional(2, "id", Types.IntegerType.get()));

    Schema updated = new SchemaUpdate(schema, 1)
        .deleteColumn("id")
        .addColumn("id", optional(2, "id", Types.IntegerType.get()).type())
        .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), updated.asStruct());
  }

  @Test
  public void testDeleteThenAddNested() {
    Schema expectedNested = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            optional(9, "feature2", Types.BooleanType.get()),
            optional(24, "feature1", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                    required(20, "address", Types.StringType.get()),
                    required(21, "city", Types.StringType.get()),
                    required(22, "state", Types.StringType.get()),
                    required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                    required(12, "lat", Types.FloatType.get()),
                    required(13, "long", Types.FloatType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                    required(15, "x", Types.LongType.get()),
                    required(16, "y", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );

    Schema updatedNested = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .deleteColumn("preferences.feature1")
        .addColumn("preferences", "feature1", Types.BooleanType.get())
        .apply();

    Assert.assertEquals("Should match with added fields", expectedNested.asStruct(), updatedNested.asStruct());
  }

  @Test
  public void testDeleteMissingColumn() {
    AssertHelpers.assertThrows("Should reject delete missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("col");
        }
    );
  }

  @Test
  public void testAddDeleteConflict() {
    AssertHelpers.assertThrows("Should reject add then delete",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("col", Types.IntegerType.get()).deleteColumn("col");
        }
    );
    AssertHelpers.assertThrows("Should reject add then delete",
        IllegalArgumentException.class, "column that has additions: preferences", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", "feature3", Types.IntegerType.get())
              .deleteColumn("preferences");
        }
    );
  }

  @Test
  public void testRenameMissingColumn() {
    AssertHelpers.assertThrows("Should reject rename missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("col", "fail");
        }
    );
  }

  @Test
  public void testRenameDeleteConflict() {
    AssertHelpers.assertThrows("Should reject rename then delete",
        IllegalArgumentException.class, "column that has updates: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("id", "col").deleteColumn("id");
        }
    );
    AssertHelpers.assertThrows("Should reject rename then delete",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("id", "col").deleteColumn("col");
        }
    );
  }

  @Test
  public void testDeleteRenameConflict() {
    AssertHelpers.assertThrows("Should reject delete then rename",
        IllegalArgumentException.class, "column that will be deleted: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("id").renameColumn("id", "identifier");
        }
    );
  }

  @Test
  public void testUpdateMissingColumn() {
    AssertHelpers.assertThrows("Should reject rename missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.updateColumn("col", Types.DateType.get());
        }
    );
  }

  @Test
  public void testUpdateDeleteConflict() {
    AssertHelpers.assertThrows("Should reject update then delete",
        IllegalArgumentException.class, "column that has updates: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.updateColumn("id", Types.LongType.get()).deleteColumn("id");
        }
    );
  }

  @Test
  public void testDeleteUpdateConflict() {
    AssertHelpers.assertThrows("Should reject delete then update",
        IllegalArgumentException.class, "column that will be deleted: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("id").updateColumn("id", Types.LongType.get());
        }
    );
  }

  @Test
  public void testDeleteMapKey() {
    AssertHelpers.assertThrows("Should reject delete map key",
        IllegalArgumentException.class, "Cannot delete map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).deleteColumn("locations.key").apply();
        }
    );
  }

  @Test
  public void testAddFieldToMapKey() {
    AssertHelpers.assertThrows("Should reject add sub-field to map key",
        IllegalArgumentException.class, "Cannot add fields to map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
              .addColumn("locations.key", "address_line_2", Types.StringType.get()).apply();
        }
    );
  }

  @Test
  public void testAlterMapKey() {
    AssertHelpers.assertThrows("Should reject alter sub-field of map key",
        IllegalArgumentException.class, "Cannot alter map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
              .updateColumn("locations.key.zip", Types.LongType.get()).apply();
        }
    );
  }

  @Test
  public void testUpdateMapKey() {
    Schema schema = new Schema(required(1, "m", Types.MapType.ofOptional(2, 3,
        Types.IntegerType.get(), Types.DoubleType.get())));
    AssertHelpers.assertThrows("Should reject update map key",
        IllegalArgumentException.class, "Cannot update map keys", () -> {
          new SchemaUpdate(schema, 3).updateColumn("m.key", Types.LongType.get()).apply();
        }
    );
  }

  @Test
  public void testUpdateAddedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    AssertHelpers.assertThrows("Should reject add and update doc",
        IllegalArgumentException.class, "Cannot update missing column", () -> {
          new SchemaUpdate(schema, 3)
              .addColumn("value", Types.LongType.get())
              .updateColumnDoc("value", "a value")
              .apply();
        }
    );
  }

  @Test
  public void testUpdateDeletedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    AssertHelpers.assertThrows("Should reject add and update doc",
        IllegalArgumentException.class, "Cannot update a column that will be deleted", () -> {
          new SchemaUpdate(schema, 3)
              .deleteColumn("i")
              .updateColumnDoc("i", "a value")
              .apply();
        }
    );
  }

  @Test
  public void testMultipleMoves() {
    Schema schema = new Schema(
        required(1, "a", Types.IntegerType.get()),
        required(2, "b", Types.IntegerType.get()),
        required(3, "c", Types.IntegerType.get()),
        required(4, "d", Types.IntegerType.get()));

    Schema expected = new Schema(
        required(3, "c", Types.IntegerType.get()),
        required(2, "b", Types.IntegerType.get()),
        required(4, "d", Types.IntegerType.get()),
        required(1, "a", Types.IntegerType.get()));

    // moves are applied in order
    Schema actual = new SchemaUpdate(schema, 4)
        .moveFirst("d")
        .moveFirst("c")
        .moveAfter("b", "d")
        .moveBefore("d", "a")
        .apply();

    Assert.assertEquals("Schema should match", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnFirst() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(2, "data", Types.StringType.get()),
        required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2)
        .moveFirst("data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnBeforeFirst() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(2, "data", Types.StringType.get()),
        required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2)
        .moveBefore("data", "id")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnAfterLast() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(2, "data", Types.StringType.get()),
        required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2)
        .moveAfter("id", "data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnAfter() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()),
        optional(3, "ts", Types.TimestampType.withZone()));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(3, "ts", Types.TimestampType.withZone()),
        required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 3)
        .moveAfter("ts", "id")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnBefore() {
    Schema schema = new Schema(
        optional(3, "ts", Types.TimestampType.withZone()),
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(3, "ts", Types.TimestampType.withZone()),
        required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 3)
        .moveBefore("ts", "data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldFirst() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(4, "data", Types.StringType.get()),
            required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4)
        .moveFirst("struct.data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldBeforeFirst() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(4, "data", Types.StringType.get()),
            required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4)
        .moveBefore("struct.data", "struct.count")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldAfterLast() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(4, "data", Types.StringType.get()),
            required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4)
        .moveAfter("struct.count", "struct.data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldAfter() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()),
            optional(5, "ts", Types.TimestampType.withZone()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            optional(5, "ts", Types.TimestampType.withZone()),
            required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5)
        .moveAfter("struct.ts", "struct.count")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldBefore() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            optional(5, "ts", Types.TimestampType.withZone()),
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            optional(5, "ts", Types.TimestampType.withZone()),
            required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5)
        .moveBefore("struct.ts", "struct.data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveListElementField() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "list", Types.ListType.ofOptional(6, Types.StructType.of(
            optional(5, "ts", Types.TimestampType.withZone()),
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get())))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "list", Types.ListType.ofOptional(6, Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            optional(5, "ts", Types.TimestampType.withZone()),
            required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 6)
        .moveBefore("list.ts", "list.data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveMapValueStructField() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "map", Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StructType.of(
            optional(5, "ts", Types.TimestampType.withZone()),
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get())))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "map", Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            optional(5, "ts", Types.TimestampType.withZone()),
            required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 7)
        .moveBefore("map.ts", "map.data")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedTopLevelColumn() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(3, "ts", Types.TimestampType.withZone()),
        required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 2)
        .addColumn("ts", Types.TimestampType.withZone())
        .moveAfter("ts", "id")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedTopLevelColumnAfterAddedColumn() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(3, "ts", Types.TimestampType.withZone()),
        optional(4, "count", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 2)
        .addColumn("ts", Types.TimestampType.withZone())
        .addColumn("count", Types.LongType.get())
        .moveAfter("ts", "id")
        .moveAfter("count", "ts")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedNestedStructField() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            optional(5, "ts", Types.TimestampType.withZone()),
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 4)
        .addColumn("struct", "ts", Types.TimestampType.withZone())
        .moveBefore("struct.ts", "struct.count")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedNestedStructFieldBeforeAddedColumn() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            optional(6, "size", Types.LongType.get()),
            optional(5, "ts", Types.TimestampType.withZone()),
            required(3, "count", Types.LongType.get()),
            required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 4)
        .addColumn("struct", "ts", Types.TimestampType.withZone())
        .addColumn("struct", "size", Types.LongType.get())
        .moveBefore("struct.ts", "struct.count")
        .moveBefore("struct.size", "struct.ts")
        .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveSelfReferenceFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should fail move for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move id before itself", () ->
            new SchemaUpdate(schema, 2)
                .moveBefore("id", "id")
                .apply());

    AssertHelpers.assertThrows("Should fail move for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move id after itself", () ->
            new SchemaUpdate(schema, 2)
                .moveAfter("id", "id")
                .apply());
  }

  @Test
  public void testMoveMissingColumnFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should fail move for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveFirst("items")
                .apply());

    AssertHelpers.assertThrows("Should fail move for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveBefore("items", "id")
                .apply());

    AssertHelpers.assertThrows("Should fail move for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveAfter("items", "data")
                .apply());
  }

  @Test
  public void testMoveBeforeAddFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should fail move for a field that has not been added yet",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveFirst("ts")
                .addColumn("ts", Types.TimestampType.withZone())
                .apply());

    AssertHelpers.assertThrows("Should fail move for a field that has not been added yet",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveBefore("ts", "id")
                .addColumn("ts", Types.TimestampType.withZone())
                .apply());

    AssertHelpers.assertThrows("Should fail move for a field that has not been added yet",
        IllegalArgumentException.class, "Cannot move missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveAfter("ts", "data")
                .addColumn("ts", Types.TimestampType.withZone())
                .apply());
  }

  @Test
  public void testMoveMissingReferenceColumnFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should fail move before a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move id before missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveBefore("id", "items")
                .apply());

    AssertHelpers.assertThrows("Should fail move after for a field that is not in the schema",
        IllegalArgumentException.class, "Cannot move data after missing column", () ->
            new SchemaUpdate(schema, 2)
                .moveAfter("data", "items")
                .apply());
  }

  @Test
  public void testMovePrimitiveMapKeyFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()),
        optional(3, "map", Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StringType.get())));

    AssertHelpers.assertThrows("Should fail move for map key",
        IllegalArgumentException.class, "Cannot move fields in non-struct type", () ->
            new SchemaUpdate(schema, 5)
                .moveBefore("map.key", "map.value")
                .apply());
  }

  @Test
  public void testMovePrimitiveMapValueFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()),
        optional(3, "map", Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StructType.of())));

    AssertHelpers.assertThrows("Should fail move for map value",
        IllegalArgumentException.class, "Cannot move fields in non-struct type", () ->
            new SchemaUpdate(schema, 5)
                .moveBefore("map.value", "map.key")
                .apply());
  }

  @Test
  public void testMovePrimitiveListElementFails() {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "data", Types.StringType.get()),
        optional(3, "list", Types.ListType.ofRequired(4, Types.StringType.get())));

    AssertHelpers.assertThrows("Should fail move for list element",
        IllegalArgumentException.class, "Cannot move fields in non-struct type", () ->
            new SchemaUpdate(schema, 4)
                .moveBefore("list.element", "list")
                .apply());
  }

  @Test
  public void testMoveTopLevelBetweenStructsFails() {
    Schema schema = new Schema(
        required(1, "a", Types.IntegerType.get()),
        required(2, "b", Types.IntegerType.get()),
        required(3, "struct", Types.StructType.of(
            required(4, "x", Types.IntegerType.get()),
            required(5, "y", Types.IntegerType.get()))));

    AssertHelpers.assertThrows("Should fail move between separate structs",
        IllegalArgumentException.class, "Cannot move field a to a different struct", () ->
            new SchemaUpdate(schema, 5)
                .moveBefore("a", "struct.x")
                .apply());
  }

  @Test
  public void testMoveBetweenStructsFails() {
    Schema schema = new Schema(
        required(1, "s1", Types.StructType.of(
            required(3, "a", Types.IntegerType.get()),
            required(4, "b", Types.IntegerType.get()))),
        required(2, "s2", Types.StructType.of(
            required(5, "x", Types.IntegerType.get()),
            required(6, "y", Types.IntegerType.get()))));

    AssertHelpers.assertThrows("Should fail move between separate structs",
        IllegalArgumentException.class, "Cannot move field s2.x to a different struct", () ->
            new SchemaUpdate(schema, 6)
                .moveBefore("s2.x", "s1.a")
                .apply());
  }

  @Test
  public void testAddExistingIdentifierFields() {
    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("id")
        .apply();

    Assert.assertEquals("add an existing field as identifier field should succeed",
        Sets.newHashSet(newSchema.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddNewIdentifierFieldColumns() {
    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn("new_field", Types.StringType.get())
        .setIdentifierFields("id", "new_field")
        .apply();

    Assert.assertEquals("add column then set as identifier should succeed",
        Sets.newHashSet(newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .setIdentifierFields("id", "new_field")
        .addRequiredColumn("new_field", Types.StringType.get())
        .apply();

    Assert.assertEquals("set identifier then add column should succeed",
        Sets.newHashSet(newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddNestedIdentifierFieldColumns() {
    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("preferences.feature1")
        .apply();

    Assert.assertEquals("set existing nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("preferences.feature1").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn("new", Types.StructType.of(
            Types.NestedField.required(SCHEMA_LAST_COLUMN_ID + 1, "field", Types.StringType.get())
        ))
        .setIdentifierFields("new.field")
        .apply();

    Assert.assertEquals("set newly added nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("new.field").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn("new", Types.StructType.of(
            Types.NestedField.required(SCHEMA_LAST_COLUMN_ID + 1, "field", Types.StructType.of(
                Types.NestedField.required(SCHEMA_LAST_COLUMN_ID + 2, "nested", Types.StringType.get())))))
        .setIdentifierFields("new.field.nested")
        .apply();

    Assert.assertEquals("set newly added multi-layer nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("new.field.nested").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddDottedIdentifierFieldColumns() {
    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn(null, "dot.field", Types.StringType.get())
        .setIdentifierFields("id", "dot.field")
        .apply();

    Assert.assertEquals("add a field with dot as identifier should succeed",
        Sets.newHashSet(newSchema.findField("id").fieldId(), newSchema.findField("dot.field").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testRemoveIdentifierFields() {
    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn("new_field", Types.StringType.get())
        .addRequiredColumn("new_field2", Types.StringType.get())
        .setIdentifierFields("id", "new_field", "new_field2")
        .apply();

    newSchema = new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("new_field", "new_field2")
        .apply();

    Assert.assertEquals("remove an identifier field should succeed",
        Sets.newHashSet(newSchema.findField("new_field").fieldId(), newSchema.findField("new_field2").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields(Sets.newHashSet())
        .apply();

    Assert.assertEquals("remove all identifier fields should succeed",
        Sets.newHashSet(),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testSetIdentifierFieldsFails() {
    AssertHelpers.assertThrows("add a field with name not exist should fail",
        IllegalArgumentException.class,
        "not found in current schema or added columns",
        () -> new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("unknown")
            .apply());

    AssertHelpers.assertThrows("add a field of non-primitive type should fail",
        IllegalArgumentException.class,
        "not a primitive type field",
        () -> new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("locations")
            .apply());

    AssertHelpers.assertThrows("add an optional field should fail",
        IllegalArgumentException.class,
        "not a required field",
        () -> new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("data")
            .apply());

    AssertHelpers.assertThrows("add a map key nested field should fail",
        IllegalArgumentException.class,
        "must not be nested in " + SCHEMA.findField("locations"),
        () -> new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("locations.key.zip")
            .apply());

    AssertHelpers.assertThrows("add a nested field in list should fail",
        IllegalArgumentException.class,
        "must not be nested in " + SCHEMA.findField("points"),
        () -> new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("points.element.x")
            .apply());

    Schema newSchema = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .allowIncompatibleChanges()
        .addRequiredColumn("col_float", Types.FloatType.get())
        .addRequiredColumn("col_double", Types.DoubleType.get())
        .addRequiredColumn("new", Types.StructType.of(
            Types.NestedField.required(SCHEMA_LAST_COLUMN_ID + 3, "fields", Types.ListType.ofOptional(
                SCHEMA_LAST_COLUMN_ID + 4, Types.StructType.of(
                    Types.NestedField.required(SCHEMA_LAST_COLUMN_ID + 5, "nested", Types.StringType.get())
                ))
            )
        ))
        .addRequiredColumn("new_map", Types.MapType.ofRequired(SCHEMA_LAST_COLUMN_ID + 6, 11,
            Types.StructType.of(
                required(SCHEMA_LAST_COLUMN_ID + 7, "key_col", Types.StringType.get())
            ),
            Types.StructType.of(
                required(SCHEMA_LAST_COLUMN_ID + 8, "val_col", Types.StringType.get())
            )), "map of address to coordinate")
        .apply();

    int lastColId = SCHEMA_LAST_COLUMN_ID + 8;

    AssertHelpers.assertThrows("add a double field should fail",
        IllegalArgumentException.class,
        "must not be float or double field",
        () -> new SchemaUpdate(newSchema, lastColId)
            .setIdentifierFields("col_double")
            .apply());

    AssertHelpers.assertThrows("add a float field should fail",
        IllegalArgumentException.class,
        "must not be float or double field",
        () -> new SchemaUpdate(newSchema, lastColId)
            .setIdentifierFields("col_float")
            .apply());

    AssertHelpers.assertThrows("add a map value nested field should fail",
        IllegalArgumentException.class,
        "must not be nested in " + newSchema.findField("new_map"),
        () -> new SchemaUpdate(newSchema, lastColId)
            .setIdentifierFields("new_map.value.val_col")
            .apply());

    AssertHelpers.assertThrows("add a nested field in struct of a map should fail",
        IllegalArgumentException.class,
        "must not be nested in " + newSchema.findField("new.fields"),
        () -> new SchemaUpdate(newSchema, lastColId)
            .setIdentifierFields("new.fields.element.nested")
            .apply());
  }

  @Test
  public void testDeleteIdentifierFieldColumns() {
    Schema schemaWithIdentifierFields = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("id")
        .apply();

    Assert.assertEquals("delete column and then reset identifier field should succeed",
        Sets.newHashSet(),
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .deleteColumn("id").setIdentifierFields(Sets.newHashSet()).apply()
            .identifierFieldIds());

    Assert.assertEquals("delete reset identifier field and then delete column should succeed",
        Sets.newHashSet(),
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields(Sets.newHashSet()).deleteColumn("id").apply()
            .identifierFieldIds());
  }

  @Test
  public void testDeleteIdentifierFieldColumnsFails() {
    Schema schemaWithIdentifierFields = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("id")
        .apply();

    AssertHelpers.assertThrows("delete an identifier column without setting identifier fields should fail",
        IllegalArgumentException.class,
        "Cannot delete identifier field 1: id: required int. To force deletion, " +
            "also call setIdentifierFields to update identifier fields.",
        () -> new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID).deleteColumn("id").apply());
  }

  @Test
  public void testRenameIdentifierFields() {
    Schema schemaWithIdentifierFields = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("id")
        .apply();

    Schema newSchema = new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
        .renameColumn("id", "id2")
        .apply();

    Assert.assertEquals("rename should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testMoveIdentifierFields() {
    Schema schemaWithIdentifierFields = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .setIdentifierFields("id")
        .apply();

    Schema newSchema = new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
        .moveAfter("id", "locations")
        .apply();

    Assert.assertEquals("move after should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
        .moveBefore("id", "locations")
        .apply();

    Assert.assertEquals("move before should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema = new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
        .moveFirst("id")
        .apply();

    Assert.assertEquals("move first should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }
}
