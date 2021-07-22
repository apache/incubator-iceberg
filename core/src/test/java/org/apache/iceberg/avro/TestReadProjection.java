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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(String desc,
                                         Schema writeSchema,
                                         Schema readSchema,
                                         Record record) throws IOException;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testFullProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Record projected = writeAndRead("full_projection", schema, schema, record);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));

    int cmp = Comparators.charSequences()
        .compare("test", (CharSequence) projected.get("data"));
    Assert.assertTrue("Should contain the correct data value", cmp == 0);
  }

  @Test
  public void testReorderedFullProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema reordered = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    Assert.assertEquals("Should contain the correct 0 value", "test", projected.get(0).toString());
    Assert.assertEquals("Should contain the correct 1 value", 34L, projected.get(1));
  }

  @Test
  public void testReorderedProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema reordered = new Schema(
        Types.NestedField.optional(2, "missing_1", Types.StringType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "missing_2", Types.LongType.get())
    );

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    Assert.assertNull("Should contain the correct 0 value", projected.get(0));
    Assert.assertEquals("Should contain the correct 1 value", "test", projected.get(1).toString());
    Assert.assertNull("Should contain the correct 2 value", projected.get(2));
  }

  @Test
  public void testEmptyProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Record projected = writeAndRead("empty_projection", schema, schema.select(), record);

    Assert.assertNotNull("Should read a non-null record", projected);
    try {
      projected.get(0);
      Assert.fail("Should not retrieve value with ordinal 0");
    } catch (ArrayIndexOutOfBoundsException e) {
      // this is expected because there are no values
    }
  }

  @Test
  public void testBasicProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("basic_projection_id", writeSchema, idOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "data");
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));

    Schema dataOnly = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    AssertHelpers.assertEmptyAvroField(projected, "id");
    int cmp = Comparators.charSequences()
        .compare("test", (CharSequence) projected.get("data"));
    Assert.assertEquals("Should contain the correct data value", 0, cmp);
  }

  @Test
  public void testRename() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema readSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "renamed", Types.StringType.get())
    );

    Record projected = writeAndRead("project_and_rename", writeSchema, readSchema, record);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    int cmp = Comparators.charSequences()
        .compare("test", (CharSequence) projected.get("renamed"));
    Assert.assertEquals("Should contain the correct data/renamed value", 0, cmp);
  }

  @Test
  public void testNestedStructProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(
        AvroSchemaUtil.fromOption(record.getSchema().getField("location").schema()));
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "location");
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));

    Schema latOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get())
        ))
    );

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    Record projectedLocation = (Record) projected.get("location");
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project location", projected.get("location"));
    AssertHelpers.assertEmptyAvroField(projectedLocation, "long");
    Assert.assertEquals("Should project latitude",
        52.995143f, (float) projectedLocation.get("lat"), 0.000001f);

    Schema longOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.get("location");
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project location", projected.get("location"));
    AssertHelpers.assertEmptyAvroField(projectedLocation, "lat");
    Assert.assertEquals("Should project longitude",
        -1.539054f, (float) projectedLocation.get("long"), 0.000001f);

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.get("location");
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project location", projected.get("location"));
    Assert.assertEquals("Should project latitude",
        52.995143f, (float) projectedLocation.get("lat"), 0.000001f);
    Assert.assertEquals("Should project longitude",
        -1.539054f, (float) projectedLocation.get("long"), 0.000001f);
  }

  @Test
  public void testMapProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "properties",
            Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StringType.get()))
    );

    Map<String, String> properties = ImmutableMap.of("a", "A", "b", "B");

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("properties", properties);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    AssertHelpers.assertEmptyAvroField(projected, "properties");

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project entire map",
        properties, toStringMap((Map) projected.get("properties")));

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project entire map",
        properties, toStringMap((Map) projected.get("properties")));

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project entire map",
        properties, toStringMap((Map) projected.get("properties")));
  }

  private Map<String, ?> toStringMap(Map<?, ?> map) {
    Map<String, Object> stringMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getValue() instanceof CharSequence) {
        stringMap.put(entry.getKey().toString(), entry.getValue().toString());
      } else {
        stringMap.put(entry.getKey().toString(), entry.getValue());
      }
    }
    return stringMap;
  }

  @Test
  public void testMapOfStructsProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.required(2, "long", Types.FloatType.get())
            )
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record l1 = new Record(AvroSchemaUtil.fromOption(
        AvroSchemaUtil.fromOption(record.getSchema().getField("locations").schema())
            .getValueType()));
    l1.put("lat", 53.992811f);
    l1.put("long", -1.542616f);
    Record l2 = new Record(l1.getSchema());
    l2.put("lat", 52.995143f);
    l2.put("long", -1.539054f);
    record.put("locations", ImmutableMap.of("L1", l1, "L2", l2));

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    AssertHelpers.assertEmptyAvroField(projected, "locations");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project locations map",
        record.get("locations"), toStringMap((Map) projected.get("locations")));

    projected = writeAndRead("lat_only",
        writeSchema, writeSchema.select("locations.lat"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Map<String, ?> locations = toStringMap((Map) projected.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals("L1 should contain lat",
        53.992811f, (float) projectedL1.get("lat"), 0.000001);
    AssertHelpers.assertEmptyAvroField(projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals("L2 should contain lat",
        52.995143f, (float) projectedL2.get("lat"), 0.000001);
    AssertHelpers.assertEmptyAvroField(projectedL2, "y");

    projected = writeAndRead("long_only",
        writeSchema, writeSchema.select("locations.long"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    AssertHelpers.assertEmptyAvroField(projectedL1, "lat");
    Assert.assertEquals("L1 should contain long",
        -1.542616f, (float) projectedL1.get("long"), 0.000001);
    projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    AssertHelpers.assertEmptyAvroField(projectedL2, "lat");
    Assert.assertEquals("L2 should contain long",
        -1.539054f, (float) projectedL2.get("long"), 0.000001);

    Schema latitiudeRenamed = new Schema(
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "latitude", Types.FloatType.get())
            )
        ))
    );

    projected = writeAndRead("latitude_renamed", writeSchema, latitiudeRenamed, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals("L1 should contain latitude",
        53.992811f, (float) projectedL1.get("latitude"), 0.000001);
    AssertHelpers.assertEmptyAvroField(projectedL1, "lat");
    AssertHelpers.assertEmptyAvroField(projectedL1, "long");
    projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals("L2 should contain latitude",
        52.995143f, (float) projectedL2.get("latitude"), 0.000001);
    AssertHelpers.assertEmptyAvroField(projectedL2, "lat");
    AssertHelpers.assertEmptyAvroField(projectedL2, "long");
  }

  @Test
  public void testListProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(10, "values",
            Types.ListType.ofOptional(11, Types.LongType.get()))
    );

    List<Long> values = ImmutableList.of(56L, 57L, 58L);

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("values", values);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    AssertHelpers.assertEmptyAvroField(projected, "values");

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project entire list", values, projected.get("values"));

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project entire list", values, projected.get("values"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListOfStructsProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get()),
                Types.NestedField.optional(18, "y", Types.IntegerType.get())
            ))
        )
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record p1 = new Record(AvroSchemaUtil.fromOption(
        AvroSchemaUtil.fromOption(record.getSchema().getField("points").schema())
            .getElementType()));
    p1.put("x", 1);
    p1.put("y", 2);
    Record p2 = new Record(p1.getSchema());
    p2.put("x", 3);
    p2.put("y", null);
    record.put("points", ImmutableList.of(p1, p2));

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    AssertHelpers.assertEmptyAvroField(projected, "points");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertEquals("Should project points list",
        record.get("points"), projected.get("points"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    List<Record> points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    Record projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.get("x"));
    AssertHelpers.assertEmptyAvroField(projectedP1, "y");
    Record projectedP2 = points.get(1);
    Assert.assertEquals("Should project x", 3, (int) projectedP2.get("x"));
    AssertHelpers.assertEmptyAvroField(projectedP2, "y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    AssertHelpers.assertEmptyAvroField(projectedP1, "x");
    Assert.assertEquals("Should project y", 2, (int) projectedP1.get("y"));
    projectedP2 = points.get(1);
    AssertHelpers.assertEmptyAvroField(projectedP2, "x");
    Assert.assertEquals("Should project null y", null, projectedP2.get("y"));

    Schema yRenamed = new Schema(
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.optional(18, "z", Types.IntegerType.get())
            ))
        )
    );

    projected = writeAndRead("y_renamed", writeSchema, yRenamed, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    AssertHelpers.assertEmptyAvroField(projectedP1, "x");
    AssertHelpers.assertEmptyAvroField(projectedP1, "y");
    Assert.assertEquals("Should project z", 2, (int) projectedP1.get("z"));
    projectedP2 = points.get(1);
    AssertHelpers.assertEmptyAvroField(projectedP2, "x");
    AssertHelpers.assertEmptyAvroField(projectedP2, "y");
    Assert.assertNull("Should project null z", projectedP2.get("z"));
  }

  @Test
  public void testEmptyStructProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(
        AvroSchemaUtil.fromOption(record.getSchema().getField("location").schema()));
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(3, "location", Types.StructType.of())
    );

    Record projected = writeAndRead("empty_proj", writeSchema, emptyStruct, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Record result = (Record) projected.get("location");
    Assert.assertEquals("location should be in the 0th position", result, projected.get(0));
    Assert.assertNotNull("Should contain an empty record", result);
    AssertHelpers.assertEmptyAvroField(result, "lat");
    AssertHelpers.assertEmptyAvroField(result, "long");
  }

  @Test
  public void testEmptyStructRequiredProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.required(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(record.getSchema().getField("location").schema());
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(3, "location", Types.StructType.of())
    );

    Record projected = writeAndRead("empty_req_proj", writeSchema, emptyStruct, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Record result = (Record) projected.get("location");
    Assert.assertEquals("location should be in the 0th position", result, projected.get(0));
    Assert.assertNotNull("Should contain an empty record", result);
    AssertHelpers.assertEmptyAvroField(result, "lat");
    AssertHelpers.assertEmptyAvroField(result, "long");
  }

  @Test
  public void testRequiredEmptyStructInRequiredStruct() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.required(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get()),
            Types.NestedField.required(4, "empty", Types.StructType.of())
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(record.getSchema().getField("location").schema());
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.required(3, "location", Types.StructType.of(
            Types.NestedField.required(4, "empty", Types.StructType.of())
        ))
    );

    Record projected = writeAndRead("req_empty_req_proj", writeSchema, emptyStruct, record);
    Assert.assertEquals("Should project id", 34L, projected.get("id"));
    Record result = (Record) projected.get("location");
    Assert.assertEquals("location should be in the 1st position", result, projected.get(1));
    Assert.assertNotNull("Should contain an empty record", result);
    AssertHelpers.assertEmptyAvroField(result, "lat");
    AssertHelpers.assertEmptyAvroField(result, "long");
    Assert.assertNotNull("Should project empty", result.getSchema().getField("empty"));
    Assert.assertNotNull("Empty should not be null", result.get("empty"));
    Assert.assertEquals("Empty should be empty", 0,
        ((Record) result.get("empty")).getSchema().getFields().size());
  }

  @Test
  public void testEmptyNestedStructProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "outer", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.optional(2, "inner", Types.StructType.of(
                Types.NestedField.required(5, "lon", Types.FloatType.get())
                )
            )
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record outer = new Record(
        AvroSchemaUtil.fromOption(record.getSchema().getField("outer").schema()));
    Record inner = new Record(AvroSchemaUtil.fromOption(outer.getSchema().getField("inner").schema()));
    inner.put("lon", 32.14f);
    outer.put("lat", 52.995143f);
    outer.put("inner", inner);
    record.put("outer", outer);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(3, "outer", Types.StructType.of(
            Types.NestedField.required(2, "inner", Types.StructType.of())
        )));

    Record projected = writeAndRead("nested_empty_proj", writeSchema, emptyStruct, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Record outerResult = (Record) projected.get("outer");
    Assert.assertEquals("Outer should be in the 0th position", outerResult, projected.get(0));
    Assert.assertNotNull("Should contain the outer record", outerResult);
    AssertHelpers.assertEmptyAvroField(outerResult, "lat");
    Record innerResult = (Record) outerResult.get("inner");
    Assert.assertEquals("Inner should be in the 0th position", innerResult, outerResult.get(0));
    Assert.assertNotNull("Should contain the inner record", innerResult);
    AssertHelpers.assertEmptyAvroField(innerResult, "lon");
  }

  @Test
  public void testEmptyNestedStructRequiredProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.required(3, "outer", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "inner", Types.StructType.of(
                Types.NestedField.required(5, "lon", Types.FloatType.get())
                )
            )
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record outer = new Record(record.getSchema().getField("outer").schema());
    Record inner = new Record(outer.getSchema().getField("inner").schema());
    inner.put("lon", 32.14f);
    outer.put("lat", 52.995143f);
    outer.put("inner", inner);
    record.put("outer", outer);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(3, "outer", Types.StructType.of(
            Types.NestedField.required(2, "inner", Types.StructType.of())
        )));

    Record projected = writeAndRead("nested_empty_req_proj", writeSchema, emptyStruct, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Record outerResult = (Record) projected.get("outer");
    Assert.assertEquals("Outer should be in the 0th position", outerResult, projected.get(0));
    Assert.assertNotNull("Should contain the outer record", outerResult);
    AssertHelpers.assertEmptyAvroField(outerResult, "lat");
    Record innerResult = (Record) outerResult.get("inner");
    Assert.assertEquals("Inner should be in the 0th position", innerResult, outerResult.get(0));
    Assert.assertNotNull("Should contain the inner record", innerResult);
    AssertHelpers.assertEmptyAvroField(innerResult, "lon");
  }

  @Test
  public void testNonExistentProjection() throws Exception {
    Assume.assumeFalse("Bug in pruning code will make the names not match when name mapping applied",
        this.getClass().getName().equals(TestAvroNameMapping.class.getName()));
    // TODO Purning code keeps records whose subfields have changed even if those fields are not required,
    // this means BuildAvroProjection builds a r_Named "foo" because "location" is kept in the pruned schema
    // even though it should not be. Otherwise location would be missing and the foo field would be returned
    // "foo" since it is a subfield of required field being built rather than the field being built.
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(
        AvroSchemaUtil.fromOption(record.getSchema().getField("location").schema()));
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct = new Schema(
        Types.NestedField.required(3, "location", Types.StructType.of(
            Types.NestedField.optional(10000, "foo", Types.StructType.of(
                Types.NestedField.optional(10001, "bar", Types.IntegerType.get())
            ))
        ))
    );

    Record projected = writeAndRead("non_existant_proj", writeSchema, emptyStruct, record);
    AssertHelpers.assertEmptyAvroField(projected, "id");
    Record result = (Record) projected.get("location");
    Assert.assertEquals("location should be in the 0th position", result, projected.get(0));
    Assert.assertNotNull("Should contain an fake optional record", result);
    AssertHelpers.assertEmptyAvroField(result, "lat");
    AssertHelpers.assertEmptyAvroField(result, "long");
    Assert.assertNotNull("Schema should contain foo", result.getSchema().getField("foo"));
    Assert.assertNull("foo should be null since it is not present in the data", result.get("foo"));
    Assert.assertNotNull("Schema should contain foo.bar",
        AvroSchemaUtil.fromOption(result.getSchema().getField("foo").schema())
            .getField("bar"));
  }
}
