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

package org.apache.iceberg.mapping;

import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMappingUpdates extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMappingUpdates(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAddColumnMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data")),
        mapping.asMappedFields());

    table.updateSchema()
        .addColumn("ts", Types.TimestampType.withZone())
        .commit();

    NameMapping updated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "ts")),
        updated.asMappedFields());
  }

  @Test
  public void testAddNestedColumnMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data")),
        mapping.asMappedFields());

    table.updateSchema()
        .addColumn("point", Types.StructType.of(
            required(1, "x", Types.DoubleType.get()),
            required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "point", MappedFields.of(
                MappedField.of(4, "x"),
                MappedField.of(5, "y")
            ))),
        updated.asMappedFields());

    table.updateSchema()
        .addColumn("point", "z", Types.DoubleType.get())
        .commit();

    NameMapping pointUpdated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "point", MappedFields.of(
                MappedField.of(4, "x"),
                MappedField.of(5, "y"),
                MappedField.of(6, "z")
            ))),
        pointUpdated.asMappedFields());
  }

  @Test
  public void testRenameMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data")),
        mapping.asMappedFields());

    table.updateSchema()
        .renameColumn("id", "object_id")
        .commit();

    NameMapping updated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, ImmutableList.of("id", "object_id")),
            MappedField.of(2, "data")),
        updated.asMappedFields());
  }

  @Test
  public void testRenameNestedFieldMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table.updateSchema()
        .addColumn("point", Types.StructType.of(
            required(1, "x", Types.DoubleType.get()),
            required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "point", MappedFields.of(
                MappedField.of(4, "x"),
                MappedField.of(5, "y")
            ))),
        updated.asMappedFields());

    table.updateSchema()
        .renameColumn("point.x", "X")
        .renameColumn("point.y", "Y")
        .commit();

    NameMapping pointUpdated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "point", MappedFields.of(
                MappedField.of(4, ImmutableList.of("x", "X")),
                MappedField.of(5, ImmutableList.of("y", "Y"))
            ))),
        pointUpdated.asMappedFields());
  }


  @Test
  public void testRenameComplexFieldMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table.updateSchema()
        .addColumn("point", Types.StructType.of(
            required(1, "x", Types.DoubleType.get()),
            required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "point", MappedFields.of(
                MappedField.of(4, "x"),
                MappedField.of(5, "y")
            ))),
        updated.asMappedFields());

    table.updateSchema()
        .renameColumn("point", "p2")
        .commit();

    NameMapping pointUpdated = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    Assert.assertEquals(
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, ImmutableList.of("point", "p2"), MappedFields.of(
                MappedField.of(4, "x"),
                MappedField.of(5, "y")
            ))),
        pointUpdated.asMappedFields());
  }
}
