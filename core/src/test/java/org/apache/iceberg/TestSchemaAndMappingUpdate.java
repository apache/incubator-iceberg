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

import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSchemaAndMappingUpdate extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSchemaAndMappingUpdate(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAddPrimitiveColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    table.updateSchema()
        .addColumn("count", Types.LongType.get())
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    validateUnchanged(mapping, updated);

    MappedField newMapping = updated.find("count");
    Assert.assertNotNull("Mapping for new column should be added", newMapping);
    Assert.assertEquals("Mapping should use the assigned field ID",
        (Integer) table.schema().findField("count").fieldId(), updated.find("count").id());
    Assert.assertNull("Should not contain a nested mapping", updated.find("count").nestedMapping());
  }

  @Test
  public void testAddStructColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    table.updateSchema()
        .addColumn("location", Types.StructType.of(
            Types.NestedField.optional(1, "lat", Types.DoubleType.get()),
            Types.NestedField.optional(2, "long", Types.DoubleType.get())))
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    validateUnchanged(mapping, updated);

    MappedField newMapping = updated.find("location");
    Assert.assertNotNull("Mapping for new column should be added", newMapping);

    Assert.assertEquals("Mapping should use the assigned field ID",
        (Integer) table.schema().findField("location").fieldId(), updated.find("location").id());
    Assert.assertNotNull("Should contain a nested mapping", updated.find("location").nestedMapping());

    Assert.assertEquals("Mapping should use the assigned field ID",
        (Integer) table.schema().findField("location.lat").fieldId(), updated.find("location.lat").id());
    Assert.assertNull("Should not contain a nested mapping", updated.find("location.lat").nestedMapping());

    Assert.assertEquals("Mapping should use the assigned field ID",
        (Integer) table.schema().findField("location.long").fieldId(), updated.find("location.long").id());
    Assert.assertNull("Should not contain a nested mapping", updated.find("location.long").nestedMapping());
  }

  @Test
  public void testRenameColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    table.updateSchema()
        .renameColumn("id", "object_id")
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("object_id").fieldId();
    validateUnchanged(
        Iterables.filter(mapping.asMappedFields().fields(), field -> !Objects.equals(idColumnId, field.id())),
        updated);

    MappedField updatedMapping = updated.find(idColumnId);
    Assert.assertNotNull("Mapping for id column should exist", updatedMapping);
    Assert.assertEquals("Should add the new column name to the existing mapping",
        MappedField.of(idColumnId, ImmutableList.of("id", "object_id")),
        updatedMapping);
  }

  @Test
  public void testDeleteColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    table.updateSchema()
        .deleteColumn("id")
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    // should not change the mapping
    validateUnchanged(mapping, updated);
  }

  @Test
  public void testDeleteAndAddColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema()
        .deleteColumn("id")
        .commit();

    // add the same column name back to the table with a different field ID
    table.updateSchema()
        .addColumn("id", Types.StringType.get())
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(mapping.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    Assert.assertNotNull("Mapping for id column should exist", newMapping);
    Assert.assertEquals("Mapping should use the new field ID", (Integer) idColumnId, newMapping.id());
    Assert.assertNull("Should not contain a nested mapping", newMapping.nestedMapping());

    MappedField updatedMapping = updated.find(startIdColumnId);
    Assert.assertNotNull("Mapping for original id column should exist", updatedMapping);
    Assert.assertEquals("Mapping should use the original field ID", (Integer) startIdColumnId, updatedMapping.id());
    Assert.assertFalse("Should not use id as a name", updatedMapping.names().contains("id"));
    Assert.assertNull("Should not contain a nested mapping", updatedMapping.nestedMapping());
  }

  @Test
  public void testDeleteAndRenameColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema()
        .deleteColumn("id")
        .commit();

    // rename the data column to id
    table.updateSchema()
        .renameColumn("data", "id")
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(mapping.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    Assert.assertNotNull("Mapping for id column should exist", newMapping);
    Assert.assertEquals("Mapping should use the new field ID", (Integer) idColumnId, newMapping.id());
    Assert.assertEquals("Should have both names", Sets.newHashSet("id", "data"), newMapping.names());
    Assert.assertNull("Should not contain a nested mapping", newMapping.nestedMapping());

    MappedField updatedMapping = updated.find(startIdColumnId);
    Assert.assertNotNull("Mapping for original id column should exist", updatedMapping);
    Assert.assertEquals("Mapping should use the original field ID", (Integer) startIdColumnId, updatedMapping.id());
    Assert.assertFalse("Should not use id as a name", updatedMapping.names().contains("id"));
    Assert.assertNull("Should not contain a nested mapping", updatedMapping.nestedMapping());
  }

  @Test
  public void testRenameAndAddColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema()
        .renameColumn("id", "object_id")
        .commit();

    NameMapping afterRename = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    Assert.assertEquals("Renamed column should have both names",
        Sets.newHashSet("id", "object_id"), afterRename.find(startIdColumnId).names());

    // add a new column with the renamed column's old name
    // also, rename the original column again to ensure its names are handled correctly
    table.updateSchema()
        .renameColumn("object_id", "oid")
        .addColumn("id", Types.StringType.get())
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(afterRename.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    Assert.assertNotNull("Mapping for id column should exist", newMapping);
    Assert.assertEquals("Mapping should use the new field ID", (Integer) idColumnId, newMapping.id());
    Assert.assertNull("Should not contain a nested mapping", newMapping.nestedMapping());

    MappedField updatedMapping = updated.find(startIdColumnId);
    Assert.assertNotNull("Mapping for original id column should exist", updatedMapping);
    Assert.assertEquals("Mapping should use the original field ID", (Integer) startIdColumnId, updatedMapping.id());
    Assert.assertEquals("Should not use id as a name", Sets.newHashSet("object_id", "oid"), updatedMapping.names());
    Assert.assertNull("Should not contain a nested mapping", updatedMapping.nestedMapping());
  }

  @Test
  public void testRenameAndRenameColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema()
        .renameColumn("id", "object_id")
        .commit();

    NameMapping afterRename = NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    Assert.assertEquals("Renamed column should have both names",
        Sets.newHashSet("id", "object_id"), afterRename.find(startIdColumnId).names());

    // rename the data column to the renamed column's old name
    // also, rename the original column again to ensure its names are handled correctly
    table.updateSchema()
        .renameColumn("object_id", "oid")
        .renameColumn("data", "id")
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(afterRename.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    Assert.assertNotNull("Mapping for id column should exist", newMapping);
    Assert.assertEquals("Renamed column should have both names",
        Sets.newHashSet("id", "data"), newMapping.names());
    Assert.assertEquals("Mapping should use the new field ID", (Integer) idColumnId, newMapping.id());
    Assert.assertNull("Should not contain a nested mapping", newMapping.nestedMapping());

    MappedField updatedMapping = updated.find(startIdColumnId);
    Assert.assertNotNull("Mapping for original id column should exist", updatedMapping);
    Assert.assertEquals("Mapping should use the original field ID", (Integer) startIdColumnId, updatedMapping.id());
    Assert.assertEquals("Should not use id as a name", Sets.newHashSet("object_id", "oid"), updatedMapping.names());
    Assert.assertNull("Should not contain a nested mapping", updatedMapping.nestedMapping());
  }

  /**
   * Asserts that the fields in the original mapping are unchanged in the updated mapping.
   */
  private void validateUnchanged(NameMapping original, NameMapping updated) {
    MappedFields updatedFields = updated.asMappedFields();
    for (MappedField field : original.asMappedFields().fields()) {
      Assert.assertEquals("Existing fields should not change", field, updatedFields.field(field.id()));
    }
  }

  /**
   * Asserts that the fields in the original mapping are unchanged in the updated mapping.
   */
  private void validateUnchanged(Iterable<MappedField> fields, NameMapping updated) {
    MappedFields updatedFields = updated.asMappedFields();
    for (MappedField field : fields) {
      Assert.assertEquals("Existing fields should not change", field, updatedFields.field(field.id()));
    }
  }
}
