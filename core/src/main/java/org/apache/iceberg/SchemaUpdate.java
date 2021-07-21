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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.schema.UnionByNameVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema evolution API implementation.
 */
class SchemaUpdate implements UpdateSchema {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUpdate.class);
  private static final int TABLE_ROOT_ID = -1;

  private final TableOperations ops;
  private final TableMetadata base;
  private final Schema schema;
  private final Map<Integer, Integer> idToParent;
  private final List<Integer> deletes = Lists.newArrayList();
  private final Map<Integer, Types.NestedField> updates = Maps.newHashMap();
  private final Multimap<Integer, Types.NestedField> adds =
      Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
  private final Map<String, Integer> addedNameToId = Maps.newHashMap();
  private final Multimap<Integer, Move> moves = Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
  private int lastColumnId;
  private boolean allowIncompatibleChanges = false;
  private Set<String> identifierFieldNames;

  SchemaUpdate(TableOperations ops) {
    this(ops, ops.current());
  }

  /**
   * For testing only.
   */
  SchemaUpdate(Schema schema, int lastColumnId) {
    this(null, null, schema, lastColumnId);
  }

  private SchemaUpdate(TableOperations ops, TableMetadata base) {
    this(ops, base, base.schema(), base.lastColumnId());
  }

  private SchemaUpdate(TableOperations ops, TableMetadata base, Schema schema, int lastColumnId) {
    this.ops = ops;
    this.base = base;
    this.schema = schema;
    this.lastColumnId = lastColumnId;
    this.idToParent = Maps.newHashMap(TypeUtil.indexParents(schema.asStruct()));
    this.identifierFieldNames = schema.identifierFieldNames();
  }

  @Override
  public SchemaUpdate allowIncompatibleChanges() {
    this.allowIncompatibleChanges = true;
    return this;
  }

  @Override
  public UpdateSchema addColumn(String name, Type type, String doc) {
    Preconditions.checkArgument(!name.contains("."),
        "Cannot add column with ambiguous name: %s, use addColumn(parent, name, type)", name);
    return addColumn(null, name, type, doc);
  }

  @Override
  public UpdateSchema addColumn(String parent, String name, Type type, String doc) {
    internalAddColumn(parent, name, true, type, doc);
    return this;
  }

  @Override
  public UpdateSchema addRequiredColumn(String name, Type type, String doc) {
    Preconditions.checkArgument(!name.contains("."),
        "Cannot add column with ambiguous name: %s, use addColumn(parent, name, type)", name);
    addRequiredColumn(null, name, type, doc);
    return this;
  }

  @Override
  public UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc) {
    Preconditions.checkArgument(allowIncompatibleChanges,
        "Incompatible change: cannot add required column: %s", name);
    internalAddColumn(parent, name, false, type, doc);
    return this;
  }

  private void internalAddColumn(String parent, String name, boolean isOptional, Type type, String doc) {
    int parentId = TABLE_ROOT_ID;
    String fullName;
    if (parent != null) {
      Types.NestedField parentField = schema.findField(parent);
      Preconditions.checkArgument(parentField != null, "Cannot find parent struct: %s", parent);
      Type parentType = parentField.type();
      if (parentType.isNestedType()) {
        Type.NestedType nested = parentType.asNestedType();
        if (nested.isMapType()) {
          // fields are added to the map value type
          parentField = nested.asMapType().fields().get(1);
        } else if (nested.isListType()) {
          // fields are added to the element type
          parentField = nested.asListType().fields().get(0);
        }
      }
      Preconditions.checkArgument(
          parentField.type().isNestedType() && parentField.type().asNestedType().isStructType(),
          "Cannot add to non-struct column: %s: %s", parent, parentField.type());
      parentId = parentField.fieldId();
      Types.NestedField currentField = schema.findField(parent + "." + name);
      Preconditions.checkArgument(!deletes.contains(parentId),
          "Cannot add to a column that will be deleted: %s", parent);
      Preconditions.checkArgument(currentField == null || deletes.contains(currentField.fieldId()),
          "Cannot add column, name already exists: %s.%s", parent, name);
      fullName = schema.findColumnName(parentId) + "." + name;
    } else {
      Types.NestedField currentField = schema.findField(name);
      Preconditions.checkArgument(currentField == null || deletes.contains(currentField.fieldId()),
          "Cannot add column, name already exists: %s", name);
      fullName = name;
    }

    // assign new IDs in order
    int newId = assignNewColumnId();

    // update tracking for moves
    addedNameToId.put(fullName, newId);
    if (parentId != TABLE_ROOT_ID) {
      idToParent.put(newId, parentId);
    }

    adds.put(parentId, Types.NestedField.of(newId, isOptional, name,
        TypeUtil.assignFreshIds(type, this::assignNewColumnId), doc));
  }

  @Override
  public UpdateSchema deleteColumn(String name) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot delete missing column: %s", name);
    Preconditions.checkArgument(!adds.containsKey(field.fieldId()),
        "Cannot delete a column that has additions: %s", name);
    Preconditions.checkArgument(!updates.containsKey(field.fieldId()),
        "Cannot delete a column that has updates: %s", name);
    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public UpdateSchema renameColumn(String name, String newName) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot rename missing column: %s", name);
    Preconditions.checkArgument(newName != null, "Cannot rename a column to null");
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot rename a column that will be deleted: %s", field.name());

    // merge with an update, if present
    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);
    if (update != null) {
      updates.put(fieldId, Types.NestedField.of(fieldId, update.isOptional(), newName, update.type(), update.doc()));
    } else {
      updates.put(fieldId, Types.NestedField.of(fieldId, field.isOptional(), newName, field.type(), field.doc()));
    }

    if (identifierFieldNames.contains(name)) {
      identifierFieldNames.remove(name);
      identifierFieldNames.add(newName);
    }

    return this;
  }

  @Override
  public UpdateSchema requireColumn(String name) {
    internalUpdateColumnRequirement(name, false);
    return this;
  }

  @Override
  public UpdateSchema makeColumnOptional(String name) {
    internalUpdateColumnRequirement(name, true);
    return this;
  }

  private void internalUpdateColumnRequirement(String name, boolean isOptional) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);

    if (!isOptional && field.isRequired() || isOptional && field.isOptional()) {
      // if the change is a noop, allow it even if allowIncompatibleChanges is false
      return;
    }

    Preconditions.checkArgument(isOptional || allowIncompatibleChanges,
        "Cannot change column nullability: %s: optional -> required", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot update a column that will be deleted: %s", field.name());

    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);

    if (update != null) {
      updates.put(fieldId, Types.NestedField.of(fieldId, isOptional, update.name(), update.type(), update.doc()));
    } else {
      updates.put(fieldId, Types.NestedField.of(fieldId, isOptional, field.name(), field.type(), field.doc()));
    }
  }

  @Override
  public UpdateSchema updateColumn(String name, Type.PrimitiveType newType) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot update a column that will be deleted: %s", field.name());

    if (field.type().equals(newType)) {
      return this;
    }

    Preconditions.checkArgument(TypeUtil.isPromotionAllowed(field.type(), newType),
        "Cannot change column type: %s: %s -> %s", name, field.type(), newType);

    // merge with a rename, if present
    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);
    if (update != null) {
      updates.put(fieldId, Types.NestedField.of(fieldId, update.isOptional(), update.name(), newType, update.doc()));
    } else {
      updates.put(fieldId, Types.NestedField.of(fieldId, field.isOptional(), field.name(), newType, field.doc()));
    }

    return this;
  }

  @Override
  public UpdateSchema updateColumnDoc(String name, String doc) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot update a column that will be deleted: %s", field.name());

    if (Objects.equals(field.doc(), doc)) {
      return this;
    }

    // merge with a rename or update, if present
    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);
    if (update != null) {
      updates.put(fieldId, Types.NestedField.of(fieldId, update.isOptional(), update.name(), update.type(), doc));
    } else {
      updates.put(fieldId, Types.NestedField.of(fieldId, field.isOptional(), field.name(), field.type(), doc));
    }

    return this;
  }

  @Override
  public UpdateSchema moveFirst(String name) {
    Integer fieldId = findForMove(name);
    Preconditions.checkArgument(fieldId != null, "Cannot move missing column: %s", name);
    internalMove(name, Move.first(fieldId));
    return this;
  }

  @Override
  public UpdateSchema moveBefore(String name, String beforeName) {
    Integer fieldId = findForMove(name);
    Preconditions.checkArgument(fieldId != null, "Cannot move missing column: %s", name);
    Integer beforeId = findForMove(beforeName);
    Preconditions.checkArgument(beforeId != null, "Cannot move %s before missing column: %s", name, beforeName);
    Preconditions.checkArgument(!fieldId.equals(beforeId), "Cannot move %s before itself", name);
    internalMove(name, Move.before(fieldId, beforeId));
    return this;
  }

  @Override
  public UpdateSchema moveAfter(String name, String afterName) {
    Integer fieldId = findForMove(name);
    Preconditions.checkArgument(fieldId != null, "Cannot move missing column: %s", name);
    Integer afterId = findForMove(afterName);
    Preconditions.checkArgument(afterId != null, "Cannot move %s after missing column: %s", name, afterName);
    Preconditions.checkArgument(!fieldId.equals(afterId), "Cannot move %s after itself", name);
    internalMove(name, Move.after(fieldId, afterId));
    return this;
  }

  @Override
  public UpdateSchema unionByNameWith(Schema newSchema) {
    UnionByNameVisitor.visit(this, schema, newSchema);
    return this;
  }

  @Override
  public UpdateSchema setIdentifierFields(Collection<String> names) {
    this.identifierFieldNames = Sets.newHashSet(names);
    return this;
  }

  private Integer findForMove(String name) {
    Types.NestedField field = schema.findField(name);
    if (field != null) {
      return field.fieldId();
    }
    return addedNameToId.get(name);
  }

  private void internalMove(String name, Move move) {
    Integer parentId = idToParent.get(move.fieldId());
    if (parentId != null) {
      Types.NestedField parent = schema.findField(parentId);
      Preconditions.checkArgument(parent.type().isStructType(),
          "Cannot move fields in non-struct type: %s", parent.type());

      if (move.type() == Move.MoveType.AFTER || move.type() == Move.MoveType.BEFORE) {
        Preconditions.checkArgument(
            parentId.equals(idToParent.get(move.referenceFieldId())),
            "Cannot move field %s to a different struct", name);
      }

      moves.put(parentId, move);
    } else {
      if (move.type() == Move.MoveType.AFTER || move.type() == Move.MoveType.BEFORE) {
        Preconditions.checkArgument(
            idToParent.get(move.referenceFieldId()) == null,
            "Cannot move field %s to a different struct", name);
      }

      moves.put(TABLE_ROOT_ID, move);
    }
  }

  /**
   * Apply the pending changes to the original schema and returns the result.
   * <p>
   * This does not result in a permanent update.
   *
   * @return the result Schema when all pending updates are applied
   */
  @Override
  public Schema apply() {
    Schema newSchema = applyChanges(schema, deletes, updates, adds, moves, identifierFieldNames);

    return newSchema;
  }

  @Override
  public void commit() {
    TableMetadata update = applyChangesToMetadata(base.updateSchema(apply(), lastColumnId));
    ops.commit(base, update);
  }

  private int assignNewColumnId() {
    int next = lastColumnId + 1;
    this.lastColumnId = next;
    return next;
  }

  private TableMetadata applyChangesToMetadata(TableMetadata metadata) {
    String mappingJson = metadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
    TableMetadata newMetadata = metadata;
    if (mappingJson != null) {
      try {
        // parse and update the mapping
        NameMapping mapping = NameMappingParser.fromJson(mappingJson);
        NameMapping updated = MappingUtil.update(mapping, updates, adds);

        // replace the table property
        Map<String, String> updatedProperties = Maps.newHashMap();
        updatedProperties.putAll(metadata.properties());
        updatedProperties.put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(updated));

        newMetadata = metadata.replaceProperties(updatedProperties);

      } catch (RuntimeException e) {
        // log the error, but do not fail the update
        LOG.warn("Failed to update external schema mapping: {}", mappingJson, e);
      }
    }

    // Transform the metrics if they exist
    if (base != null && base.properties() != null) {
      Schema newSchema = newMetadata.schema();
      List<String> deletedColumns = deletes.stream()
          .map(schema::findColumnName)
          .collect(Collectors.toList());
      Map<String, String> renamedColumns = updates.keySet().stream()
          .filter(id -> !schema.findColumnName(id).equals(newSchema.findColumnName(id)))
          .collect(Collectors.toMap(schema::findColumnName, newSchema::findColumnName));
      Map<String, String> updatedProperties = MetricsConfig.updateProperties(
          newMetadata.properties(), deletedColumns, renamedColumns);
      newMetadata = newMetadata.replaceProperties(updatedProperties);
    }

    return newMetadata;
  }

  private static Schema applyChanges(Schema schema, List<Integer> deletes,
                                     Map<Integer, Types.NestedField> updates,
                                     Multimap<Integer, Types.NestedField> adds,
                                     Multimap<Integer, Move> moves,
                                     Set<String> identifierFieldNames) {
    // validate existing identifier fields are not deleted
    for (String name : identifierFieldNames) {
      Types.NestedField field = schema.findField(name);
      if (field != null) {
        Preconditions.checkArgument(!deletes.contains(field.fieldId()),
            "Cannot delete identifier field %s. To force deletion, " +
                "also call setIdentifierFields to update identifier fields.", field);
      }
    }

    // apply schema changes
    Types.StructType struct = TypeUtil
        .visit(schema, new ApplyChanges(deletes, updates, adds, moves))
        .asNestedType().asStructType();

    // validate identifier requirements based on the latest schema
    Map<String, Integer> nameToId = TypeUtil.indexByName(struct);
    Set<Integer> freshIdentifierFieldIds = Sets.newHashSet();
    for (String name : identifierFieldNames) {
      Preconditions.checkArgument(nameToId.containsKey(name),
          "Cannot add field %s as an identifier field: not found in current schema or added columns", name);
      freshIdentifierFieldIds.add(nameToId.get(name));
    }

    Map<Integer, Integer> idToParent = TypeUtil.indexParents(schema.asStruct());
    Map<Integer, Types.NestedField> idToField = TypeUtil.indexById(struct);
    freshIdentifierFieldIds.forEach(id -> validateIdentifierField(id, idToField, idToParent));

    return new Schema(struct.fields(), freshIdentifierFieldIds);
  }

  private static void validateIdentifierField(int fieldId, Map<Integer, Types.NestedField> idToField,
                                              Map<Integer, Integer> idToParent) {
    Types.NestedField field = idToField.get(fieldId);
    Preconditions.checkArgument(field.type().isPrimitiveType(),
        "Cannot add field %s as an identifier field: not a primitive type field", field.name());
    Preconditions.checkArgument(field.isRequired(),
        "Cannot add field %s as an identifier field: not a required field", field.name());
    Preconditions.checkArgument(!Types.DoubleType.get().equals(field.type()) &&
        !Types.FloatType.get().equals(field.type()),
        "Cannot add field %s as an identifier field: must not be float or double field", field.name());

    // check whether the nested field is in a chain of struct fields
    Integer parentId = idToParent.get(field.fieldId());
    while (parentId != null) {
      Types.NestedField parent = idToField.get(parentId);
      Preconditions.checkArgument(parent.type().isStructType(),
          "Cannot add field %s as an identifier field: must not be nested in %s", field.name(), parent);
      parentId = idToParent.get(parent.fieldId());
    }
  }

  private static class ApplyChanges extends TypeUtil.SchemaVisitor<Type> {
    private final List<Integer> deletes;
    private final Map<Integer, Types.NestedField> updates;
    private final Multimap<Integer, Types.NestedField> adds;
    private final Multimap<Integer, Move> moves;

    private ApplyChanges(List<Integer> deletes,
                         Map<Integer, Types.NestedField> updates,
                         Multimap<Integer, Types.NestedField> adds,
                         Multimap<Integer, Move> moves) {
      this.deletes = deletes;
      this.updates = updates;
      this.adds = adds;
      this.moves = moves;
    }

    @Override
    public Type schema(Schema schema, Type structResult) {
      List<Types.NestedField> fields = addAndMoveFields(structResult.asStructType().fields(),
          adds.get(TABLE_ROOT_ID), moves.get(TABLE_ROOT_ID));

      if (fields != null) {
        return Types.StructType.of(fields);
      }

      return structResult;
    }

    @Override
    public Type struct(Types.StructType struct, List<Type> fieldResults) {
      boolean hasChange = false;
      List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Type resultType = fieldResults.get(i);
        if (resultType == null) {
          hasChange = true;
          continue;
        }

        Types.NestedField field = struct.fields().get(i);
        String name = field.name();
        String doc = field.doc();
        boolean isOptional = field.isOptional();
        Types.NestedField update = updates.get(field.fieldId());
        if (update != null) {
          name = update.name();
          doc = update.doc();
          isOptional = update.isOptional();
        }

        if (name.equals(field.name()) &&
            isOptional == field.isOptional() &&
            field.type() == resultType &&
            Objects.equals(doc, field.doc())) {
          newFields.add(field);
        } else {
          hasChange = true;
          newFields.add(Types.NestedField.of(field.fieldId(), isOptional, name, resultType, doc));
        }
      }

      if (hasChange) {
        // TODO: What happens if there are no fields left?
        return Types.StructType.of(newFields);
      }

      return struct;
    }

    @Override
    public Type field(Types.NestedField field, Type fieldResult) {
      // the API validates deletes, updates, and additions don't conflict
      // handle deletes
      int fieldId = field.fieldId();
      if (deletes.contains(fieldId)) {
        return null;
      }

      // handle updates
      Types.NestedField update = updates.get(field.fieldId());
      if (update != null && update.type() != field.type()) {
        // rename is handled in struct, but struct needs the correct type from the field result
        return update.type();
      }

      // handle adds
      Collection<Types.NestedField> newFields = adds.get(fieldId);
      Collection<Move> columnsToMove = moves.get(fieldId);
      if (!newFields.isEmpty() || !columnsToMove.isEmpty()) {
        // if either collection is non-null, then this must be a struct type. try to apply the changes
        List<Types.NestedField> fields = addAndMoveFields(
            fieldResult.asStructType().fields(), newFields, columnsToMove);
        if (fields != null) {
          return Types.StructType.of(fields);
        }
      }

      return fieldResult;
    }

    @Override
    public Type list(Types.ListType list, Type elementResult) {
      // use field to apply updates
      Types.NestedField elementField = list.fields().get(0);
      Type elementType = field(elementField, elementResult);
      if (elementType == null) {
        throw new IllegalArgumentException("Cannot delete element type from list: " + list);
      }

      Types.NestedField elementUpdate = updates.get(elementField.fieldId());
      boolean isElementOptional = elementUpdate != null ? elementUpdate.isOptional() : list.isElementOptional();

      if (isElementOptional == elementField.isOptional() && list.elementType() == elementType) {
        return list;
      }

      if (isElementOptional) {
        return Types.ListType.ofOptional(list.elementId(), elementType);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementType);
      }
    }

    @Override
    public Type map(Types.MapType map, Type kResult, Type valueResult) {
      // if any updates are intended for the key, throw an exception
      int keyId = map.fields().get(0).fieldId();
      if (deletes.contains(keyId)) {
        throw new IllegalArgumentException("Cannot delete map keys: " + map);
      } else if (updates.containsKey(keyId)) {
        throw new IllegalArgumentException("Cannot update map keys: " + map);
      } else if (adds.containsKey(keyId)) {
        throw new IllegalArgumentException("Cannot add fields to map keys: " + map);
      } else if (!map.keyType().equals(kResult)) {
        throw new IllegalArgumentException("Cannot alter map keys: " + map);
      }

      // use field to apply updates to the value
      Types.NestedField valueField = map.fields().get(1);
      Type valueType = field(valueField, valueResult);
      if (valueType == null) {
        throw new IllegalArgumentException("Cannot delete value type from map: " + map);
      }

      Types.NestedField valueUpdate = updates.get(valueField.fieldId());
      boolean isValueOptional = valueUpdate != null ? valueUpdate.isOptional() : map.isValueOptional();

      if (isValueOptional == map.isValueOptional() && map.valueType() == valueType) {
        return map;
      }

      if (isValueOptional) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueType);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueType);
      }
    }

    @Override
    public Type primitive(Type.PrimitiveType primitive) {
      return primitive;
    }
  }

  private static List<Types.NestedField> addAndMoveFields(List<Types.NestedField> fields,
                                                          Collection<Types.NestedField> adds,
                                                          Collection<Move> moves) {
    if (adds != null && !adds.isEmpty()) {
      if (moves != null && !moves.isEmpty()) {
        // always apply adds first so that added fields can be moved
        return moveFields(addFields(fields, adds), moves);
      } else {
        return addFields(fields, adds);
      }
    } else if (moves != null && !moves.isEmpty()) {
      return moveFields(fields, moves);
    }
    return null;
  }

  private static List<Types.NestedField> addFields(List<Types.NestedField> fields,
                                                   Collection<Types.NestedField> adds) {
    List<Types.NestedField> newFields = Lists.newArrayList(fields);
    newFields.addAll(adds);
    return newFields;
  }

  @SuppressWarnings({"checkstyle:IllegalType", "JdkObsolete"})
  private static List<Types.NestedField> moveFields(List<Types.NestedField> fields,
                                                    Collection<Move> moves) {
    LinkedList<Types.NestedField> reordered = Lists.newLinkedList(fields);

    for (Move move : moves) {
      Types.NestedField toMove = Iterables.find(reordered, field -> field.fieldId() == move.fieldId());
      reordered.remove(toMove);

      switch (move.type()) {
        case FIRST:
          reordered.addFirst(toMove);
          break;

        case BEFORE:
          Types.NestedField before = Iterables.find(reordered, field -> field.fieldId() == move.referenceFieldId());
          int beforeIndex = reordered.indexOf(before);
          // insert the new node at the index of the existing node
          reordered.add(beforeIndex, toMove);
          break;

        case AFTER:
          Types.NestedField after = Iterables.find(reordered, field -> field.fieldId() == move.referenceFieldId());
          int afterIndex = reordered.indexOf(after);
          reordered.add(afterIndex + 1, toMove);
          break;

        default:
          throw new UnsupportedOperationException("Unknown move type: " + move.type());
      }
    }

    return reordered;
  }

  /**
   * Represents a requested column move in a struct.
   */
  private static class Move {
    private enum MoveType {
      FIRST,
      BEFORE,
      AFTER
    }

    static Move first(int fieldId) {
      return new Move(fieldId, -1, MoveType.FIRST);
    }

    static Move before(int fieldId, int referenceFieldId) {
      return new Move(fieldId, referenceFieldId, MoveType.BEFORE);
    }

    static Move after(int fieldId, int referenceFieldId) {
      return new Move(fieldId, referenceFieldId, MoveType.AFTER);
    }

    private final int fieldId;
    private final int referenceFieldId;
    private final MoveType type;

    private Move(int fieldId, int referenceFieldId, MoveType type) {
      this.fieldId = fieldId;
      this.referenceFieldId = referenceFieldId;
      this.type = type;
    }

    public int fieldId() {
      return fieldId;
    }

    public int referenceFieldId() {
      return referenceFieldId;
    }

    public MoveType type() {
      return type;
    }
  }
}
