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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.util.Pair;

class BaseUpdatePartitionSpec implements UpdatePartitionSpec {
  private final TableOperations ops;
  private final TableMetadata base;
  private final int formatVersion;
  private final PartitionSpec spec;
  private final Schema schema;
  private final Map<String, PartitionField> nameToField;
  private final Map<Pair<Integer, String>, PartitionField> transformToField;

  private final List<PartitionField> adds = Lists.newArrayList();
  private final Map<Integer, PartitionField> addedTimeFields = Maps.newHashMap();
  private final Map<Pair<Integer, String>, PartitionField> transformToAddedField = Maps.newHashMap();
  private final Map<String, PartitionField> nameToAddedField = Maps.newHashMap();
  private final Set<Object> deletes = Sets.newHashSet();
  private final Map<String, String> renames = Maps.newHashMap();

  private boolean caseSensitive;
  private int lastAssignedPartitionId;

  BaseUpdatePartitionSpec(TableOperations ops) {
    this.ops = ops;
    this.caseSensitive = true;
    this.base = ops.current();
    this.formatVersion = base.formatVersion();
    this.spec = base.spec();
    this.schema = spec.schema();
    this.nameToField = indexSpecByName(spec);
    this.transformToField = indexSpecByTransform(spec);
    this.lastAssignedPartitionId = base.lastAssignedPartitionId();

    spec.fields().stream()
        .filter(field -> field.transform() instanceof UnknownTransform)
        .findAny()
        .ifPresent(field -> {
          throw new IllegalArgumentException("Cannot update partition spec with unknown transform: " + field);
        });
  }

  /**
   * For testing only.
   */
  @VisibleForTesting
  BaseUpdatePartitionSpec(int formatVersion, PartitionSpec spec) {
    this(formatVersion, spec, spec.fields().stream().mapToInt(PartitionField::fieldId).max().orElse(999));
  }

  /**
   * For testing only.
   */
  @VisibleForTesting
  BaseUpdatePartitionSpec(int formatVersion, PartitionSpec spec, int lastAssignedPartitionId) {
    this.ops = null;
    this.base = null;
    this.formatVersion = formatVersion;
    this.caseSensitive = true;
    this.spec = spec;
    this.schema = spec.schema();
    this.nameToField = indexSpecByName(spec);
    this.transformToField = indexSpecByTransform(spec);
    this.lastAssignedPartitionId = lastAssignedPartitionId;
  }

  private int assignFieldId() {
    this.lastAssignedPartitionId += 1;
    return lastAssignedPartitionId;
  }

  @Override
  public UpdatePartitionSpec caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  @Override
  public BaseUpdatePartitionSpec addField(String sourceName) {
    return addField(Expressions.ref(sourceName));
  }

  @Override
  public BaseUpdatePartitionSpec addField(Term term) {
    return addField(null, term);
  }

  @Override
  public BaseUpdatePartitionSpec addField(String name, Term term) {
    PartitionField alreadyAdded = nameToAddedField.get(name);
    Preconditions.checkArgument(alreadyAdded == null, "Cannot add duplicate partition field: %s", alreadyAdded);

    Pair<Integer, Transform<?, ?>> sourceTransform = resolve(term);
    Pair<Integer, String> validationKey = Pair.of(sourceTransform.first(), sourceTransform.second().toString());

    PartitionField existing = transformToField.get(validationKey);
    Preconditions.checkArgument(existing == null,
        "Cannot add duplicate partition field %s=%s, conflicts with %s", name, term, existing);

    PartitionField added = transformToAddedField.get(validationKey);
    Preconditions.checkArgument(added == null,
        "Cannot add duplicate partition field %s=%s, already added: %s", name, term, added);

    PartitionField newField = new PartitionField(
        sourceTransform.first(), assignFieldId(), name, sourceTransform.second());
    checkForRedundantAddedPartitions(newField);

    transformToAddedField.put(validationKey, newField);
    if (name != null) {
      nameToAddedField.put(name, newField);
    }

    adds.add(newField);

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec removeField(String name) {
    PartitionField alreadyAdded = nameToAddedField.get(name);
    Preconditions.checkArgument(alreadyAdded == null, "Cannot delete newly added field: %s", alreadyAdded);

    Preconditions.checkArgument(renames.get(name) == null,
        "Cannot rename and delete partition field: %s", name);

    PartitionField field = nameToField.get(name);
    Preconditions.checkArgument(field != null,
        "Cannot find partition field to remove: %s", name);

    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec removeField(Term term) {
    Pair<Integer, Transform<?, ?>> sourceTransform = resolve(term);
    Pair<Integer, String> key = Pair.of(sourceTransform.first(), sourceTransform.second().toString());

    PartitionField added = transformToAddedField.get(key);
    Preconditions.checkArgument(added == null, "Cannot delete newly added field: %s", added);

    PartitionField field = transformToField.get(key);
    Preconditions.checkArgument(field != null,
        "Cannot find partition field to remove: %s", term);
    Preconditions.checkArgument(renames.get(field.name()) == null,
        "Cannot rename and delete partition field: %s", field.name());

    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec renameField(String name, String newName) {
    PartitionField added = nameToAddedField.get(name);
    Preconditions.checkArgument(added == null,
        "Cannot rename newly added partition field: %s", name);

    PartitionField field = nameToField.get(name);
    Preconditions.checkArgument(field != null,
        "Cannot find partition field to rename: %s", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot delete and rename partition field: %s", name);

    renames.put(name, newName);

    return this;
  }

  @Override
  public PartitionSpec apply() {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    for (PartitionField field : spec.fields()) {
      if (!deletes.contains(field.fieldId())) {
        String newName = renames.get(field.name());
        if (newName != null) {
          builder.add(field.sourceId(), field.fieldId(), newName, field.transform());
        } else {
          builder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
        }
      } else if (formatVersion < 2) {
        // field IDs were not required for v1 and were assigned sequentially in each partition spec starting at 1,000.
        // to maintain consistent field ids across partition specs in v1 tables, any partition field that is removed
        // must be replaced with a null transform. null values are always allowed in partition data.
        builder.add(field.sourceId(), field.fieldId(), field.name(), Transforms.alwaysNull());
      }
    }

    for (PartitionField newField : adds) {
      String partitionName;
      if (newField.name() != null) {
        partitionName = newField.name();
      } else {
        partitionName = PartitionSpecVisitor.visit(schema, newField, PartitionNameGenerator.INSTANCE);
      }

      builder.add(newField.sourceId(), newField.fieldId(), partitionName, newField.transform());
    }

    return builder.build();
  }

  @Override
  public void commit() {
    TableMetadata update = base.updatePartitionSpec(apply());
    ops.commit(base, update);
  }

  private Pair<Integer, Transform<?, ?>> resolve(Term term) {
    Preconditions.checkArgument(term instanceof UnboundTerm, "Term must be unbound");

    BoundTerm<?> boundTerm = ((UnboundTerm<?>) term).bind(schema.asStruct(), caseSensitive);
    int sourceId = boundTerm.ref().fieldId();
    Transform<?, ?> transform = toTransform(boundTerm);

    return Pair.of(sourceId, transform);
  }

  private Transform<?, ?> toTransform(BoundTerm<?> term) {
    if (term instanceof BoundReference) {
      return Transforms.identity(term.type());
    } else if (term instanceof BoundTransform) {
      return ((BoundTransform<?, ?>) term).transform();
    } else {
      throw new ValidationException("Invalid term: %s, expected either a bound reference or transform", term);
    }
  }

  private void checkForRedundantAddedPartitions(PartitionField field) {
    if (isTimeTransform(field)) {
      PartitionField timeField = addedTimeFields.get(field.sourceId());
      Preconditions.checkArgument(timeField == null,
          "Cannot add redundant partition field: %s conflicts with %s", timeField, field);
      addedTimeFields.put(field.sourceId(), field);
    }
  }

  private static Map<String, PartitionField> indexSpecByName(PartitionSpec spec) {
    ImmutableMap.Builder<String, PartitionField> builder = ImmutableMap.builder();
    List<PartitionField> fields = spec.fields();
    for (PartitionField field : fields) {
      builder.put(field.name(), field);
    }

    return builder.build();
  }

  private static Map<Pair<Integer, String>, PartitionField> indexSpecByTransform(PartitionSpec spec) {
    ImmutableMap.Builder<Pair<Integer, String>, PartitionField> builder = ImmutableMap.builder();
    List<PartitionField> fields = spec.fields();
    for (PartitionField field : fields) {
      if (!field.transform().equals(Transforms.alwaysNull())) {
        builder.put(Pair.of(field.sourceId(), field.transform().toString()), field);
      }
    }

    return builder.build();
  }

  private boolean isTimeTransform(PartitionField field) {
    return PartitionSpecVisitor.visit(schema, field, IsTimeTransform.INSTANCE);
  }

  private static class IsTimeTransform implements PartitionSpecVisitor<Boolean> {
    private static final IsTimeTransform INSTANCE = new IsTimeTransform();

    private IsTimeTransform() {
    }

    @Override
    public Boolean identity(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return false;
    }

    @Override
    public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
      return false;
    }

    @Override
    public Boolean year(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean month(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean day(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean hour(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
      return false;
    }
  }

  private static class PartitionNameGenerator implements PartitionSpecVisitor<String> {
    private static final PartitionNameGenerator INSTANCE = new PartitionNameGenerator();

    private PartitionNameGenerator() {
    }

    @Override
    public String identity(int fieldId, String sourceName, int sourceId) {
      return sourceName;
    }

    @Override
    public String bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return sourceName + "_bucket_" + numBuckets;
    }

    @Override
    public String truncate(int fieldId, String sourceName, int sourceId, int width) {
      return sourceName + "_trunc_" + width;
    }

    @Override
    public String year(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_year";
    }

    @Override
    public String month(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_month";
    }

    @Override
    public String day(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_day";
    }

    @Override
    public String hour(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_hour";
    }

    @Override
    public String alwaysNull(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_null";
    }
  }
}
