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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Metadata for a table.
 */
public class TableMetadata implements Serializable {
  static final long INITIAL_SEQUENCE_NUMBER = 0;
  static final long INVALID_SEQUENCE_NUMBER = -1;
  static final int DEFAULT_TABLE_FORMAT_VERSION = 1;
  static final int SUPPORTED_TABLE_FORMAT_VERSION = 2;
  static final int INITIAL_SPEC_ID = 0;
  static final int INITIAL_SORT_ORDER_ID = 1;
  static final int INITIAL_SCHEMA_ID = 0;

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  /**
   * @deprecated will be removed in 0.9.0; use newTableMetadata(Schema, PartitionSpec, String, Map) instead.
   */
  @Deprecated
  public static TableMetadata newTableMetadata(TableOperations ops,
                                               Schema schema,
                                               PartitionSpec spec,
                                               String location,
                                               Map<String, String> properties) {
    return newTableMetadata(schema, spec, SortOrder.unsorted(), location, properties, DEFAULT_TABLE_FORMAT_VERSION);
  }

  public static TableMetadata newTableMetadata(Schema schema,
                                               PartitionSpec spec,
                                               SortOrder sortOrder,
                                               String location,
                                               Map<String, String> properties) {
    return newTableMetadata(schema, spec, sortOrder, location, properties, DEFAULT_TABLE_FORMAT_VERSION);
  }

  public static TableMetadata newTableMetadata(Schema schema,
                                               PartitionSpec spec,
                                               String location,
                                               Map<String, String> properties) {
    return newTableMetadata(schema, spec, SortOrder.unsorted(), location, properties, DEFAULT_TABLE_FORMAT_VERSION);
  }

  static TableMetadata newTableMetadata(Schema schema,
                                        PartitionSpec spec,
                                        SortOrder sortOrder,
                                        String location,
                                        Map<String, String> properties,
                                        int formatVersion) {
    // reassign all column ids to ensure consistency
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema = TypeUtil.assignFreshIds(INITIAL_SCHEMA_ID, schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(freshSchema)
        .withSpecId(INITIAL_SPEC_ID);
    for (PartitionField field : spec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.findColumnName(field.sourceId());
      // reassign all partition fields with fresh partition field Ids to ensure consistency
      specBuilder.add(
          freshSchema.findField(sourceName).fieldId(),
          field.name(),
          field.transform().toString());
    }
    PartitionSpec freshSpec = specBuilder.build();

    // rebuild the sort order using the new column ids
    int freshSortOrderId = sortOrder.isUnsorted() ? sortOrder.orderId() : INITIAL_SORT_ORDER_ID;
    SortOrder freshSortOrder = freshSortOrder(freshSortOrderId, freshSchema, sortOrder);

    // Validate the metrics configuration. Note: we only do this on new tables to we don't
    // break existing tables.
    MetricsConfig.fromProperties(properties).validateReferencedColumns(schema);

    return new TableMetadata(null, formatVersion, UUID.randomUUID().toString(), location,
        INITIAL_SEQUENCE_NUMBER, System.currentTimeMillis(),
        lastColumnId.get(), freshSchema.schemaId(), ImmutableList.of(freshSchema),
        freshSpec.specId(), ImmutableList.of(freshSpec), freshSpec.lastAssignedFieldId(),
        freshSortOrderId, ImmutableList.of(freshSortOrder),
        ImmutableMap.copyOf(properties), -1, ImmutableList.of(),
        ImmutableList.of(), ImmutableList.of());
  }

  public static class SnapshotLogEntry implements HistoryEntry {
    private final long timestampMillis;
    private final long snapshotId;

    SnapshotLogEntry(long timestampMillis, long snapshotId) {
      this.timestampMillis = timestampMillis;
      this.snapshotId = snapshotId;
    }

    @Override
    public long timestampMillis() {
      return timestampMillis;
    }

    @Override
    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof SnapshotLogEntry)) {
        return false;
      }
      SnapshotLogEntry that = (SnapshotLogEntry) other;
      return timestampMillis == that.timestampMillis && snapshotId == that.snapshotId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampMillis, snapshotId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("timestampMillis", timestampMillis)
          .add("snapshotId", snapshotId)
          .toString();
    }
  }

  public static class MetadataLogEntry {
    private final long timestampMillis;
    private final String file;

    MetadataLogEntry(long timestampMillis, String file) {
      this.timestampMillis = timestampMillis;
      this.file = file;
    }

    public long timestampMillis() {
      return timestampMillis;
    }

    public String file() {
      return file;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof MetadataLogEntry)) {
        return false;
      }
      MetadataLogEntry that = (MetadataLogEntry) other;
      return timestampMillis == that.timestampMillis &&
              java.util.Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampMillis, file);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("timestampMillis", timestampMillis)
          .add("file", file)
          .toString();
    }
  }

  private final transient InputFile file;

  // stored metadata
  private final String metadataFileLocation;
  private final int formatVersion;
  private final String uuid;
  private final String location;
  private final long lastSequenceNumber;
  private final long lastUpdatedMillis;
  private final int lastColumnId;
  private final int currentSchemaId;
  private final List<Schema> schemas;
  private final int defaultSpecId;
  private final List<PartitionSpec> specs;
  private final int lastAssignedPartitionId;
  private final int defaultSortOrderId;
  private final List<SortOrder> sortOrders;
  private final Map<String, String> properties;
  private final long currentSnapshotId;
  private final List<Snapshot> snapshots;
  private final Map<Long, Snapshot> snapshotsById;
  private final Map<Integer, Schema> schemasById;
  private final Map<Integer, PartitionSpec> specsById;
  private final Map<Integer, SortOrder> sortOrdersById;
  private final List<HistoryEntry> snapshotLog;
  private final List<MetadataLogEntry> previousFiles;

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  TableMetadata(InputFile file,
                int formatVersion,
                String uuid,
                String location,
                long lastSequenceNumber,
                long lastUpdatedMillis,
                int lastColumnId,
                int currentSchemaId,
                List<Schema> schemas,
                int defaultSpecId,
                List<PartitionSpec> specs,
                int lastAssignedPartitionId,
                int defaultSortOrderId,
                List<SortOrder> sortOrders,
                Map<String, String> properties,
                long currentSnapshotId,
                List<Snapshot> snapshots,
                List<HistoryEntry> snapshotLog,
                List<MetadataLogEntry> previousFiles) {
    Preconditions.checkArgument(specs != null && !specs.isEmpty(), "Partition specs cannot be null or empty");
    Preconditions.checkArgument(sortOrders != null && !sortOrders.isEmpty(), "Sort orders cannot be null or empty");
    Preconditions.checkArgument(formatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
        "Unsupported format version: v%s", formatVersion);
    Preconditions.checkArgument(formatVersion == 1 || uuid != null,
        "UUID is required in format v%s", formatVersion);
    Preconditions.checkArgument(formatVersion > 1 || lastSequenceNumber == 0,
        "Sequence number must be 0 in v1: %s", lastSequenceNumber);

    this.formatVersion = formatVersion;
    this.file = file;
    this.metadataFileLocation = file != null ? file.location() : null;
    this.uuid = uuid;
    this.location = location;
    this.lastSequenceNumber = lastSequenceNumber;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.lastColumnId = lastColumnId;
    this.currentSchemaId = currentSchemaId;
    this.schemas = schemas;
    this.specs = specs;
    this.defaultSpecId = defaultSpecId;
    this.lastAssignedPartitionId = lastAssignedPartitionId;
    this.defaultSortOrderId = defaultSortOrderId;
    this.sortOrders = sortOrders;
    this.properties = properties;
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = snapshots;
    this.snapshotLog = snapshotLog;
    this.previousFiles = previousFiles;

    this.snapshotsById = indexAndValidateSnapshots(snapshots, lastSequenceNumber);
    this.schemasById = indexSchemas();
    this.specsById = indexSpecs(specs);
    this.sortOrdersById = indexSortOrders(sortOrders);

    HistoryEntry last = null;
    for (HistoryEntry logEntry : snapshotLog) {
      if (last != null) {
        Preconditions.checkArgument(
            (logEntry.timestampMillis() - last.timestampMillis()) >= -ONE_MINUTE,
            "[BUG] Expected sorted snapshot log entries.");
      }
      last = logEntry;
    }
    if (last != null) {
      Preconditions.checkArgument(
          // commits can happen concurrently from different machines.
          // A tolerance helps us avoid failure for small clock skew
          lastUpdatedMillis - last.timestampMillis() >= -ONE_MINUTE,
          "Invalid update timestamp %s: before last snapshot log entry at %s",
          lastUpdatedMillis, last.timestampMillis());
    }

    MetadataLogEntry previous = null;
    for (MetadataLogEntry metadataEntry : previousFiles) {
      if (previous != null) {
        Preconditions.checkArgument(
            // commits can happen concurrently from different machines.
            // A tolerance helps us avoid failure for small clock skew
            (metadataEntry.timestampMillis() - previous.timestampMillis()) >= -ONE_MINUTE,
            "[BUG] Expected sorted previous metadata log entries.");
      }
      previous = metadataEntry;
    }
      // Make sure that this update's lastUpdatedMillis is > max(previousFile's timestamp)
    if (previous != null) {
      Preconditions.checkArgument(
          // commits can happen concurrently from different machines.
          // A tolerance helps us avoid failure for small clock skew
          lastUpdatedMillis - previous.timestampMillis >= -ONE_MINUTE,
          "Invalid update timestamp %s: before the latest metadata log entry timestamp %s",
          lastUpdatedMillis, previous.timestampMillis);
    }

    Preconditions.checkArgument(
        currentSnapshotId < 0 || snapshotsById.containsKey(currentSnapshotId),
        "Invalid table metadata: Cannot find current version");
  }

  public int formatVersion() {
    return formatVersion;
  }

  public String metadataFileLocation() {
    return metadataFileLocation;
  }

  public String uuid() {
    return uuid;
  }

  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  public long nextSequenceNumber() {
    return formatVersion > 1 ? lastSequenceNumber + 1 : INITIAL_SEQUENCE_NUMBER;
  }

  public long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  public int lastColumnId() {
    return lastColumnId;
  }

  public Schema schema() {
    return schemasById.get(currentSchemaId);
  }

  public List<Schema> schemas() {
    return schemas;
  }

  public Map<Integer, Schema> schemasById() {
    return schemasById;
  }

  public int currentSchemaId() {
    return currentSchemaId;
  }

  public PartitionSpec spec() {
    return specsById.get(defaultSpecId);
  }

  public PartitionSpec spec(int id) {
    return specsById.get(id);
  }

  public List<PartitionSpec> specs() {
    return specs;
  }

  public Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  int lastAssignedPartitionId() {
    return lastAssignedPartitionId;
  }

  public int defaultSpecId() {
    return defaultSpecId;
  }

  public int defaultSortOrderId() {
    return defaultSortOrderId;
  }

  public SortOrder sortOrder() {
    return sortOrdersById.get(defaultSortOrderId);
  }

  public List<SortOrder> sortOrders() {
    return sortOrders;
  }

  public Map<Integer, SortOrder> sortOrdersById() {
    return sortOrdersById;
  }

  public String location() {
    return location;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public String property(String property, String defaultValue) {
    return properties.getOrDefault(property, defaultValue);
  }

  public boolean propertyAsBoolean(String property, boolean defaultValue) {
    return PropertyUtil.propertyAsBoolean(properties, property, defaultValue);
  }

  public int propertyAsInt(String property, int defaultValue) {
    return PropertyUtil.propertyAsInt(properties, property, defaultValue);
  }

  public long propertyAsLong(String property, long defaultValue) {
    return PropertyUtil.propertyAsLong(properties, property, defaultValue);
  }

  public Snapshot snapshot(long snapshotId) {
    return snapshotsById.get(snapshotId);
  }

  public Snapshot currentSnapshot() {
    return snapshotsById.get(currentSnapshotId);
  }

  public List<Snapshot> snapshots() {
    return snapshots;
  }

  public List<HistoryEntry> snapshotLog() {
    return snapshotLog;
  }

  public List<MetadataLogEntry> previousFiles() {
    return previousFiles;
  }

  public TableMetadata withUUID() {
    if (uuid != null) {
      return this;
    } else {
      return new TableMetadata(null, formatVersion, UUID.randomUUID().toString(), location,
          lastSequenceNumber, lastUpdatedMillis, lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
          lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties,
          currentSnapshotId, snapshots, snapshotLog, addPreviousFile(file, lastUpdatedMillis));
    }
  }

  public TableMetadata updateSchema(Schema newSchema, int newLastColumnId) {
    PartitionSpec.checkCompatibility(spec(), newSchema);
    SortOrder.checkCompatibility(sortOrder(), newSchema);
    // rebuild all of the partition specs and sort orders for the new current schema
    List<PartitionSpec> updatedSpecs = Lists.transform(specs, spec -> updateSpecSchema(newSchema, spec));
    List<SortOrder> updatedSortOrders = Lists.transform(sortOrders, order -> updateSortOrderSchema(newSchema, order));

    int newSchemaId = reuseOrCreateNewSchemaId(newSchema);
    if (currentSchemaId == newSchemaId && newLastColumnId == lastColumnId) {
      // the new spec and last column Id is already current and no change is needed
      return this;
    }

    ImmutableList.Builder<Schema> builder = ImmutableList.<Schema>builder().addAll(schemas);
    if (!schemasById.containsKey(newSchemaId)) {
      builder.add(new Schema(newSchemaId, newSchema.columns(), newSchema.identifierFieldIds()));
    }

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), newLastColumnId,
        newSchemaId, builder.build(), defaultSpecId, updatedSpecs, lastAssignedPartitionId,
        defaultSortOrderId, updatedSortOrders, properties, currentSnapshotId, snapshots, snapshotLog,
        addPreviousFile(file, lastUpdatedMillis));
  }

  // The caller is responsible to pass a newPartitionSpec with correct partition field IDs
  public TableMetadata updatePartitionSpec(PartitionSpec newPartitionSpec) {
    Schema schema = schema();

    PartitionSpec.checkCompatibility(newPartitionSpec, schema);
    ValidationException.check(formatVersion > 1 || PartitionSpec.hasSequentialIds(newPartitionSpec),
        "Spec does not use sequential IDs that are required in v1: %s", newPartitionSpec);

    // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
    int newDefaultSpecId = INITIAL_SPEC_ID;
    for (PartitionSpec spec : specs) {
      if (newPartitionSpec.compatibleWith(spec)) {
        newDefaultSpecId = spec.specId();
        break;
      } else if (newDefaultSpecId <= spec.specId()) {
        newDefaultSpecId = spec.specId() + 1;
      }
    }

    if (defaultSpecId == newDefaultSpecId) {
      // the new spec is already current and no change is needed
      return this;
    }

    ImmutableList.Builder<PartitionSpec> builder = ImmutableList.<PartitionSpec>builder()
        .addAll(specs);
    if (!specsById.containsKey(newDefaultSpecId)) {
      // get a fresh spec to ensure the spec ID is set to the new default
      builder.add(freshSpec(newDefaultSpecId, schema, newPartitionSpec));
    }

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, newDefaultSpecId,
        builder.build(), Math.max(lastAssignedPartitionId, newPartitionSpec.lastAssignedFieldId()),
        defaultSortOrderId, sortOrders, properties,
        currentSnapshotId, snapshots, snapshotLog, addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata replaceSortOrder(SortOrder newOrder) {
    Schema schema = schema();
    SortOrder.checkCompatibility(newOrder, schema);

    // determine the next order id
    int newOrderId = INITIAL_SORT_ORDER_ID;
    for (SortOrder order : sortOrders) {
      if (order.sameOrder(newOrder)) {
        newOrderId = order.orderId();
        break;
      } else if (newOrderId <= order.orderId()) {
        newOrderId = order.orderId() + 1;
      }
    }

    if (newOrderId == defaultSortOrderId) {
      return this;
    }

    ImmutableList.Builder<SortOrder> builder = ImmutableList.builder();
    builder.addAll(sortOrders);

    if (!sortOrdersById.containsKey(newOrderId)) {
      if (newOrder.isUnsorted()) {
        newOrderId = SortOrder.unsorted().orderId();
        builder.add(SortOrder.unsorted());
      } else {
        // rebuild the sort order using new column ids
        builder.add(freshSortOrder(newOrderId, schema, newOrder));
      }
    }

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, newOrderId, builder.build(), properties, currentSnapshotId, snapshots, snapshotLog,
        addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata addStagedSnapshot(Snapshot snapshot) {
    ValidationException.check(formatVersion == 1 || snapshot.sequenceNumber() > lastSequenceNumber,
        "Cannot add snapshot with sequence number %s older than last sequence number %s",
        snapshot.sequenceNumber(), lastSequenceNumber);

    List<Snapshot> newSnapshots = ImmutableList.<Snapshot>builder()
        .addAll(snapshots)
        .add(snapshot)
        .build();

    return new TableMetadata(null, formatVersion, uuid, location,
        snapshot.sequenceNumber(), snapshot.timestampMillis(), lastColumnId,
        currentSchemaId, schemas, defaultSpecId, specs, lastAssignedPartitionId,
        defaultSortOrderId, sortOrders, properties, currentSnapshotId, newSnapshots, snapshotLog,
        addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata replaceCurrentSnapshot(Snapshot snapshot) {
    // there can be operations (viz. rollback, cherrypick) where an existing snapshot could be replacing current
    if (snapshotsById.containsKey(snapshot.snapshotId())) {
      return setCurrentSnapshotTo(snapshot);
    }

    ValidationException.check(formatVersion == 1 || snapshot.sequenceNumber() > lastSequenceNumber,
        "Cannot add snapshot with sequence number %s older than last sequence number %s",
        snapshot.sequenceNumber(), lastSequenceNumber);

    List<Snapshot> newSnapshots = ImmutableList.<Snapshot>builder()
        .addAll(snapshots)
        .add(snapshot)
        .build();
    List<HistoryEntry> newSnapshotLog = ImmutableList.<HistoryEntry>builder()
        .addAll(snapshotLog)
        .add(new SnapshotLogEntry(snapshot.timestampMillis(), snapshot.snapshotId()))
        .build();

    return new TableMetadata(null, formatVersion, uuid, location,
        snapshot.sequenceNumber(), snapshot.timestampMillis(), lastColumnId,
        currentSchemaId, schemas, defaultSpecId, specs, lastAssignedPartitionId,
        defaultSortOrderId, sortOrders, properties, snapshot.snapshotId(), newSnapshots, newSnapshotLog,
        addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata removeSnapshotsIf(Predicate<Snapshot> removeIf) {
    List<Snapshot> filtered = Lists.newArrayListWithExpectedSize(snapshots.size());
    for (Snapshot snapshot : snapshots) {
      // keep the current snapshot and any snapshots that do not match the removeIf condition
      if (snapshot.snapshotId() == currentSnapshotId || !removeIf.test(snapshot)) {
        filtered.add(snapshot);
      }
    }

    // update the snapshot log
    Set<Long> validIds = Sets.newHashSet(Iterables.transform(filtered, Snapshot::snapshotId));
    List<HistoryEntry> newSnapshotLog = Lists.newArrayList();
    for (HistoryEntry logEntry : snapshotLog) {
      if (validIds.contains(logEntry.snapshotId())) {
        // copy the log entries that are still valid
        newSnapshotLog.add(logEntry);
      } else {
        // any invalid entry causes the history before it to be removed. otherwise, there could be
        // history gaps that cause time-travel queries to produce incorrect results. for example,
        // if history is [(t1, s1), (t2, s2), (t3, s3)] and s2 is removed, the history cannot be
        // [(t1, s1), (t3, s3)] because it appears that s3 was current during the time between t2
        // and t3 when in fact s2 was the current snapshot.
        newSnapshotLog.clear();
      }
    }

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, currentSnapshotId, filtered,
        ImmutableList.copyOf(newSnapshotLog), addPreviousFile(file, lastUpdatedMillis));
  }

  private TableMetadata setCurrentSnapshotTo(Snapshot snapshot) {
    ValidationException.check(snapshotsById.containsKey(snapshot.snapshotId()),
        "Cannot set current snapshot to unknown: %s", snapshot.snapshotId());
    ValidationException.check(formatVersion == 1 || snapshot.sequenceNumber() <= lastSequenceNumber,
        "Last sequence number %s is less than existing snapshot sequence number %s",
        lastSequenceNumber, snapshot.sequenceNumber());

    if (currentSnapshotId == snapshot.snapshotId()) {
      // change is a noop
      return this;
    }

    long nowMillis = System.currentTimeMillis();
    List<HistoryEntry> newSnapshotLog = ImmutableList.<HistoryEntry>builder()
        .addAll(snapshotLog)
        .add(new SnapshotLogEntry(nowMillis, snapshot.snapshotId()))
        .build();

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, nowMillis, lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, snapshot.snapshotId(), snapshots,
        newSnapshotLog, addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata replaceProperties(Map<String, String> newProperties) {
    ValidationException.check(newProperties != null, "Cannot set properties to null");
    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, newProperties, currentSnapshotId, snapshots,
        snapshotLog, addPreviousFile(file, lastUpdatedMillis, newProperties));
  }

  public TableMetadata removeSnapshotLogEntries(Set<Long> snapshotIds) {
    List<HistoryEntry> newSnapshotLog = Lists.newArrayList();
    for (HistoryEntry logEntry : snapshotLog) {
      if (!snapshotIds.contains(logEntry.snapshotId())) {
        // copy the log entries that are still valid
        newSnapshotLog.add(logEntry);
      }
    }

    ValidationException.check(currentSnapshotId < 0 || // not set
            Iterables.getLast(newSnapshotLog).snapshotId() == currentSnapshotId,
        "Cannot set invalid snapshot log: latest entry is not the current snapshot");

    return new TableMetadata(null, formatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, currentSnapshotId,
        snapshots, newSnapshotLog, addPreviousFile(file, lastUpdatedMillis));
  }

  private PartitionSpec reassignPartitionIds(PartitionSpec partitionSpec, TypeUtil.NextID nextID) {
    if (formatVersion > 1) {
      Map<Pair<Integer, String>, Integer> transformToFieldId = specs.stream()
          .flatMap(spec -> spec.fields().stream())
          .collect(Collectors.toMap(
              field -> Pair.of(field.sourceId(), field.transform().toString()),
              PartitionField::fieldId,
              (n1, n2) -> n2));

      PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(partitionSpec.schema())
          .withSpecId(partitionSpec.specId());

      for (PartitionField field : partitionSpec.fields()) {
        // reassign the partition field ids
        int fieldId = transformToFieldId.computeIfAbsent(
            Pair.of(field.sourceId(), field.transform().toString()), k -> nextID.get());
        specBuilder.add(
            field.sourceId(),
            fieldId,
            field.name(),
            field.transform());
      }
      return specBuilder.build();
    } else {
      // noop for v1 table
      return partitionSpec;
    }
  }

  // The caller is responsible to pass a updatedPartitionSpec with correct partition field IDs
  public TableMetadata buildReplacement(Schema updatedSchema, PartitionSpec updatedPartitionSpec,
                                        SortOrder updatedSortOrder, String newLocation,
                                        Map<String, String> updatedProperties) {
    ValidationException.check(formatVersion > 1 || PartitionSpec.hasSequentialIds(updatedPartitionSpec),
        "Spec does not use sequential IDs that are required in v1: %s", updatedPartitionSpec);

    AtomicInteger newLastColumnId = new AtomicInteger(lastColumnId);
    Schema freshSchema = TypeUtil.assignFreshIds(updatedSchema, schema(), newLastColumnId::incrementAndGet);

    // determine the next spec id
    OptionalInt maxSpecId = specs.stream().mapToInt(PartitionSpec::specId).max();
    int nextSpecId = maxSpecId.orElse(TableMetadata.INITIAL_SPEC_ID) + 1;

    // rebuild the partition spec using the new column ids
    PartitionSpec newSpec = freshSpec(nextSpecId, freshSchema, updatedPartitionSpec);

    // reassign partition field ids with existing partition specs in the table
    AtomicInteger lastPartitionId = new AtomicInteger(lastAssignedPartitionId);
    PartitionSpec freshSpec = reassignPartitionIds(newSpec, lastPartitionId::incrementAndGet);

    // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
    int specId = specs.stream()
        .filter(freshSpec::compatibleWith)
        .findFirst()
        .map(PartitionSpec::specId)
        .orElse(nextSpecId);

    ImmutableList.Builder<PartitionSpec> specListBuilder = ImmutableList.<PartitionSpec>builder().addAll(specs);
    if (!specsById.containsKey(specId)) {
      specListBuilder.add(freshSpec);
    }

    // determine the next order id
    OptionalInt maxOrderId = sortOrders.stream().mapToInt(SortOrder::orderId).max();
    int nextOrderId = maxOrderId.isPresent() ? maxOrderId.getAsInt() + 1 : INITIAL_SORT_ORDER_ID;

    // rebuild the sort order using new column ids
    int freshSortOrderId = updatedSortOrder.isUnsorted() ? updatedSortOrder.orderId() : nextOrderId;
    SortOrder freshSortOrder = freshSortOrder(freshSortOrderId, freshSchema, updatedSortOrder);

    // if the order already exists, use the same ID. otherwise, use the fresh order ID
    Optional<SortOrder> sameSortOrder = sortOrders.stream()
        .filter(sortOrder -> sortOrder.sameOrder(freshSortOrder))
        .findAny();
    int orderId = sameSortOrder.map(SortOrder::orderId).orElse(freshSortOrderId);

    ImmutableList.Builder<SortOrder> sortOrdersBuilder = ImmutableList.<SortOrder>builder().addAll(sortOrders);
    if (!sortOrdersById.containsKey(orderId)) {
      sortOrdersBuilder.add(freshSortOrder);
    }

    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(this.properties);
    newProperties.putAll(updatedProperties);

    // determine the next schema id
    int freshSchemaId = reuseOrCreateNewSchemaId(freshSchema);
    ImmutableList.Builder<Schema> schemasBuilder = ImmutableList.<Schema>builder().addAll(schemas);

    if (!schemasById.containsKey(freshSchemaId)) {
      schemasBuilder.add(new Schema(freshSchemaId, freshSchema.columns(), freshSchema.identifierFieldIds()));
    }

    return new TableMetadata(null, formatVersion, uuid, newLocation,
        lastSequenceNumber, System.currentTimeMillis(), newLastColumnId.get(), freshSchemaId, schemasBuilder.build(),
        specId, specListBuilder.build(), Math.max(lastAssignedPartitionId, freshSpec.lastAssignedFieldId()),
        orderId, sortOrdersBuilder.build(), ImmutableMap.copyOf(newProperties),
        -1, snapshots, ImmutableList.of(), addPreviousFile(file, lastUpdatedMillis, newProperties));
  }

  public TableMetadata updateLocation(String newLocation) {
    return new TableMetadata(null, formatVersion, uuid, newLocation,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, currentSnapshotId,
        snapshots, snapshotLog, addPreviousFile(file, lastUpdatedMillis));
  }

  public TableMetadata upgradeToFormatVersion(int newFormatVersion) {
    Preconditions.checkArgument(newFormatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
        "Cannot upgrade table to unsupported format version: v%s (supported: v%s)",
        newFormatVersion, SUPPORTED_TABLE_FORMAT_VERSION);
    Preconditions.checkArgument(newFormatVersion >= formatVersion,
        "Cannot downgrade v%s table to v%s", formatVersion, newFormatVersion);

    if (newFormatVersion == formatVersion) {
      return this;
    }

    return new TableMetadata(null, newFormatVersion, uuid, location,
        lastSequenceNumber, System.currentTimeMillis(), lastColumnId, currentSchemaId, schemas, defaultSpecId, specs,
        lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, currentSnapshotId,
        snapshots, snapshotLog, addPreviousFile(file, lastUpdatedMillis));
  }

  private List<MetadataLogEntry> addPreviousFile(InputFile previousFile, long timestampMillis) {
    return addPreviousFile(previousFile, timestampMillis, properties);
  }

  private List<MetadataLogEntry> addPreviousFile(InputFile previousFile, long timestampMillis,
                                                 Map<String, String> updatedProperties) {
    if (previousFile == null) {
      return previousFiles;
    }

    int maxSize = Math.max(1, PropertyUtil.propertyAsInt(updatedProperties,
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT));

    List<MetadataLogEntry> newMetadataLog;
    if (previousFiles.size() >= maxSize) {
      int removeIndex = previousFiles.size() - maxSize + 1;
      newMetadataLog = Lists.newArrayList(previousFiles.subList(removeIndex, previousFiles.size()));
    } else {
      newMetadataLog = Lists.newArrayList(previousFiles);
    }
    newMetadataLog.add(new MetadataLogEntry(timestampMillis, previousFile.location()));

    return newMetadataLog;
  }

  private static PartitionSpec updateSpecSchema(Schema schema, PartitionSpec partitionSpec) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema)
        .withSpecId(partitionSpec.specId());

    // add all of the fields to the builder. IDs should not change.
    for (PartitionField field : partitionSpec.fields()) {
      specBuilder.add(field.sourceId(), field.fieldId(), field.name(), field.transform().toString());
    }

    return specBuilder.build();
  }

  private static SortOrder updateSortOrderSchema(Schema schema, SortOrder sortOrder) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(sortOrder.orderId());

    // add all of the fields to the builder. IDs should not change.
    for (SortField field : sortOrder.fields()) {
      builder.addSortField(field.transform().toString(), field.sourceId(), field.direction(), field.nullOrder());
    }

    return builder.build();
  }

  private static PartitionSpec freshSpec(int specId, Schema schema, PartitionSpec partitionSpec) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema)
        .withSpecId(specId);

    for (PartitionField field : partitionSpec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = partitionSpec.schema().findColumnName(field.sourceId());
      specBuilder.add(
          schema.findField(sourceName).fieldId(),
          field.fieldId(),
          field.name(),
          field.transform().toString());
    }

    return specBuilder.build();
  }

  private static SortOrder freshSortOrder(int orderId, Schema schema, SortOrder sortOrder) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);

    for (SortField field : sortOrder.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = sortOrder.schema().findColumnName(field.sourceId());
      // reassign all sort fields with fresh sort field IDs
      int newSourceId = schema.findField(sourceName).fieldId();
      builder.addSortField(
          field.transform().toString(),
          newSourceId,
          field.direction(),
          field.nullOrder());
    }

    return builder.build();
  }

  private static Map<Long, Snapshot> indexAndValidateSnapshots(List<Snapshot> snapshots, long lastSequenceNumber) {
    ImmutableMap.Builder<Long, Snapshot> builder = ImmutableMap.builder();
    for (Snapshot snap : snapshots) {
      ValidationException.check(snap.sequenceNumber() <= lastSequenceNumber,
          "Invalid snapshot with sequence number %s greater than last sequence number %s",
          snap.sequenceNumber(), lastSequenceNumber);
      builder.put(snap.snapshotId(), snap);
    }
    return builder.build();
  }

  private Map<Integer, Schema> indexSchemas() {
    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas) {
      builder.put(schema.schemaId(), schema);
    }
    return builder.build();
  }

  private static Map<Integer, PartitionSpec> indexSpecs(List<PartitionSpec> specs) {
    ImmutableMap.Builder<Integer, PartitionSpec> builder = ImmutableMap.builder();
    for (PartitionSpec spec : specs) {
      builder.put(spec.specId(), spec);
    }
    return builder.build();
  }

  private static Map<Integer, SortOrder> indexSortOrders(List<SortOrder> sortOrders) {
    ImmutableMap.Builder<Integer, SortOrder> builder = ImmutableMap.builder();
    for (SortOrder sortOrder : sortOrders) {
      builder.put(sortOrder.orderId(), sortOrder);
    }
    return builder.build();
  }

  private int reuseOrCreateNewSchemaId(Schema newSchema) {
    // if the schema already exists, use its id; otherwise use the highest id + 1
    int newSchemaId = currentSchemaId;
    for (Schema schema : schemas) {
      if (schema.sameSchema(newSchema)) {
        newSchemaId = schema.schemaId();
        break;
      } else if (schema.schemaId() >= newSchemaId) {
        newSchemaId = schema.schemaId() + 1;
      }
    }
    return newSchemaId;
  }
}
