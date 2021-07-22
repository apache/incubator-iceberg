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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadataParser.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableMetadataParser.FORMAT_VERSION;
import static org.apache.iceberg.TableMetadataParser.LAST_COLUMN_ID;
import static org.apache.iceberg.TableMetadataParser.LAST_UPDATED_MILLIS;
import static org.apache.iceberg.TableMetadataParser.LOCATION;
import static org.apache.iceberg.TableMetadataParser.PARTITION_SPEC;
import static org.apache.iceberg.TableMetadataParser.PROPERTIES;
import static org.apache.iceberg.TableMetadataParser.SCHEMA;
import static org.apache.iceberg.TableMetadataParser.SNAPSHOTS;
import static org.apache.iceberg.TestHelpers.assertSameSchemaList;

public class TestTableMetadata {
  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA = new Schema(7,
      Types.NestedField.required(1, "x", Types.LongType.get()),
      Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
      Types.NestedField.required(3, "z", Types.LongType.get())
  );

  private static final long SEQ_NO = 34;
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  private static final PartitionSpec SPEC_5 = PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();
  private static final SortOrder SORT_ORDER_3 = SortOrder.builderFor(TEST_SCHEMA)
      .withOrderId(3)
      .asc("y", NullOrder.NULLS_FIRST)
      .desc(Expressions.bucket("z", 4), NullOrder.NULLS_LAST)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, 7, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> snapshotLog = ImmutableList.<HistoryEntry>builder()
        .add(new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
        .add(new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
        .build();

    Schema schema = new Schema(6,
        Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata expected = new TableMetadata(null, 2, UUID.randomUUID().toString(), TEST_LOCATION,
        SEQ_NO, System.currentTimeMillis(), 3,
        7, ImmutableList.of(TEST_SCHEMA, schema),
        5, ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(),
        3, ImmutableList.of(SORT_ORDER_3), ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), snapshotLog, ImmutableList.of());

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(ops.io(), null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Format version should match",
        expected.formatVersion(), metadata.formatVersion());
    Assert.assertEquals("Table UUID should match",
        expected.uuid(), metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last sequence number should match",
        expected.lastSequenceNumber(), metadata.lastSequenceNumber());
    Assert.assertEquals("Last column ID should match",
        expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals("Current schema id should match",
        expected.currentSchemaId(), metadata.currentSchemaId());
    assertSameSchemaList(expected.schemas(), metadata.schemas());
    Assert.assertEquals("Partition spec should match",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Default spec ID should match",
        expected.defaultSpecId(), metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec map should match",
        expected.specs(), metadata.specs());
    Assert.assertEquals("lastAssignedFieldId across all PartitionSpecs should match",
        expected.spec().lastAssignedFieldId(), metadata.lastAssignedPartitionId());
    Assert.assertEquals("Default sort ID should match",
        expected.defaultSortOrderId(), metadata.defaultSortOrderId());
    Assert.assertEquals("Sort order should match",
        expected.sortOrder(), metadata.sortOrder());
    Assert.assertEquals("Sort order map should match",
        expected.sortOrders(), metadata.sortOrders());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Snapshot logs should match",
        expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot ID should match",
        (Long) previousSnapshotId, metadata.currentSnapshot().parentId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.allManifests(), metadata.currentSnapshot().allManifests());
    Assert.assertEquals("Schema ID for current snapshot should match",
        (Integer) 7, metadata.currentSnapshot().schemaId());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.allManifests(),
        metadata.snapshot(previousSnapshotId).allManifests());
    Assert.assertNull("Previous snapshot's schema ID should be null",
        metadata.snapshot(previousSnapshotId).schemaId());
  }

  @Test
  public void testBackwardCompat() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TEST_SCHEMA).identity("x").withSpecId(6).build();
    SortOrder sortOrder = SortOrder.unsorted();
    Schema schema = new Schema(TableMetadata.INITIAL_SCHEMA_ID, TEST_SCHEMA.columns());

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), spec.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), spec.specId())));

    TableMetadata expected = new TableMetadata(null, 1, null, TEST_LOCATION,
        0, System.currentTimeMillis(), 3, TableMetadata.INITIAL_SCHEMA_ID,
        ImmutableList.of(schema), 6, ImmutableList.of(spec), spec.lastAssignedFieldId(),
        TableMetadata.INITIAL_SORT_ORDER_ID, ImmutableList.of(sortOrder), ImmutableMap.of("property", "value"),
        currentSnapshotId, Arrays.asList(previousSnapshot, currentSnapshot), ImmutableList.of(), ImmutableList.of());

    String asJson = toJsonWithoutSpecAndSchemaList(expected);
    TableMetadata metadata = TableMetadataParser
        .fromJson(ops.io(), null, JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Format version should match",
        expected.formatVersion(), metadata.formatVersion());
    Assert.assertNull("Table UUID should not be assigned", metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last sequence number should default to 0",
        expected.lastSequenceNumber(), metadata.lastSequenceNumber());
    Assert.assertEquals("Last column ID should match",
        expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals("Current schema ID should be default to TableMetadata.INITIAL_SCHEMA_ID",
        TableMetadata.INITIAL_SCHEMA_ID, metadata.currentSchemaId());
    Assert.assertEquals("Schemas size should match",
        1, metadata.schemas().size());
    Assert.assertEquals("Schemas should contain the schema",
        metadata.schemas().get(0).asStruct(), schema.asStruct());
    Assert.assertEquals("Partition spec should be the default",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Default spec ID should default to TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec should contain the spec",
        1, metadata.specs().size());
    Assert.assertTrue("PartitionSpec should contain the spec",
        metadata.specs().get(0).compatibleWith(spec));
    Assert.assertEquals("PartitionSpec should have ID TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.specs().get(0).specId());
    Assert.assertEquals("lastAssignedFieldId across all PartitionSpecs should match",
        expected.spec().lastAssignedFieldId(), metadata.lastAssignedPartitionId());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Snapshot logs should match",
        expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot ID should match",
        (Long) previousSnapshotId, metadata.currentSnapshot().parentId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.allManifests(), metadata.currentSnapshot().allManifests());
    Assert.assertNull("Current snapshot's schema ID should be null",
        metadata.currentSnapshot().schemaId());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.allManifests(),
        metadata.snapshot(previousSnapshotId).allManifests());
    Assert.assertEquals("Snapshot logs should match",
            expected.previousFiles(), metadata.previousFiles());
    Assert.assertNull("Previous snapshot's schema ID should be null",
        metadata.snapshot(previousSnapshotId).schemaId());
  }

  private static String toJsonWithoutSpecAndSchemaList(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, 1);
      generator.writeStringField(LOCATION, metadata.location());
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
      generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

      // mimic an old writer by writing only schema and not the current ID or schema list
      generator.writeFieldName(SCHEMA);
      SchemaParser.toJson(metadata.schema().asStruct(), generator);

      // mimic an old writer by writing only partition-spec and not the default ID or spec list
      generator.writeFieldName(PARTITION_SPEC);
      PartitionSpecParser.toJsonFields(metadata.spec(), generator);

      generator.writeObjectFieldStart(PROPERTIES);
      for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
        generator.writeStringField(keyValue.getKey(), keyValue.getValue());
      }
      generator.writeEndObject();

      generator.writeNumberField(CURRENT_SNAPSHOT_ID,
          metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1);

      generator.writeArrayFieldStart(SNAPSHOTS);
      for (Snapshot snapshot : metadata.snapshots()) {
        SnapshotParser.toJson(snapshot, generator);
      }
      generator.writeEndArray();
      // skip the snapshot log

      generator.writeEndObject(); // end table metadata object

      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write json for: %s", metadata), e);
    }
    return writer.toString();
  }

  @Test
  public void testJsonWithPreviousMetadataLog() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));

    TableMetadata base = new TableMetadata(null, 1, UUID.randomUUID().toString(), TEST_LOCATION,
        0, System.currentTimeMillis(), 3,
        7, ImmutableList.of(TEST_SCHEMA), 5, ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(),
        3, ImmutableList.of(SORT_ORDER_3), ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    String asJson = TableMetadataParser.toJson(base);
    TableMetadata metadataFromJson = TableMetadataParser.fromJson(ops.io(), null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Metadata logs should match", previousMetadataLog, metadataFromJson.previousFiles());
  }

  @Test
  public void testAddPreviousMetadataRemoveNone() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 80, 3,
        7, ImmutableList.of(TEST_SCHEMA), 5, ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(),
        3, ImmutableList.of(SORT_ORDER_3), ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));
    Set<MetadataLogEntry> removedPreviousMetadata = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog, metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should be empty", 0, removedPreviousMetadata.size());
  }

  @Test
  public void testAddPreviousMetadataRemoveOne() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 70,
        "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 60,
        "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 50,
        "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 50, 3,
        7, ImmutableList.of(TEST_SCHEMA), 5,
        ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(), 3, ImmutableList.of(SORT_ORDER_3),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog.subList(1, 6),
        metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should contain 1", previousMetadataLog.subList(0, 1),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testAddPreviousMetadataRemoveMultiple() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, null, ImmutableList.of(
          new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 70,
        "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 60,
        "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 50,
        "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 50, 3, 7, ImmutableList.of(TEST_SCHEMA), 2,
        ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(),
        TableMetadata.INITIAL_SORT_ORDER_ID, ImmutableList.of(SortOrder.unsorted()),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog.subList(4, 6),
        metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should contain 4", previousMetadataLog.subList(0, 4),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testV2UUIDValidation() {
    AssertHelpers.assertThrows("Should reject v2 metadata without a UUID",
        IllegalArgumentException.class, "UUID is required in format v2",
        () -> new TableMetadata(null, 2, null, TEST_LOCATION, SEQ_NO, System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID, 7, ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(), ImmutableList.of(SPEC_5), SPEC_5.lastAssignedFieldId(),
            3, ImmutableList.of(SORT_ORDER_3), ImmutableMap.of(), -1L,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of())
    );
  }

  @Test
  public void testVersionValidation() {
    int unsupportedVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1;
    AssertHelpers.assertThrows("Should reject unsupported metadata",
        IllegalArgumentException.class, "Unsupported format version: v" + unsupportedVersion,
        () -> new TableMetadata(null, unsupportedVersion, null, TEST_LOCATION, SEQ_NO,
            System.currentTimeMillis(), LAST_ASSIGNED_COLUMN_ID,
            7, ImmutableList.of(TEST_SCHEMA), SPEC_5.specId(), ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(), 3, ImmutableList.of(SORT_ORDER_3), ImmutableMap.of(), -1L,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of())
    );
  }

  @Test
  public void testParserVersionValidation() throws Exception {
    String supportedVersion1 = readTableMetadataInputFile("TableMetadataV1Valid.json");
    TableMetadata parsed1 = TableMetadataParser.fromJson(
        ops.io(), null, JsonUtil.mapper().readValue(supportedVersion1, JsonNode.class));
    Assert.assertNotNull("Should successfully read supported metadata version", parsed1);

    String supportedVersion2 = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed2 = TableMetadataParser.fromJson(
        ops.io(), null, JsonUtil.mapper().readValue(supportedVersion2, JsonNode.class));
    Assert.assertNotNull("Should successfully read supported metadata version", parsed2);

    String unsupportedVersion = readTableMetadataInputFile("TableMetadataUnsupportedVersion.json");
    AssertHelpers.assertThrows("Should not read unsupported metadata",
        IllegalArgumentException.class, "Cannot read unsupported version",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupportedVersion, JsonNode.class))
    );
  }


  @Test
  public void testParserV2PartitionSpecsValidation() throws Exception {
    String unsupportedVersion = readTableMetadataInputFile("TableMetadataV2MissingPartitionSpecs.json");
    AssertHelpers.assertThrows("Should reject v2 metadata without partition specs",
        IllegalArgumentException.class, "partition-specs must exist in format v2",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupportedVersion, JsonNode.class))
    );
  }

  @Test
  public void testParserV2LastAssignedFieldIdValidation() throws Exception {
    String unsupportedVersion = readTableMetadataInputFile("TableMetadataV2MissingLastPartitionId.json");
    AssertHelpers.assertThrows("Should reject v2 metadata without last assigned partition field id",
        IllegalArgumentException.class, "last-partition-id must exist in format v2",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupportedVersion, JsonNode.class))
    );
  }

  @Test
  public void testParserV2SortOrderValidation() throws Exception {
    String unsupportedVersion = readTableMetadataInputFile("TableMetadataV2MissingSortOrder.json");
    AssertHelpers.assertThrows("Should reject v2 metadata without sort order",
        IllegalArgumentException.class, "sort-orders must exist in format v2",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupportedVersion, JsonNode.class))
    );
  }

  @Test
  public void testParserV2CurrentSchemaIdValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2CurrentSchemaNotFound.json");
    AssertHelpers.assertThrows("Should reject v2 metadata without valid schema id",
        IllegalArgumentException.class, "Cannot find schema with current-schema-id=2 from schemas",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupported, JsonNode.class))
    );
  }

  @Test
  public void testParserV2SchemasValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2MissingSchemas.json");
    AssertHelpers.assertThrows("Should reject v2 metadata without schemas",
        IllegalArgumentException.class, "schemas must exist in format v2",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupported, JsonNode.class))
    );
  }

  private String readTableMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }

  @Test
  public void testNewTableMetadataReassignmentAllIds() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(3, "x", Types.LongType.get()),
        Types.NestedField.required(4, "y", Types.LongType.get()),
        Types.NestedField.required(5, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5)
        .add(3, 1005, "x_partition", "bucket[4]")
        .add(5, 1003, "z_partition", "bucket[8]")
        .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    // newTableMetadata should reassign column ids and partition field ids.
    PartitionSpec expected = PartitionSpec.builderFor(metadata.schema()).withSpecId(0)
        .add(1, 1000, "x_partition", "bucket[4]")
        .add(3, 1001, "z_partition", "bucket[8]")
        .build();

    Assert.assertEquals(expected, metadata.spec());
  }

  @Test
  public void testInvalidUpdatePartitionSpecForV1Table() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5)
        .add(1, 1005, "x_partition", "bucket[4]")
        .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    AssertHelpers.assertThrows("Should fail to update an invalid partition spec",
        ValidationException.class, "Spec does not use sequential IDs that are required in v1",
        () -> metadata.updatePartitionSpec(spec));
  }

  @Test
  public void testSortOrder() {
    Schema schema = new Schema(
        Types.NestedField.required(10, "x", Types.StringType.get())
    );

    TableMetadata meta = TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    Assert.assertTrue("Should default to unsorted order", meta.sortOrder().isUnsorted());
    Assert.assertSame("Should detect identical unsorted order", meta, meta.replaceSortOrder(SortOrder.unsorted()));
  }

  @Test
  public void testUpdateSortOrder() {
    Schema schema = new Schema(
        Types.NestedField.required(10, "x", Types.StringType.get())
    );

    SortOrder order = SortOrder.builderFor(schema).asc("x").build();

    TableMetadata sortedByX = TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), order, null, ImmutableMap.of());
    Assert.assertEquals("Should have 1 sort order", 1, sortedByX.sortOrders().size());
    Assert.assertEquals("Should use orderId 1", 1, sortedByX.sortOrder().orderId());
    Assert.assertEquals("Should be sorted by one field", 1, sortedByX.sortOrder().fields().size());
    Assert.assertEquals("Should use the table's field ids", 1, sortedByX.sortOrder().fields().get(0).sourceId());
    Assert.assertEquals("Should be ascending",
        SortDirection.ASC, sortedByX.sortOrder().fields().get(0).direction());
    Assert.assertEquals("Should be nulls first",
        NullOrder.NULLS_FIRST, sortedByX.sortOrder().fields().get(0).nullOrder());

    // build an equivalent order with the correct schema
    SortOrder newOrder = SortOrder.builderFor(sortedByX.schema()).asc("x").build();

    TableMetadata alsoSortedByX = sortedByX.replaceSortOrder(newOrder);
    Assert.assertSame("Should detect current sortOrder and not update", alsoSortedByX, sortedByX);

    TableMetadata unsorted = alsoSortedByX.replaceSortOrder(SortOrder.unsorted());
    Assert.assertEquals("Should have 2 sort orders", 2, unsorted.sortOrders().size());
    Assert.assertEquals("Should use orderId 0", 0, unsorted.sortOrder().orderId());
    Assert.assertTrue("Should be unsorted", unsorted.sortOrder().isUnsorted());

    TableMetadata sortedByXDesc = unsorted.replaceSortOrder(SortOrder.builderFor(unsorted.schema()).desc("x").build());
    Assert.assertEquals("Should have 3 sort orders", 3, sortedByXDesc.sortOrders().size());
    Assert.assertEquals("Should use orderId 2", 2, sortedByXDesc.sortOrder().orderId());
    Assert.assertEquals("Should be sorted by one field", 1, sortedByXDesc.sortOrder().fields().size());
    Assert.assertEquals("Should use the table's field ids", 1, sortedByXDesc.sortOrder().fields().get(0).sourceId());
    Assert.assertEquals("Should be ascending",
        SortDirection.DESC, sortedByXDesc.sortOrder().fields().get(0).direction());
    Assert.assertEquals("Should be nulls first",
        NullOrder.NULLS_FIRST, sortedByX.sortOrder().fields().get(0).nullOrder());
  }

  @Test
  public void testParseSchemaIdentifierFields() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed = TableMetadataParser.fromJson(
        ops.io(), null, JsonUtil.mapper().readValue(data, JsonNode.class));
    Assert.assertEquals(Sets.newHashSet(), parsed.schemasById().get(0).identifierFieldIds());
    Assert.assertEquals(Sets.newHashSet(1, 2), parsed.schemasById().get(1).identifierFieldIds());
  }

  @Test
  public void testUpdateSchemaIdentifierFields() {
    Schema schema = new Schema(
        Types.NestedField.required(10, "x", Types.StringType.get())
    );

    TableMetadata meta = TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    Schema newSchema = new Schema(
        Lists.newArrayList(Types.NestedField.required(1, "x", Types.StringType.get())),
        Sets.newHashSet(1)
    );
    TableMetadata newMeta = meta.updateSchema(newSchema, 1);
    Assert.assertEquals(2, newMeta.schemas().size());
    Assert.assertEquals(Sets.newHashSet(1), newMeta.schema().identifierFieldIds());
  }

  @Test
  public void testUpdateSchema() {
    Schema schema = new Schema(0,
        Types.NestedField.required(1, "y", Types.LongType.get(), "comment")
    );
    TableMetadata freshTable = TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    Assert.assertEquals("Should use TableMetadata.INITIAL_SCHEMA_ID for current schema id",
        TableMetadata.INITIAL_SCHEMA_ID, freshTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema), freshTable.schemas());
    Assert.assertEquals("Should have expected schema upon return",
        schema.asStruct(), freshTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id", 1, freshTable.lastColumnId());

    // update schema
    Schema schema2 = new Schema(
        Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
        Types.NestedField.required(2, "x", Types.StringType.get())
    );
    TableMetadata twoSchemasTable = freshTable.updateSchema(schema2, 2);
    Assert.assertEquals("Should have current schema id as 1",
        1, twoSchemasTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema, new Schema(1, schema2.columns())),
        twoSchemasTable.schemas());
    Assert.assertEquals("Should have expected schema upon return",
        schema2.asStruct(), twoSchemasTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id", 2, twoSchemasTable.lastColumnId());

    // update schema with the the same schema and last column ID as current shouldn't cause change
    Schema sameSchema2 = new Schema(
        Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
        Types.NestedField.required(2, "x", Types.StringType.get())
    );
    TableMetadata sameSchemaTable = twoSchemasTable.updateSchema(sameSchema2, 2);
    Assert.assertSame("Should return same table metadata",
        twoSchemasTable, sameSchemaTable);

    // update schema with the the same schema and different last column ID as current should create a new table
    TableMetadata differentColumnIdTable = sameSchemaTable.updateSchema(sameSchema2, 3);
    Assert.assertEquals("Should have current schema id as 1",
        1, differentColumnIdTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema, new Schema(1, schema2.columns())),
        differentColumnIdTable.schemas());
    Assert.assertEquals("Should have expected schema upon return",
        schema2.asStruct(), differentColumnIdTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id",
        3, differentColumnIdTable.lastColumnId());

    // update schema with old schema does not change schemas
    TableMetadata revertSchemaTable = differentColumnIdTable.updateSchema(schema, 3);
    Assert.assertEquals("Should have current schema id as 0",
        0, revertSchemaTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema, new Schema(1, schema2.columns())),
        revertSchemaTable.schemas());
    Assert.assertEquals("Should have expected schema upon return",
        schema.asStruct(), revertSchemaTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id",
        3, revertSchemaTable.lastColumnId());

    // create new schema will use the largest schema id + 1
    Schema schema3 = new Schema(
        Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
        Types.NestedField.required(4, "x", Types.StringType.get()),
        Types.NestedField.required(6, "z", Types.IntegerType.get())
    );
    TableMetadata threeSchemaTable = revertSchemaTable.updateSchema(schema3, 6);
    Assert.assertEquals("Should have current schema id as 2",
        2, threeSchemaTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema,
            new Schema(1, schema2.columns()),
            new Schema(2, schema3.columns())), threeSchemaTable.schemas());
    Assert.assertEquals("Should have expected schema upon return",
        schema3.asStruct(), threeSchemaTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id",
        6, threeSchemaTable.lastColumnId());
  }
}
