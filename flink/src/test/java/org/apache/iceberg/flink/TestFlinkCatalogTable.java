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

package org.apache.iceberg.flink;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestFlinkCatalogTable extends FlinkCatalogTestBase {

  public TestFlinkCatalogTable(String catalogName, String[] baseNamepace) {
    super(catalogName, baseNamepace);
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP TABLE IF EXISTS %s.tl2", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testGetTable() {
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(
            Types.NestedField.optional(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "strV", Types.StringType.get())));
    Assert.assertEquals(
        Arrays.asList(
            TableColumn.of("id", DataTypes.BIGINT()),
            TableColumn.of("strV", DataTypes.STRING())),
        getTableEnv().from("tl").getSchema().getTableColumns());
    Assert.assertTrue(getTableEnv().getCatalog(catalogName).get().tableExists(ObjectPath.fromString("db.tl")));
  }

  @Test
  public void testRenameTable() {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));
    sql("ALTER TABLE tl RENAME TO tl2");
    AssertHelpers.assertThrows(
        "Should fail if trying to get a nonexistent table",
        ValidationException.class,
        "Table `tl` was not found.",
        () -> getTableEnv().from("tl")
    );
    Assert.assertEquals(
        Collections.singletonList(TableColumn.of("id", DataTypes.BIGINT())),
        getTableEnv().from("tl2").getSchema().getTableColumns());
  }

  @Test
  public void testCreateTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(Maps.newHashMap(), table.properties());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(TableSchema.builder().field("id", DataTypes.BIGINT()).build(), catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
  }

  @Test
  public void testCreateTableLocation() {
    Assume.assumeFalse("HadoopCatalog does not support creating table with location", isHadoopCatalog);

    sql("CREATE TABLE tl(id BIGINT) WITH ('location'='/tmp/location')");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals("/tmp/location", table.location());
    Assert.assertEquals(Maps.newHashMap(), table.properties());
  }

  @Test
  public void testCreatePartitionTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT, dt STRING) PARTITIONED BY(dt)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "dt", Types.StringType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(PartitionSpec.builderFor(table.schema()).identity("dt").build(), table.spec());
    Assert.assertEquals(Maps.newHashMap(), table.properties());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).field("dt", DataTypes.STRING()).build(),
        catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
    Assert.assertEquals(Collections.singletonList("dt"), catalogTable.getPartitionKeys());
  }

  @Test
  public void testCreateTableWithPrimaryKey() throws Exception {
    sql("CREATE TABLE tl(id BIGINT, dt STRING, PRIMARY KEY(id, dt) NOT ENFORCED)");

    Table table = table("tl");
    Assert.assertEquals(new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "dt", Types.StringType.get())
    ).asStruct(), table.schema().asStruct());
    Assert.assertEquals("Should have the expected primary keys", "id,dt",
        PropertyUtil.propertyAsString(table.properties(),
            TableProperties.EQUALITY_FIELD_COLUMNS,
            TableProperties.DEFAULT_EQUALITY_FIELD_COLUMNS));

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertTrue(catalogTable.getSchema().getTableColumn("id").isPresent());
    Assert.assertTrue(catalogTable.getSchema().getTableColumn("dt").isPresent());
    Assert.assertEquals(ImmutableMap.of(TableProperties.EQUALITY_FIELD_COLUMNS, "id,dt"), catalogTable.getOptions());

    Optional<UniqueConstraint> constraint = catalogTable.getSchema().getPrimaryKey();
    Assert.assertTrue("Should have an unique constraint.", constraint.isPresent());
    Assert.assertEquals("Should have the expected primary key columns", ImmutableList.of("id", "dt"),
        constraint.get().getColumns());
  }

  @Test
  public void testLoadTransformPartitionTable() throws TableNotExistException {
    Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"), schema,
        PartitionSpec.builderFor(schema).bucket("id", 100).build());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).build(),
        catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
    Assert.assertEquals(Collections.emptyList(), catalogTable.getPartitionKeys());
  }

  @Test
  public void testAlterTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT) WITH ('oldK'='oldV')");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("oldK", "oldV");

    // new
    sql("ALTER TABLE tl SET('newK'='newV')");
    properties.put("newK", "newV");
    Assert.assertEquals(properties, table("tl").properties());

    // update old
    sql("ALTER TABLE tl SET('oldK'='oldV2')");
    properties.put("oldK", "oldV2");
    Assert.assertEquals(properties, table("tl").properties());

    // remove property
    CatalogTable catalogTable = catalogTable("tl");
    properties.remove("oldK");
    getTableEnv().getCatalog(getTableEnv().getCurrentCatalog()).get().alterTable(
        new ObjectPath(DATABASE, "tl"), catalogTable.copy(properties), false);
    Assert.assertEquals(properties, table("tl").properties());
  }

  @Test
  public void testRelocateTable() {
    Assume.assumeFalse("HadoopCatalog does not support relocate table", isHadoopCatalog);

    sql("CREATE TABLE tl(id BIGINT)");
    sql("ALTER TABLE tl SET('location'='/tmp/location')");
    Assert.assertEquals("/tmp/location", table("tl").location());
  }

  @Test
  public void testSetCurrentAndCherryPickSnapshotId() {
    sql("CREATE TABLE tl(c1 INT, c2 STRING, c3 STRING) PARTITIONED BY (c1)");

    Table table = table("tl");

    DataFile fileA = DataFiles.builder(table.spec())
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("c1=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
    DataFile fileB = DataFiles.builder(table.spec())
        .withPath("/path/to/data-b.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("c1=1") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
    DataFile replacementFile = DataFiles.builder(table.spec())
        .withPath("/path/to/data-a-replacement.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("c1=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();

    table.newAppend()
        .appendFile(fileA)
        .commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions()
        .addFile(replacementFile)
        .stageOnly()
        .commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals("Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend()
        .appendFile(fileB)
        .commit();

    // test cherry pick
    sql("ALTER TABLE tl SET('cherry-pick-snapshot-id'='%s')", staged.snapshotId());
    validateTableFiles(table, fileB, replacementFile);

    // test set current snapshot
    sql("ALTER TABLE tl SET('current-snapshot-id'='%s')", snapshotId);
    validateTableFiles(table, fileA);
  }

  private void validateTableFiles(Table tbl, DataFile... expectedFiles) {
    tbl.refresh();
    Set<CharSequence> expectedFilePaths = Arrays.stream(expectedFiles).map(DataFile::path).collect(Collectors.toSet());
    Set<CharSequence> actualFilePaths = StreamSupport.stream(tbl.newScan().planFiles().spliterator(), false)
        .map(FileScanTask::file).map(ContentFile::path)
        .collect(Collectors.toSet());
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
  }

  private Table table(String name) {
    return validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, name));
  }

  private CatalogTable catalogTable(String name) throws TableNotExistException {
    return (CatalogTable) getTableEnv().getCatalog(getTableEnv().getCurrentCatalog()).get()
        .getTable(new ObjectPath(DATABASE, name));
  }
}
