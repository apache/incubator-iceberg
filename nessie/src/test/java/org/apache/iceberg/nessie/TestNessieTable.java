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

package org.apache.iceberg.nessie;


import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class TestNessieTable extends BaseTestIceberg {

  private static final String BRANCH = "iceberg-table-test";

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final ContentsKey KEY = ContentsKey.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get())).fields());
  private static final Schema ALTERED = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.LongType.get())).fields());

  private Path tableLocation;

  public TestNessieTable() {
    super(BRANCH);
  }

  @Before
  public void beforeEach() throws IOException {
    super.beforeEach();
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, SCHEMA).location());
  }

  @After
  public void afterEach() throws Exception {
    // drop the table data
    if (tableLocation != null) {
      tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
      catalog.refresh();
      catalog.dropTable(TABLE_IDENTIFIER, false);
    }

    super.afterEach();
  }

  private com.dremio.nessie.model.IcebergTable getTable(ContentsKey key) throws NessieNotFoundException {
    return client.getContentsApi()
        .getContents(key, BRANCH)
        .unwrap(IcebergTable.class).get();
  }

  @Test
  public void testCreate() throws NessieNotFoundException, IOException {
    // Table should be created in iceberg
    // Table should be renamed in iceberg
    String tableName = TABLE_IDENTIFIER.name();
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    IcebergTable table = getTable(KEY);
    // check parameters are in expected state
    Assert.assertEquals(getTableLocation(tableName),
        (temp.getRoot().toURI().toString() + DB_NAME + "/" +
            tableName).replace("//",
            "/"));

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(tableName).size());
    Assert.assertEquals(0, manifestFiles(tableName).size());
  }

  @Test
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(),
        renamedTableName);

    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue(catalog.tableExists(renameTableIdentifier));

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assert.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
    Assert.assertEquals(original.spec(), renamed.spec());
    Assert.assertEquals(original.location(), renamed.location());
    Assert.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());

    Assert.assertTrue(catalog.dropTable(renameTableIdentifier));
  }

  @Test
  public void testDrop() {
    Assert.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue(catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);


    String fileLocation = addRecordsToFile(table, "file");

    DataFile file = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();

    table.newAppend().appendFile(file).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assert.assertTrue(catalog.dropTable(TABLE_IDENTIFIER, false));
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertTrue(new File(fileLocation).exists());
    Assert.assertTrue(new File(manifestListLocation).exists());
  }

  @Test
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(SCHEMA, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String location1 = addRecordsToFile(table, "file1");
    String location2 = addRecordsToFile(table, "file2");

    DataFile file1 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location1)
        .withFileSizeInBytes(Files.localInput(location2).getLength())
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location2)
        .withFileSizeInBytes(Files.localInput(location1).getLength())
        .build();

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();

    Assert.assertTrue(catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertTrue(new File(location1).exists());
    Assert.assertTrue(new File(location2).exists());
    Assert.assertTrue(new File(manifestListLocation).exists());
    for (ManifestFile manifest : manifests) {
      Assert.assertTrue(new File(manifest.path().replace("file:", "")).exists());
    }
    Assert.assertTrue(new File(
        ((HasTableOperations) table).operations()
            .current()
            .metadataFileLocation()
            .replace("file:", ""))
        .exists());
  }

  @Test
  public void testExistingTableUpdate() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(ALTERED.asStruct(), icebergTable.schema().asStruct());

  }

  @Test
  public void testFailure() throws NessieNotFoundException, NessieConflictException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    Branch branch = (Branch) client.getTreeApi().getReferenceByName(BRANCH);

    IcebergTable table = client.getContentsApi().getContents(KEY, BRANCH).unwrap(IcebergTable.class).get();

    client.getContentsApi().setContents(KEY, branch.getName(), branch.getHash(), "",
        IcebergTable.of("dummytable.metadata.json"));

    AssertHelpers.assertThrows("Update schema fails with conflict exception, ref not up to date",
        CommitFailedException.class,
        () -> icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit());
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
        .filter(t -> t.namespace()
            .level(0)
            .equals(DB_NAME) &&
            t.name().equals(TABLE_NAME))
        .collect(Collectors.toList());

    Assert.assertEquals(1, expectedIdents.size());
    Assert.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
  }

  private String getTableBasePath(String tableName) {
    String databasePath = temp.getRoot().toString() + "/" + DB_NAME;
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  protected Path getTableLocationPath(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString());
  }

  protected String getTableLocation(String tableName) {
    return getTableLocationPath(tableName).toString();
  }

  private String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());
  }

  protected List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
  }

  protected List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName)
        .stream()
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }

  private static String addRecordsToFile(Table table, String filename) throws IOException {
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(SCHEMA, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String fileLocation = table.location().replace("file:", "") +
        String.format("/data/%s.avro", filename);
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
        .schema(SCHEMA)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }
    return fileLocation;
  }
}
