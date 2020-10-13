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

package org.apache.iceberg.hadoop;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestHadoopTables {

  private static final HadoopTables TABLES = new HadoopTables();
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get(), "unique ID"),
      required(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Before
  public void setupTableLocation() throws Exception {
    tableDir = temp.newFolder();
  }

  @Test
  public void testDropTable() {
    TABLES.create(SCHEMA, tableDir.toURI().toString());
    TABLES.dropTable(tableDir.toURI().toString());
    AssertHelpers.assertThrows(
        "Should complain about missing table", NoSuchTableException.class,
        "Table does not exist", () -> TABLES.load(tableDir.toURI().toString()));
  }

  @Test
  public void testDropTableWithPurge() throws IOException {
    File dataDir = temp.newFolder();

    createDummyTable(tableDir, dataDir);

    TABLES.dropTable(tableDir.toURI().toString(), true);
    AssertHelpers.assertThrows(
        "Should complain about missing table", NoSuchTableException.class,
        "Table does not exist", () -> TABLES.load(tableDir.toURI().toString()));

    Assert.assertEquals(0, dataDir.listFiles().length);
    Assert.assertFalse(tableDir.exists());

    Assert.assertFalse(TABLES.dropTable(tableDir.toURI().toString()));
  }

  @Test
  public void testDropTableWithoutPurge() throws IOException {
    File dataDir = temp.newFolder();

    createDummyTable(tableDir, dataDir);

    TABLES.dropTable(tableDir.toURI().toString(), false);
    AssertHelpers.assertThrows(
        "Should complain about missing table", NoSuchTableException.class,
        "Table does not exist", () -> TABLES.load(tableDir.toURI().toString()));

    Assert.assertEquals(1, dataDir.listFiles().length);
    Assert.assertFalse(tableDir.exists());

    Assert.assertFalse(TABLES.dropTable(tableDir.toURI().toString()));
  }

  @Test
  public void testDefaultSortOrder() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .build();
    Table table = TABLES.create(SCHEMA, spec, tableDir.toURI().toString());

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 0, sortOrder.orderId());
    Assert.assertTrue("Order must unsorted", sortOrder.isUnsorted());
  }

  @Test
  public void testCustomSortOrder() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .build();
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .asc("id", NULLS_FIRST)
        .build();
    Table table = TABLES.create(SCHEMA, spec, order, Maps.newHashMap(), tableDir.toURI().toString());

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
    Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
    Assert.assertEquals("Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
    Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
    Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());
  }

  @Test
  public void testTableName() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .build();
    String location = tableDir.toURI().toString();
    TABLES.create(SCHEMA, spec, location);

    Table table = TABLES.load(location);
    Assert.assertEquals("Name must match", location, table.name());

    Table snapshotsTable = TABLES.load(location + "#snapshots");
    Assert.assertEquals("Name must match", location + "#snapshots", snapshotsTable.name());
  }

  private static void createDummyTable(File tableDir, File dataDir) throws IOException {
    Table table = TABLES.create(SCHEMA, tableDir.toURI().toString());
    AppendFiles append = table.newAppend();
    String data = dataDir.getPath() + "/data.parquet";
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    append.appendFile(dataFile);
    append.commit();

    // Make sure that the data file and the manifest dir is created
    Assert.assertEquals(1, dataDir.listFiles().length);
    Assert.assertEquals(1, tableDir.listFiles().length);
  }
}
