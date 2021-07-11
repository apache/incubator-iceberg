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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class TestStaticTable extends HadoopTableTestBase {

  private Table getStaticTable() {
    return TABLES.load(((HasTableOperations) table).operations().current().metadataFileLocation());
  }

  private Table getStaticTable(MetadataTableType type) {
    return TABLES.load(((HasTableOperations) table).operations().current().metadataFileLocation() + "#" + type);
  }

  @Test
  public void testLoadFromMetadata() {
    Table staticTable = getStaticTable();
    Assert.assertTrue("Loading a metadata file based table should return StaticTableOperations",
        ((HasTableOperations) staticTable).operations() instanceof StaticTableOperations);
  }

  @Test
  public void testCannotBeAddedTo() {
    Table staticTable = getStaticTable();
    AssertHelpers.assertThrows("Cannot modify a static table", UnsupportedOperationException.class,
        () -> staticTable.newOverwrite().addFile(FILE_A).commit());
  }

  @Test
  public void testCannotBeDeletedFrom() {
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    AssertHelpers.assertThrows("Cannot modify a static table", UnsupportedOperationException.class,
        () -> staticTable.newDelete().deleteFile(FILE_A).commit());
  }

  @Test
  public void testCannotDoIncrementalScanOnMetadataTable() {
    table.newAppend().appendFile(FILE_A).commit();

    for (MetadataTableType type : MetadataTableType.values()) {
      Table staticTable = getStaticTable(type);
      AssertHelpers.assertThrows("Static tables do not support incremental scans",
          UnsupportedOperationException.class,
          String.format("Cannot incrementally scan table of type %s", type),
          () -> staticTable.newScan().dataAfter(1));
    }
  }

  @Test
  public void testHasSameProperties() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();
    Table staticTable = getStaticTable();
    Assert.assertTrue("Same history?",
        table.history().containsAll(staticTable.history()));
    Assert.assertTrue("Same snapshot?",
        table.currentSnapshot().snapshotId() ==  staticTable.currentSnapshot().snapshotId());
    Assert.assertTrue("Same properties?",
        Maps.difference(table.properties(), staticTable.properties()).areEqual());
  }

  @Test
  public void testImmutable() {
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    long originalSnapshot = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();
    staticTable.refresh();

    Assert.assertEquals("Snapshot unchanged after table modified",
        staticTable.currentSnapshot().snapshotId(), originalSnapshot);
  }

  @Test
  public void testMetadataTables() {
    for (MetadataTableType type : MetadataTableType.values()) {
      String enumName = type.name().replace("_", "").toLowerCase();
      Assert.assertTrue("Should be able to get MetadataTable of type : " + type,
          getStaticTable(type).getClass().getName().toLowerCase().contains(enumName));
    }
  }


}
