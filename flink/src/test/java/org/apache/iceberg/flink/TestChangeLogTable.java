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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.flink.source.ChangeLogTableTestBase;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * In this test case, we mainly cover the impact of primary key selection, multiple operations within a single
 * transaction, and multiple operations between different txn on the correctness of the data.
 */
@RunWith(Parameterized.class)
public class TestChangeLogTable extends ChangeLogTableTestBase {
  private static final Configuration CONF = new Configuration();
  private static final String SOURCE_TABLE = "default_catalog.default_database.source_change_logs";

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static String warehouse;

  private final boolean partitioned;

  @Parameterized.Parameters(name = "PartitionedTable={0}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {true},
        new Object[] {false}
    );
  }

  public TestChangeLogTable(boolean partitioned) {
    this.partitioned = partitioned;
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    warehouse = String.format("file:%s", warehouseFile);
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
  }

  @After
  @Override
  public void clean() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
    BoundedTableFactory.clearDataSets();
  }

  @Test
  public void testSqlChangeLogOnIdKey() throws Exception {
    List<List<Row>> inputRowsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            insertRow(1, "aaa"),
            deleteRow(1, "aaa"),
            insertRow(1, "bbb"),
            insertRow(2, "aaa"),
            deleteRow(2, "aaa"),
            insertRow(2, "bbb")
        ),
        ImmutableList.of(
            updateBeforeRow(2, "bbb"),
            updateAfterRow(2, "ccc"),
            deleteRow(2, "ccc"),
            insertRow(2, "ddd")
        ),
        ImmutableList.of(
            deleteRow(1, "bbb"),
            insertRow(1, "ccc"),
            deleteRow(1, "ccc"),
            insertRow(1, "ddd")
        )
    );

    List<List<Row>> expectedRecordsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(insertRow(1, "bbb"), insertRow(2, "bbb")),
        ImmutableList.of(insertRow(1, "bbb"), insertRow(2, "ddd")),
        ImmutableList.of(insertRow(1, "ddd"), insertRow(2, "ddd"))
    );

    testSqlChangeLog(TABLE_NAME, ImmutableList.of("id"), inputRowsPerCheckpoint,
        expectedRecordsPerCheckpoint);
  }

  @Test
  public void testChangeLogOnDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            insertRow(1, "aaa"),
            deleteRow(1, "aaa"),
            insertRow(2, "bbb"),
            insertRow(1, "bbb"),
            insertRow(2, "aaa")
        ),
        ImmutableList.of(
            updateBeforeRow(2, "aaa"),
            updateAfterRow(1, "ccc"),
            insertRow(1, "aaa")
        ),
        ImmutableList.of(
            deleteRow(1, "bbb"),
            insertRow(2, "aaa"),
            insertRow(2, "ccc")
        )
    );

    List<List<Row>> expectedRecords = ImmutableList.of(
        ImmutableList.of(insertRow(1, "bbb"), insertRow(2, "aaa")),
        ImmutableList.of(insertRow(1, "aaa"), insertRow(1, "bbb"), insertRow(1, "ccc")),
        ImmutableList.of(insertRow(1, "aaa"), insertRow(1, "ccc"), insertRow(2, "aaa"), insertRow(2, "ccc"))
    );

    testSqlChangeLog(TABLE_NAME, ImmutableList.of("data"), elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testChangeLogOnIdDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            insertRow(1, "aaa"),
            deleteRow(1, "aaa"),
            insertRow(2, "bbb"),
            insertRow(1, "bbb"),
            insertRow(2, "aaa")
        ),
        ImmutableList.of(
            updateBeforeRow(2, "aaa"),
            updateAfterRow(1, "ccc"),
            insertRow(1, "aaa")
        ),
        ImmutableList.of(
            deleteRow(1, "bbb"),
            insertRow(2, "aaa")
        )
    );

    List<List<Row>> expectedRecords = ImmutableList.of(
        ImmutableList.of(insertRow(1, "bbb"), insertRow(2, "aaa"), insertRow(2, "bbb")),
        ImmutableList.of(insertRow(1, "aaa"), insertRow(1, "bbb"), insertRow(1, "ccc"), insertRow(2, "bbb")),
        ImmutableList.of(insertRow(1, "aaa"), insertRow(1, "ccc"), insertRow(2, "aaa"), insertRow(2, "bbb"))
    );

    testSqlChangeLog(TABLE_NAME, ImmutableList.of("data", "id"), elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testPureInsertOnIdKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            insertRow(1, "aaa"),
            insertRow(2, "bbb")
        ),
        ImmutableList.of(
            insertRow(3, "ccc"),
            insertRow(4, "ddd")
        ),
        ImmutableList.of(
            insertRow(5, "eee"),
            insertRow(6, "fff")
        )
    );

    List<List<Row>> expectedRecords = ImmutableList.of(
        ImmutableList.of(
            insertRow(1, "aaa"),
            insertRow(2, "bbb")
        ),
        ImmutableList.of(
            insertRow(1, "aaa"),
            insertRow(2, "bbb"),
            insertRow(3, "ccc"),
            insertRow(4, "ddd")
        ),
        ImmutableList.of(
            insertRow(1, "aaa"),
            insertRow(2, "bbb"),
            insertRow(3, "ccc"),
            insertRow(4, "ddd"),
            insertRow(5, "eee"),
            insertRow(6, "fff")
        )
    );

    testSqlChangeLog(TABLE_NAME, ImmutableList.of("data"), elementsPerCheckpoint, expectedRecords);
  }

  private static Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  private Table createTable(String tableName, List<String> key, boolean isPartitioned) {
    String partitionByCause = isPartitioned ? "PARTITIONED BY (data)" : "";
    sql("CREATE TABLE %s(id INT, data VARCHAR, PRIMARY KEY(%s) NOT ENFORCED) %s",
        tableName, Joiner.on(',').join(key), partitionByCause);

    // Upgrade the iceberg table to format v2.
    CatalogLoader loader = CatalogLoader.hadoop("my_catalog", CONF, ImmutableMap.of(
        CatalogProperties.WAREHOUSE_LOCATION, warehouse
    ));
    Table table = loader.loadCatalog().loadTable(TableIdentifier.of(DATABASE_NAME, TABLE_NAME));
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  private void testSqlChangeLog(String tableName,
                                List<String> key,
                                List<List<Row>> inputRowsPerCheckpoint,
                                List<List<Row>> expectedRecordsPerCheckpoint) throws Exception {
    String dataId = BoundedTableFactory.registerDataSet(inputRowsPerCheckpoint);
    sql("CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL)" +
        " WITH ('connector'='BoundedSource', 'data-id'='%s')", SOURCE_TABLE, dataId);

    Assert.assertEquals("Should have the expected rows",
        listJoin(inputRowsPerCheckpoint),
        sql("SELECT * FROM %s", SOURCE_TABLE));

    Table table = createTable(tableName, key, partitioned);
    sql("INSERT INTO %s SELECT * FROM %s", tableName, SOURCE_TABLE);

    sql("SELECT * FROM %s", tableName);

    table.refresh();
    List<Snapshot> snapshots = findValidSnapshots(table);
    int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
    Assert.assertEquals("Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

    for (int i = 0; i < expectedSnapshotNum; i++) {
      long snapshotId = snapshots.get(i).snapshotId();
      List<Row> expectedRowss = expectedRecordsPerCheckpoint.get(i);
      Assert.assertEquals("Should have the expected records for the checkpoint#" + i,
          expectedRowSet(table, expectedRowss), actualRowSet(table, snapshotId));
    }

    if (expectedSnapshotNum > 0) {
      Assert.assertEquals("Should have the expected rows in the final table",
          Sets.newHashSet(expectedRecordsPerCheckpoint.get(expectedSnapshotNum - 1)),
          Sets.newHashSet(sql("SELECT * FROM %s", tableName)));
    }
  }

  private List<Snapshot> findValidSnapshots(Table table) {
    List<Snapshot> validSnapshots = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.allManifests().stream().anyMatch(m -> snapshot.snapshotId() == m.snapshotId())) {
        validSnapshots.add(snapshot);
      }
    }
    return validSnapshots;
  }

  private static StructLikeSet expectedRowSet(Table table, List<Row> rows) {
    Record[] records = new Record[rows.size()];
    for (int i = 0; i < records.length; i++) {
      records[i] = record((int) rows.get(i).getField(0), (String) rows.get(i).getField(1));
    }
    return SimpleDataUtil.expectedRowSet(table, records);
  }

  private static StructLikeSet actualRowSet(Table table, long snapshotId) throws IOException {
    return SimpleDataUtil.actualRowSet(table, snapshotId, "*");
  }
}
