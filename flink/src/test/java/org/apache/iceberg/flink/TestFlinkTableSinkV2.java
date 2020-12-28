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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSinkV2 extends FlinkCatalogTestBase {
  private static final String TABLE_NAME_V2 = "test_table_v2";
  private static final int ROW_ID_POS = 0;
  private static final int ROW_DATA_POS = 1;

  private final boolean partitioned;

  private final StreamExecutionEnvironment env;

  private StreamTableEnvironment tEnv;
  private Map<String, String> tableProps;

  @Parameterized.Parameters(name = "CatalogName={0}, Format={1}, Partitioned={2}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {"testhive", "avro", true},
        new Object[] {"testhive", "avro", false},
        new Object[] {"testhive", "parquet", true},
        new Object[] {"testhive", "parquet", false},
        new Object[] {"testhadoop", "avro", true},
        new Object[] {"testhadoop", "avro", false},
        new Object[] {"testhadoop", "parquet", true},
        new Object[] {"testhadoop", "parquet", false}
    );
  }

  public TestFlinkTableSinkV2(String catalogName, String format, Boolean partitioned) {
    super(catalogName, new String[0]);
    this.partitioned = partitioned;

    this.env = StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(400);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    this.tableProps = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format, TableProperties.FORMAT_VERSION, "2");
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          tEnv = StreamTableEnvironment.create(env, EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inStreamingMode()
              .build());
        }
      }
    }
    return tEnv;
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_V2);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testSqlChangeLogOnIdKey() throws Exception {
    List<List<Row>> inputRowsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 1, "bbb"),
            row("+I", 2, "aaa"),
            row("-D", 2, "aaa"),
            row("+I", 2, "bbb")
        ),
        ImmutableList.of(
            row("-U", 2, "bbb"),
            row("+U", 2, "ccc"),
            row("-D", 2, "ccc"),
            row("+I", 2, "ddd")
        ),
        ImmutableList.of(
            row("-D", 1, "bbb"),
            row("+I", 1, "ccc"),
            row("-D", 1, "ccc"),
            row("+I", 1, "ddd")
        )
    );

    List<List<Record>> expectedRecordsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(record(1, "bbb"), record(2, "bbb")),
        ImmutableList.of(record(1, "bbb"), record(2, "ddd")),
        ImmutableList.of(record(1, "ddd"), record(2, "ddd"))
    );

    testSqlChangeLog(ImmutableList.of("id"), row -> Row.of(row.getField(ROW_ID_POS)), inputRowsPerCheckpoint,
        expectedRecordsPerCheckpoint);
  }

  @Test
  public void testChangeLogOnDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 2, "bbb"),
            row("+I", 1, "bbb"),
            row("+I", 2, "aaa")
        ),
        ImmutableList.of(
            row("-U", 2, "aaa"),
            row("+U", 1, "ccc"),
            row("+I", 1, "aaa")
        ),
        ImmutableList.of(
            row("-D", 1, "bbb"),
            row("+I", 2, "aaa"),
            row("+I", 2, "ccc")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "bbb"), record(2, "aaa")),
        ImmutableList.of(record(1, "aaa"), record(1, "bbb"), record(1, "ccc")),
        ImmutableList.of(record(1, "aaa"), record(1, "ccc"), record(2, "aaa"), record(2, "ccc"))
    );

    testSqlChangeLog(ImmutableList.of("data"), row -> Row.of(row.getField(ROW_DATA_POS)), elementsPerCheckpoint,
        expectedRecords);
  }

  @Test
  public void testChangeLogOnIdDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 2, "bbb"),
            row("+I", 1, "bbb"),
            row("+I", 2, "aaa")
        ),
        ImmutableList.of(
            row("-U", 2, "aaa"),
            row("+U", 1, "ccc"),
            row("+I", 1, "aaa")
        ),
        ImmutableList.of(
            row("-D", 1, "bbb"),
            row("+I", 2, "aaa")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "bbb"), record(2, "aaa"), record(2, "bbb")),
        ImmutableList.of(record(1, "aaa"), record(1, "bbb"), record(1, "ccc"), record(2, "bbb")),
        ImmutableList.of(record(1, "aaa"), record(1, "ccc"), record(2, "aaa"), record(2, "bbb"))
    );

    testSqlChangeLog(ImmutableList.of("data", "id"),
        row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testPureInsertOnIdKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("+I", 2, "bbb")
        ),
        ImmutableList.of(
            row("+I", 3, "ccc"),
            row("+I", 4, "ddd")
        ),
        ImmutableList.of(
            row("+I", 5, "eee"),
            row("+I", 6, "fff")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(
            record(1, "aaa"),
            record(2, "bbb")
        ),
        ImmutableList.of(
            record(1, "aaa"),
            record(2, "bbb"),
            record(3, "ccc"),
            record(4, "ddd")
        ),
        ImmutableList.of(
            record(1, "aaa"),
            record(2, "bbb"),
            record(3, "ccc"),
            record(4, "ddd"),
            record(5, "eee"),
            record(6, "fff")
        )
    );

    testSqlChangeLog(ImmutableList.of("data"), row -> Row.of(row.getField(ROW_DATA_POS)), elementsPerCheckpoint,
        expectedRecords);
  }

  private static Row row(String rowKind, int id, String data) {
    return SimpleDataUtil.createRow(rowKind, id, data);
  }

  private Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  private void testSqlChangeLog(List<String> equalityColumns,
                                KeySelector<Row, Row> keySelector,
                                List<List<Row>> inputRowsPerCheckpoint,
                                List<List<Record>> expectedRecordsPerCheckpoint) throws Exception {
    String partitionByCause = partitioned ? "PARTITIONED BY (data)" : "";
    sql("CREATE TABLE %s (id INT, data VARCHAR, PRIMARY KEY(%s) NOT ENFORCED) %s WITH %s",
        TABLE_NAME_V2,
        StringUtils.join(equalityColumns, ","),
        partitionByCause,
        toWithClause(tableProps));

    TableSchema flinkSchema = TableSchema.builder()
        .field("id", DataTypes.INT().notNull())
        .field("data", DataTypes.STRING().notNull())
        .primaryKey(equalityColumns.toArray(new String[0]))
        .build();
    RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();
    DataFormatConverters.RowConverter mapper = new DataFormatConverters.RowConverter(flinkSchema.getFieldDataTypes());

    DataStream<RowData> inputStream =
        env.addSource(new BoundedTestSource<>(inputRowsPerCheckpoint), new RowTypeInfo(flinkSchema.getFieldTypes()))
            .keyBy(keySelector) // Shuffle by key so that different version of same key could be in the correct order.
            .map(mapper::toInternal, RowDataTypeInfo.of(rowType));

    tEnv.createTemporaryView("source_change_logs", tEnv.fromDataStream(inputStream));

    sql("INSERT INTO %s SELECT * FROM source_change_logs", TABLE_NAME_V2);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_V2));
    List<Snapshot> snapshots = findValidSnapshots(table);
    int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
    Assert.assertEquals("Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

    for (int i = 0; i < expectedSnapshotNum; i++) {
      long snapshotId = snapshots.get(i).snapshotId();
      List<Record> expectedRecords = expectedRecordsPerCheckpoint.get(i);
      Assert.assertEquals("Should have the expected records for the checkpoint#" + i,
          expectedRowSet(table, expectedRecords), actualRowSet(table, snapshotId, "*"));
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

  private static StructLikeSet expectedRowSet(Table table, List<Record> records) {
    return SimpleDataUtil.expectedRowSet(table, records.toArray(new Record[0]));
  }

  private static StructLikeSet actualRowSet(Table table, long snapshotId, String... columns) throws IOException {
    return SimpleDataUtil.actualRowSet(table, snapshotId, columns);
  }
}
