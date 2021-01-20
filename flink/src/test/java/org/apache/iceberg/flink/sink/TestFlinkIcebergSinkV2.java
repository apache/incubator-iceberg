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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSinkV2 extends TableTestBase {
  private static final int FORMAT_V2 = 2;
  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

  private static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
      "+I", RowKind.INSERT,
      "-D", RowKind.DELETE,
      "-U", RowKind.UPDATE_BEFORE,
      "+U", RowKind.UPDATE_AFTER);

  private static final int ROW_ID_POS = 0;
  private static final int ROW_DATA_POS = 1;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;

  private StreamExecutionEnvironment env;
  private TestTableLoader tableLoader;

  @Parameterized.Parameters(name = "FileFormat = {0}, Parallelism = {1}, Partitioned={2}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1, true},
        new Object[] {"avro", 1, false},
        new Object[] {"avro", 2, true},
        new Object[] {"avro", 2, false},
        new Object[] {"parquet", 1, true},
        new Object[] {"parquet", 1, false},
        new Object[] {"parquet", 2, true},
        new Object[] {"parquet", 2, false}
    };
  }

  public TestFlinkIcebergSinkV2(String format, int parallelism, boolean partitioned) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    if (!partitioned) {
      table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());
    } else {
      table = create(SimpleDataUtil.SCHEMA, PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build());
    }

    table.updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
        .commit();

    env = StreamExecutionEnvironment.getExecutionEnvironment()
        .enableCheckpointing(100L)
        .setParallelism(parallelism)
        .setMaxParallelism(parallelism);

    tableLoader = new TestTableLoader(tableDir.getAbsolutePath());
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

  private void testChangeLogs(List<String> equalityFieldColumns,
                              KeySelector<Row, Object> keySelector,
                              boolean insertAsUpsert,
                              List<List<Row>> elementsPerCheckpoint,
                              List<List<Record>> expectedRecordsPerCheckpoint) throws Exception {
    DataStream<Row> dataStream = env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);

    // Shuffle by the equality key, so that different operations of the same key could be wrote in order when
    // executing tasks in parallel.
    dataStream = dataStream.keyBy(keySelector);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .equalityFieldColumns(equalityFieldColumns)
        .upsert(insertAsUpsert)
        .build();

    // Execute the program.
    env.execute("Test Iceberg Change-Log DataStream.");

    table.refresh();
    List<Snapshot> snapshots = findValidSnapshots(table);
    int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
    Assert.assertEquals("Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

    for (int i = 0; i < expectedSnapshotNum; i++) {
      long snapshotId = snapshots.get(i).snapshotId();
      List<Record> expectedRecords = expectedRecordsPerCheckpoint.get(i);
      Assert.assertEquals("Should have the expected records for the checkpoint#" + i,
          expectedRowSet(expectedRecords.toArray(new Record[0])), actualRowSet(snapshotId, "*"));
    }
  }

  private Row row(String rowKind, int id, String data) {
    RowKind kind = ROW_KIND_MAP.get(rowKind);
    if (kind == null) {
      throw new IllegalArgumentException("Unknown row kind: " + rowKind);
    }

    return Row.ofKind(kind, id, data);
  }

  private Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  @Test
  public void testChangeLogOnIdKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
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

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "bbb"), record(2, "bbb")),
        ImmutableList.of(record(1, "bbb"), record(2, "ddd")),
        ImmutableList.of(record(1, "ddd"), record(2, "ddd"))
    );

    testChangeLogs(ImmutableList.of("id"), row -> row.getField(ROW_ID_POS), false,
        elementsPerCheckpoint, expectedRecords);
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

    testChangeLogs(ImmutableList.of("data"), row -> row.getField(ROW_DATA_POS), false,
        elementsPerCheckpoint, expectedRecords);
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

    testChangeLogs(ImmutableList.of("data", "id"), row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        false, elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testChangeLogOnSameKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        // Checkpoint #1
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 1, "aaa")
        ),
        // Checkpoint #2
        ImmutableList.of(
            row("-U", 1, "aaa"),
            row("+U", 1, "aaa")
        ),
        // Checkpoint #3
        ImmutableList.of(
            row("-D", 1, "aaa"),
            row("+I", 1, "aaa")
        ),
        // Checkpoint #4
        ImmutableList.of(
            row("-U", 1, "aaa"),
            row("+U", 1, "aaa"),
            row("+I", 1, "aaa")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "aaa")),
        ImmutableList.of(record(1, "aaa")),
        ImmutableList.of(record(1, "aaa")),
        ImmutableList.of(record(1, "aaa"), record(1, "aaa"))
    );

    testChangeLogs(ImmutableList.of("id", "data"), row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        false, elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testUpsertOnIdKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("+U", 1, "bbb")
        ),
        ImmutableList.of(
            row("+I", 1, "ccc")
        ),
        ImmutableList.of(
            row("+U", 1, "ddd"),
            row("+I", 1, "eee")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "bbb")),
        ImmutableList.of(record(1, "ccc")),
        ImmutableList.of(record(1, "eee"))
    );

    if (!partitioned) {
      testChangeLogs(ImmutableList.of("id"), row -> row.getField(ROW_ID_POS), true,
          elementsPerCheckpoint, expectedRecords);
    } else {
      AssertHelpers.assertThrows("Should be error because equality field columns don't include all partition keys",
          IllegalStateException.class, "not included in equality fields",
          () -> {
            testChangeLogs(ImmutableList.of("id"), row -> row.getField(ROW_ID_POS), true, elementsPerCheckpoint,
                expectedRecords);
            return null;
          });
    }
  }

  @Test
  public void testUpsertOnDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("+I", 2, "aaa"),
            row("+I", 3, "bbb")
        ),
        ImmutableList.of(
            row("+U", 4, "aaa"),
            row("-U", 3, "bbb"),
            row("+U", 5, "bbb")
        ),
        ImmutableList.of(
            row("+I", 6, "aaa"),
            row("+U", 7, "bbb")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(2, "aaa"), record(3, "bbb")),
        ImmutableList.of(record(4, "aaa"), record(5, "bbb")),
        ImmutableList.of(record(6, "aaa"), record(7, "bbb"))
    );

    testChangeLogs(ImmutableList.of("data"), row -> row.getField(ROW_DATA_POS), true,
        elementsPerCheckpoint, expectedRecords);
  }

  @Test
  public void testUpsertOnIdDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("+U", 1, "aaa"),
            row("+I", 2, "bbb")
        ),
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 2, "bbb"),
            row("+I", 2, "ccc")
        ),
        ImmutableList.of(
            row("-U", 1, "aaa"),
            row("+U", 1, "bbb")
        )
    );

    List<List<Record>> expectedRecords = ImmutableList.of(
        ImmutableList.of(record(1, "aaa"), record(2, "bbb")),
        ImmutableList.of(record(1, "aaa"), record(2, "ccc")),
        ImmutableList.of(record(1, "bbb"), record(2, "ccc"))
    );

    testChangeLogs(ImmutableList.of("id", "data"), row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        true, elementsPerCheckpoint, expectedRecords);
  }

  private StructLikeSet expectedRowSet(Record... records) {
    return SimpleDataUtil.expectedRowSet(table, records);
  }

  private StructLikeSet actualRowSet(long snapshotId, String... columns) throws IOException {
    table.refresh();
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
        .useSnapshot(snapshotId)
        .select(columns)
        .build()) {
      reader.forEach(set::add);
    }
    return set;
  }
}
