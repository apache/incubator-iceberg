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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.HashMultiset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

public class SimpleDataUtil {

  private SimpleDataUtil() {
  }

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("id", DataTypes.INT())
      .field("data", DataTypes.STRING())
      .build();

  public static final RowType ROW_TYPE = (RowType) FLINK_SCHEMA.toRowDataType().getLogicalType();

  public static final Record RECORD = GenericRecord.create(SCHEMA);

  public static Table createTable(String path, Map<String, String> properties, boolean partitioned) {
    PartitionSpec spec;
    if (partitioned) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }
    return new HadoopTables().create(SCHEMA, spec, properties, path);
  }

  public static Record createRecord(Integer id, String data) {
    Record record = RECORD.copy();
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
      "+I", RowKind.INSERT,
      "-D", RowKind.DELETE,
      "-U", RowKind.UPDATE_BEFORE,
      "+U", RowKind.UPDATE_AFTER);

  public static Row createRow(String rowKind, int id, String data) {
    RowKind kind = ROW_KIND_MAP.get(rowKind);
    if (kind == null) {
      throw new IllegalArgumentException("Unknown row kind: " + rowKind);
    }

    return Row.ofKind(kind, id, data);
  }

  public static RowData createRowData(Integer id, String data) {
    return GenericRowData.of(id, StringData.fromString(data));
  }

  public static RowData createInsert(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.INSERT, id, StringData.fromString(data));
  }

  public static RowData createDelete(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.DELETE, id, StringData.fromString(data));
  }

  public static RowData createUpdateBefore(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.UPDATE_BEFORE, id, StringData.fromString(data));
  }

  public static RowData createUpdateAfter(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.UPDATE_AFTER, id, StringData.fromString(data));
  }

  public static DataFile writeFile(Schema schema, PartitionSpec spec, Configuration conf,
                                   String location, String filename, List<RowData> rows)
      throws IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    FileAppenderFactory<RowData> appenderFactory =
        new FlinkAppenderFactory(schema, flinkSchema, ImmutableMap.of(), spec);

    FileAppender<RowData> appender = appenderFactory.newAppender(fromPath(path, conf), fileFormat);
    try (FileAppender<RowData> closeableAppender = appender) {
      closeableAppender.addAll(rows);
    }

    return DataFiles.builder(spec)
        .withInputFile(HadoopInputFile.fromPath(path, conf))
        .withMetrics(appender.metrics())
        .build();
  }

  public static DeleteFile writeEqDeleteFile(Table table, FileFormat format, String tablePath, String filename,
                                             FileAppenderFactory<RowData> appenderFactory,
                                             List<RowData> deletes) throws IOException {
    EncryptedOutputFile outputFile =
        table.encryption().encrypt(fromPath(new Path(tablePath, filename), new Configuration()));

    EqualityDeleteWriter<RowData> eqWriter = appenderFactory.newEqDeleteWriter(outputFile, format, null);
    try (EqualityDeleteWriter<RowData> writer = eqWriter) {
      writer.deleteAll(deletes);
    }
    return eqWriter.toDeleteFile();
  }

  public static DeleteFile writePosDeleteFile(Table table, FileFormat format, String tablePath,
                                              String filename,
                                              FileAppenderFactory<RowData> appenderFactory,
                                              List<Pair<CharSequence, Long>> positions) throws IOException {
    EncryptedOutputFile outputFile =
        table.encryption().encrypt(fromPath(new Path(tablePath, filename), new Configuration()));

    PositionDeleteWriter<RowData> posWriter = appenderFactory.newPosDeleteWriter(outputFile, format, null);
    try (PositionDeleteWriter<RowData> writer = posWriter) {
      for (Pair<CharSequence, Long> p : positions) {
        writer.delete(p.first(), p.second());
      }
    }
    return posWriter.toDeleteFile();
  }

  private static List<Record> convertToRecords(List<RowData> rows) {
    List<Record> records = Lists.newArrayList();
    for (RowData row : rows) {
      Integer id = row.isNullAt(0) ? null : row.getInt(0);
      String data = row.isNullAt(1) ? null : row.getString(1).toString();
      records.add(createRecord(id, data));
    }
    return records;
  }

  public static void assertTableRows(String tablePath, List<RowData> expected) throws IOException {
    assertTableRecords(tablePath, convertToRecords(expected));
  }

  public static void assertTableRows(Table table, List<RowData> expected) throws IOException {
    assertTableRecords(table, convertToRecords(expected));
  }

  public static void assertTableRecords(Table table, List<Record> expected) throws IOException {
    table.refresh();
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      Assert.assertEquals("Should produce the expected record",
          HashMultiset.create(expected), HashMultiset.create(iterable));
    }
  }

  public static void assertTableRecords(String tablePath, List<Record> expected) throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    assertTableRecords(new HadoopTables().load(tablePath), expected);
  }

  public static StructLikeSet expectedRowSet(Table table, Record... records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    Collections.addAll(set, records);
    return set;
  }

  public static StructLikeSet actualRowSet(Table table, String... columns) throws IOException {
    table.refresh();
    return actualRowSet(table, table.currentSnapshot().snapshotId(), columns);
  }

  public static StructLikeSet actualRowSet(Table table, long snapshotId, String... columns) throws IOException {
    table.refresh();
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics
        .read(table)
        .useSnapshot(snapshotId)
        .select(columns)
        .build()) {
      reader.forEach(set::add);
    }
    return set;
  }
}
