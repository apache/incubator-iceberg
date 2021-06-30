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

package org.apache.iceberg.arrow.vectorized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link ArrowReader}.
 * <p>All tests create a table with monthly partitions and write 1 year of data to the table.
 */
public class ArrowReaderTest {

  private static final int NUM_ROWS_PER_MONTH = 20;
  private static final ImmutableList<String> ALL_COLUMNS =
      ImmutableList.of(
          "timestamp",
          "timestamp_nullable",
          "boolean",
          "boolean_nullable",
          "int",
          "int_nullable",
          "long",
          "long_nullable",
          "float",
          "float_nullable",
          "double",
          "double_nullable",
          "timestamp_tz",
          "timestamp_tz_nullable",
          "string",
          "string_nullable",
          "bytes",
          "bytes_nullable",
          "date",
          "date_nullable",
          "int_promotion",
          "time",
          "time_nullable",
          "uuid",
          "uuid_nullable"
      );

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private HadoopTables tables;

  private String tableLocation;
  private List<GenericRecord> rowsWritten;

  /**
   * Read all rows and columns from the table without any filter. The test asserts that the Arrow {@link
   * VectorSchemaRoot} contains the expected schema and expected vector types. Then the test asserts that the vectors
   * contains expected values. The test also asserts the total number of rows match the expected value.
   */
  @Test
  public void testReadAll() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    readAndCheckQueryResult(table.newScan(), NUM_ROWS_PER_MONTH, 12 * NUM_ROWS_PER_MONTH, ALL_COLUMNS);
  }

  /**
   * This test writes each partition with constant value rows. The Arrow vectors returned are mostly of type int32 which
   * is unexpected. This is happening because of dictionary encoding at the storage level.
   * <p>
   * Following are the expected and actual Arrow schema:
   * <pre>
   * Expected Arrow Schema:
   * timestamp: Timestamp(MICROSECOND, null) not null,
   * timestamp_nullable: Timestamp(MICROSECOND, null),
   * boolean: Bool not null,
   * boolean_nullable: Bool,
   * int: Int(32, true) not null,
   * int_nullable: Int(32, true),
   * long: Int(64, true) not null,
   * long_nullable: Int(64, true),
   * float: FloatingPoint(SINGLE) not null,
   * float_nullable: FloatingPoint(SINGLE),
   * double: FloatingPoint(DOUBLE) not null,
   * double_nullable: FloatingPoint(DOUBLE),
   * timestamp_tz: Timestamp(MICROSECOND, UTC) not null,
   * timestamp_tz_nullable: Timestamp(MICROSECOND, UTC),
   * string: Utf8 not null,
   * string_nullable: Utf8,
   * bytes: Binary not null,
   * bytes_nullable: Binary,
   * date: Date(DAY) not null,
   * date_nullable: Date(DAY),
   * int_promotion: Int(32, true) not null
   *
   * Actual Arrow Schema:
   * timestamp: Int(32, true) not null,
   * timestamp_nullable: Int(32, true),
   * boolean: Bool not null,
   * boolean_nullable: Bool,
   * int: Int(32, true) not null,
   * int_nullable: Int(32, true),
   * long: Int(32, true) not null,
   * long_nullable: Int(32, true),
   * float: Int(32, true) not null,
   * float_nullable: Int(32, true),
   * double: Int(32, true) not null,
   * double_nullable: Int(32, true),
   * timestamp_tz: Int(32, true) not null,
   * timestamp_tz_nullable: Int(32, true),
   * string: Int(32, true) not null,
   * string_nullable: Int(32, true),
   * bytes: Int(32, true) not null,
   * bytes_nullable: Int(32, true),
   * date: Date(DAY) not null,
   * date_nullable: Date(DAY),
   * int_promotion: Int(32, true) not null
   * </pre>
   * <p>
   * TODO: fix the returned Arrow vectors to have vector types consistent with Iceberg types.
   * <p>
   * Read all rows and columns from the table without any filter. The test asserts that the Arrow {@link
   * VectorSchemaRoot} contains the expected schema and expected vector types. Then the test asserts that the vectors
   * contains expected values. The test also asserts the total number of rows match the expected value.
   */
  @Test
  @Ignore
  public void testReadAllWithConstantRecords() throws Exception {
    writeTableWithConstantRecords();
    Table table = tables.load(tableLocation);
    readAndCheckQueryResult(table.newScan(), NUM_ROWS_PER_MONTH, 12 * NUM_ROWS_PER_MONTH, ALL_COLUMNS);
  }

  /**
   * Read all rows and columns from the table without any filter. The test uses a batch size smaller than the number of
   * rows in a partition. The test asserts that the Arrow {@link VectorSchemaRoot} contains the expected schema and
   * expected vector types. Then the test asserts that the vectors contains expected values. The test also asserts the
   * total number of rows match the expected value.
   */
  @Test
  public void testReadAllWithSmallerBatchSize() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    TableScan scan = table.newScan();
    readAndCheckQueryResult(scan, 10, 12 * NUM_ROWS_PER_MONTH, ALL_COLUMNS);
  }

  /**
   * Read selected rows and all columns from the table using a time range row filter. The test asserts that the Arrow
   * {@link VectorSchemaRoot} contains the expected schema and expected vector types. Then the test asserts that the
   * vectors contains expected values. The test also asserts the total number of rows match the expected value.
   */
  @Test
  public void testReadRangeFilter() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    LocalDateTime beginTime = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
    LocalDateTime endTime = LocalDateTime.of(2020, 2, 1, 0, 0, 0);
    TableScan scan = table.newScan()
        .filter(Expressions.and(
            Expressions.greaterThanOrEqual("timestamp", timestampToMicros(beginTime)),
            Expressions.lessThan("timestamp", timestampToMicros(endTime))));
    readAndCheckQueryResult(scan, NUM_ROWS_PER_MONTH, NUM_ROWS_PER_MONTH, ALL_COLUMNS);
  }

  /**
   * Read selected rows and all columns from the table using a time range row filter. The test asserts that the result
   * is empty.
   */
  @Test
  public void testReadRangeFilterEmptyResult() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    LocalDateTime beginTime = LocalDateTime.of(2021, 1, 1, 0, 0, 0);
    LocalDateTime endTime = LocalDateTime.of(2021, 2, 1, 0, 0, 0);
    TableScan scan = table.newScan()
        .filter(Expressions.and(
            Expressions.greaterThanOrEqual("timestamp", timestampToMicros(beginTime)),
            Expressions.lessThan("timestamp", timestampToMicros(endTime))));
    int numRoots = 0;
    try (VectorizedTableScanIterable itr = new VectorizedTableScanIterable(scan, NUM_ROWS_PER_MONTH, false)) {
      for (ColumnarBatch batch : itr) {
        numRoots++;
      }
    }
    assertEquals(0, numRoots);
  }

  /**
   * Read all rows and selected columns from the table with a column selection filter. The test asserts that the Arrow
   * {@link VectorSchemaRoot} contains the expected schema and expected vector types. Then the test asserts that the
   * vectors contains expected values. The test also asserts the total number of rows match the expected value.
   */
  @Test
  public void testReadColumnFilter1() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    TableScan scan = table.newScan()
        .select("timestamp", "int", "string");
    readAndCheckQueryResult(
        scan, NUM_ROWS_PER_MONTH, 12 * NUM_ROWS_PER_MONTH,
        ImmutableList.of("timestamp", "int", "string"));
  }

  /**
   * Read all rows and a single column from the table with a column selection filter. The test asserts that the Arrow
   * {@link VectorSchemaRoot} contains the expected schema and expected vector types. Then the test asserts that the
   * vectors contains expected values. The test also asserts the total number of rows match the expected value.
   */
  @Test
  public void testReadColumnFilter2() throws Exception {
    writeTableWithIncrementalRecords();
    Table table = tables.load(tableLocation);
    TableScan scan = table.newScan()
        .select("timestamp");
    readAndCheckQueryResult(
        scan, NUM_ROWS_PER_MONTH, 12 * NUM_ROWS_PER_MONTH,
        ImmutableList.of("timestamp"));
  }

  /**
   * Run the following verifications:
   * <ol>
   *   <li>Read the data and verify that the returned ColumnarBatches match expected rows.</li>
   *   <li>Read the data and verify that the returned Arrow VectorSchemaRoots match expected rows.</li>
   * </ol>
   */
  private void readAndCheckQueryResult(
      TableScan scan,
      int numRowsPerRoot,
      int expectedTotalRows,
      List<String> columns) throws IOException {
    // Read the data and verify that the returned ColumnarBatches match expected rows.
    readAndCheckColumnarBatch(scan, numRowsPerRoot, columns);
    // Read the data and verify that the returned Arrow VectorSchemaRoots match expected rows.
    readAndCheckArrowResult(scan, numRowsPerRoot, expectedTotalRows, columns);
  }

  private void readAndCheckColumnarBatch(
      TableScan scan,
      int numRowsPerRoot,
      List<String> columns) throws IOException {
    int rowIndex = 0;
    try (VectorizedTableScanIterable itr = new VectorizedTableScanIterable(scan, numRowsPerRoot, false)) {
      for (ColumnarBatch batch : itr) {
        List<GenericRecord> expectedRows = rowsWritten.subList(rowIndex, rowIndex + numRowsPerRoot);
        checkColumnarBatch(numRowsPerRoot, expectedRows, batch, columns);
        rowIndex += numRowsPerRoot;
      }
    }
  }

  private void readAndCheckArrowResult(
      TableScan scan,
      int numRowsPerRoot,
      int expectedTotalRows,
      List<String> columns) throws IOException {
    Set<String> columnSet = ImmutableSet.copyOf(columns);
    int rowIndex = 0;
    int totalRows = 0;
    try (VectorizedTableScanIterable itr = new VectorizedTableScanIterable(scan, numRowsPerRoot, false)) {
      for (ColumnarBatch batch : itr) {
        List<GenericRecord> expectedRows = rowsWritten.subList(rowIndex, rowIndex + numRowsPerRoot);
        VectorSchemaRoot root = batch.createVectorSchemaRootFromVectors();
        assertEquals(createExpectedArrowSchema(columnSet), root.getSchema());
        checkAllVectorTypes(root, columnSet);
        checkAllVectorValues(numRowsPerRoot, expectedRows, root, columnSet);
        rowIndex += numRowsPerRoot;
        totalRows += root.getRowCount();
      }
    }
    assertEquals(expectedTotalRows, totalRows);
  }

  @SuppressWarnings("MethodLength")
  private void checkColumnarBatch(
      int expectedNumRows,
      List<GenericRecord> expectedRows,
      ColumnarBatch batch,
      List<String> columns) {

    Map<String, Integer> columnNameToIndex = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      columnNameToIndex.put(columns.get(i), i);
    }
    Set<String> columnSet = columnNameToIndex.keySet();

    assertEquals(expectedNumRows, batch.numRows());
    assertEquals(columns.size(), batch.numCols());

    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("timestamp"),
        columnSet, "timestamp",
        (records, i) -> records.get(i).getField("timestamp"),
        (array, i) -> timestampFromMicros(array.getLong(i))
    );

    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("timestamp_nullable"),
        columnSet, "timestamp_nullable",
        (records, i) -> records.get(i).getField("timestamp_nullable"),
        (array, i) -> timestampFromMicros(array.getLong(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("boolean"),
        columnSet, "boolean",
        (records, i) -> records.get(i).getField("boolean"),
        ColumnVector::getBoolean
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("boolean_nullable"),
        columnSet, "boolean_nullable",
        (records, i) -> records.get(i).getField("boolean_nullable"),
        ColumnVector::getBoolean
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("int"),
        columnSet, "int",
        (records, i) -> records.get(i).getField("int"),
        ColumnVector::getInt
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("int_nullable"),
        columnSet, "int_nullable",
        (records, i) -> records.get(i).getField("int_nullable"),
        ColumnVector::getInt
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("long"),
        columnSet, "long",
        (records, i) -> records.get(i).getField("long"),
        ColumnVector::getLong
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("long_nullable"),
        columnSet, "long_nullable",
        (records, i) -> records.get(i).getField("long_nullable"),
        ColumnVector::getLong
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("float"),
        columnSet, "float",
        (records, i) -> Float.floatToIntBits((float) records.get(i).getField("float")),
        (array, i) -> Float.floatToIntBits(array.getFloat(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("float_nullable"),
        columnSet, "float_nullable",
        (records, i) -> Float.floatToIntBits((float) records.get(i).getField("float_nullable")),
        (array, i) -> Float.floatToIntBits(array.getFloat(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("double"),
        columnSet, "double",
        (records, i) -> Double.doubleToLongBits((double) records.get(i).getField("double")),
        (array, i) -> Double.doubleToLongBits(array.getDouble(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("double_nullable"),
        columnSet, "double_nullable",
        (records, i) -> Double.doubleToLongBits((double) records.get(i).getField("double_nullable")),
        (array, i) -> Double.doubleToLongBits(array.getDouble(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("timestamp_tz"),
        columnSet, "timestamp_tz",
        (records, i) -> timestampToMicros((OffsetDateTime) records.get(i).getField("timestamp_tz")),
        ColumnVector::getLong
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("timestamp_tz_nullable"),
        columnSet, "timestamp_tz_nullable",
        (records, i) -> timestampToMicros((OffsetDateTime) records.get(i).getField("timestamp_tz_nullable")),
        ColumnVector::getLong
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("string"),
        columnSet, "string",
        (records, i) -> records.get(i).getField("string"),
        ColumnVector::getString
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("string_nullable"),
        columnSet, "string_nullable",
        (records, i) -> records.get(i).getField("string_nullable"),
        ColumnVector::getString
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("bytes"),
        columnSet, "bytes",
        (records, i) -> records.get(i).getField("bytes"),
        (array, i) -> ByteBuffer.wrap(array.getBinary(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("bytes_nullable"),
        columnSet, "bytes_nullable",
        (records, i) -> records.get(i).getField("bytes_nullable"),
        (array, i) -> ByteBuffer.wrap(array.getBinary(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("date"),
        columnSet, "date",
        (records, i) -> records.get(i).getField("date"),
        (array, i) -> dateFromDay(array.getInt(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("date_nullable"),
        columnSet, "date_nullable",
        (records, i) -> records.get(i).getField("date_nullable"),
        (array, i) -> dateFromDay(array.getInt(i))
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("int_promotion"),
        columnSet, "int_promotion",
        (records, i) -> records.get(i).getField("int_promotion"),
        ColumnVector::getInt
    );

    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("uuid"),
        columnSet, "uuid",
        (records, i) -> records.get(i).getField("uuid"),
        ColumnVector::getBinary

    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("uuid_nullable"),
        columnSet, "uuid_nullable",
        (records, i) -> records.get(i).getField("uuid_nullable"),
        ColumnVector::getBinary
    );

    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("time"),
        columnSet, "time",
        (records, i) -> records.get(i).getField("time"),
        (array, i) -> LocalTime.ofNanoOfDay(array.getLong(i) * 1000)
    );
    checkColumnarArrayValues(
        expectedNumRows, expectedRows, batch, columnNameToIndex.get("time_nullable"),
        columnSet, "time_nullable",
        (records, i) -> records.get(i).getField("time_nullable"),
        (array, i) -> LocalTime.ofNanoOfDay(array.getLong(i) * 1000)
    );
  }

  private static void checkColumnarArrayValues(
      int expectedNumRows,
      List<GenericRecord> expectedRows,
      ColumnarBatch columnBatch,
      Integer columnIndex,
      Set<String> columnSet,
      String columnName,
      BiFunction<List<GenericRecord>, Integer, Object> expectedValueExtractor,
      BiFunction<ColumnVector, Integer, Object> vectorValueExtractor) {
    if (columnSet.contains(columnName)) {
      ColumnVector columnVector = columnBatch.column(columnIndex);
      for (int i = 0; i < expectedNumRows; i++) {
        Object expectedValue = expectedValueExtractor.apply(expectedRows, i);
        Object actualValue = vectorValueExtractor.apply(columnVector, i);
        // we need to use assertThat() here because it does a java.util.Objects.deepEquals() and that
        // is relevant for byte[]
        Assertions.assertThat(actualValue).as("Row#" + i + " mismatches").isEqualTo(expectedValue);
      }
    }
  }

  private void writeTableWithConstantRecords() throws Exception {
    writeTable(true);
  }

  private void writeTableWithIncrementalRecords() throws Exception {
    writeTable(false);
  }

  private void writeTable(boolean constantRecords) throws Exception {
    rowsWritten = new ArrayList<>();
    tables = new HadoopTables();
    tableLocation = temp.newFolder("test").toString();

    Schema schema = new Schema(
        Types.NestedField.required(1, "timestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(2, "timestamp_nullable", Types.TimestampType.withoutZone()),
        Types.NestedField.required(3, "boolean", Types.BooleanType.get()),
        Types.NestedField.optional(4, "boolean_nullable", Types.BooleanType.get()),
        Types.NestedField.required(5, "int", Types.IntegerType.get()),
        Types.NestedField.optional(6, "int_nullable", Types.IntegerType.get()),
        Types.NestedField.required(7, "long", Types.LongType.get()),
        Types.NestedField.optional(8, "long_nullable", Types.LongType.get()),
        Types.NestedField.required(9, "float", Types.FloatType.get()),
        Types.NestedField.optional(10, "float_nullable", Types.FloatType.get()),
        Types.NestedField.required(11, "double", Types.DoubleType.get()),
        Types.NestedField.optional(12, "double_nullable", Types.DoubleType.get()),
        Types.NestedField.required(13, "timestamp_tz", Types.TimestampType.withZone()),
        Types.NestedField.optional(14, "timestamp_tz_nullable", Types.TimestampType.withZone()),
        Types.NestedField.required(15, "string", Types.StringType.get()),
        Types.NestedField.optional(16, "string_nullable", Types.StringType.get()),
        Types.NestedField.required(17, "bytes", Types.BinaryType.get()),
        Types.NestedField.optional(18, "bytes_nullable", Types.BinaryType.get()),
        Types.NestedField.required(19, "date", Types.DateType.get()),
        Types.NestedField.optional(20, "date_nullable", Types.DateType.get()),
        Types.NestedField.required(21, "int_promotion", Types.IntegerType.get()),
        Types.NestedField.required(22, "time", Types.TimeType.get()),
        Types.NestedField.optional(23, "time_nullable", Types.TimeType.get()),
        Types.NestedField.required(24, "uuid", Types.UUIDType.get()),
        Types.NestedField.optional(25, "uuid_nullable", Types.UUIDType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .month("timestamp")
        .build();

    Table table = tables.create(schema, spec, tableLocation);

    OverwriteFiles overwrite = table.newOverwrite();
    for (int i = 1; i <= 12; i++) {
      final List<GenericRecord> records;
      if (constantRecords) {
        records = createConstantRecordsForDate(
            table.schema(), LocalDateTime.of(2020, i, 1, 0, 0, 0)
        );
      } else {
        records = createIncrementalRecordsForDate(
            table.schema(), LocalDateTime.of(2020, i, 1, 0, 0, 0)
        );
      }
      overwrite.addFile(writeParquetFile(table, records));
    }
    overwrite.commit();

    // Perform a type promotion
    // TODO: The read Arrow vector should of type BigInt (promoted type) but it is Int (old type).
    Table tableLatest = tables.load(tableLocation);
    tableLatest.updateSchema()
        .updateColumn("int_promotion", Types.LongType.get())
        .commit();
  }

  private static org.apache.arrow.vector.types.pojo.Schema createExpectedArrowSchema(Set<String> columnSet) {
    List<Field> allFields = ImmutableList.of(
        new Field(
            "timestamp", new FieldType(false, MinorType.TIMESTAMPMICRO.getType(), null), null),
        new Field(
            "timestamp_nullable", new FieldType(true, MinorType.TIMESTAMPMICRO.getType(), null), null),
        new Field(
            "boolean", new FieldType(false, MinorType.BIT.getType(), null), null),
        new Field(
            "boolean_nullable", new FieldType(true, MinorType.BIT.getType(), null), null),
        new Field(
            "int", new FieldType(false, MinorType.INT.getType(), null), null),
        new Field(
            "int_nullable", new FieldType(true, MinorType.INT.getType(), null), null),
        new Field(
            "long", new FieldType(false, MinorType.BIGINT.getType(), null), null),
        new Field(
            "long_nullable", new FieldType(true, MinorType.BIGINT.getType(), null), null),
        new Field(
            "float", new FieldType(false, MinorType.FLOAT4.getType(), null), null),
        new Field(
            "float_nullable", new FieldType(true, MinorType.FLOAT4.getType(), null), null),
        new Field(
            "double", new FieldType(false, MinorType.FLOAT8.getType(), null), null),
        new Field(
            "double_nullable", new FieldType(true, MinorType.FLOAT8.getType(), null), null),
        new Field(
            "timestamp_tz", new FieldType(false, new ArrowType.Timestamp(
            org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"), null), null),
        new Field(
            "timestamp_tz_nullable", new FieldType(true, new ArrowType.Timestamp(
            org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"), null), null),
        new Field(
            "string", new FieldType(false, MinorType.VARCHAR.getType(), null), null),
        new Field(
            "string_nullable", new FieldType(true, MinorType.VARCHAR.getType(), null), null),
        new Field(
            "bytes", new FieldType(false, MinorType.VARBINARY.getType(), null), null),
        new Field(
            "bytes_nullable", new FieldType(true, MinorType.VARBINARY.getType(), null), null),
        new Field(
            "date", new FieldType(false, MinorType.DATEDAY.getType(), null), null),
        new Field(
            "date_nullable", new FieldType(true, MinorType.DATEDAY.getType(), null), null),
        new Field(
            "int_promotion", new FieldType(false, MinorType.INT.getType(), null), null),
        new Field(
            "time", new FieldType(false, MinorType.TIMEMICRO.getType(), null), null),
        new Field(
            "time_nullable", new FieldType(true, MinorType.TIMEMICRO.getType(), null), null),
        new Field(
            "uuid", new FieldType(false, new ArrowType.FixedSizeBinary(16), null), null),
        new Field(
            "uuid_nullable", new FieldType(true, new ArrowType.FixedSizeBinary(16), null), null)
    );
    List<Field> filteredFields = allFields.stream()
        .filter(f -> columnSet.contains(f.getName()))
        .collect(Collectors.toList());
    return new org.apache.arrow.vector.types.pojo.Schema(filteredFields);
  }

  private List<GenericRecord> createIncrementalRecordsForDate(Schema schema, LocalDateTime datetime) {
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS_PER_MONTH; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("timestamp", datetime.plus(i, ChronoUnit.DAYS));
      rec.setField("timestamp_nullable", datetime.plus(i, ChronoUnit.DAYS));
      rec.setField("boolean", i % 2 == 0);
      rec.setField("boolean_nullable", i % 2 == 0);
      rec.setField("int", i);
      rec.setField("int_nullable", i);
      rec.setField("long", (long) i * 2);
      rec.setField("long_nullable", (long) i * 2);
      rec.setField("float", (float) i * 3);
      rec.setField("float_nullable", (float) i * 3);
      rec.setField("double", (double) i * 4);
      rec.setField("double_nullable", (double) i * 4);
      rec.setField("timestamp_tz", datetime.plus(i, ChronoUnit.MINUTES).atOffset(ZoneOffset.UTC));
      rec.setField("timestamp_tz_nullable", datetime.plus(i, ChronoUnit.MINUTES).atOffset(ZoneOffset.UTC));
      rec.setField("string", "String-" + i);
      rec.setField("string_nullable", "String-" + i);
      rec.setField("bytes", ByteBuffer.wrap(("Bytes-" + i).getBytes(StandardCharsets.UTF_8)));
      rec.setField("bytes_nullable", ByteBuffer.wrap(("Bytes-" + i).getBytes(StandardCharsets.UTF_8)));
      rec.setField("date", LocalDate.of(2020, 1, 1).plus(i, ChronoUnit.DAYS));
      rec.setField("date_nullable", LocalDate.of(2020, 1, 1).plus(i, ChronoUnit.DAYS));
      rec.setField("int_promotion", i);
      rec.setField("time", LocalTime.of(11, i));
      rec.setField("time_nullable", LocalTime.of(11, i));
      ByteBuffer bb = UUIDUtil.convertToByteBuffer(UUID.randomUUID());
      byte[] uuid = bb.array();
      rec.setField("uuid", uuid);
      rec.setField("uuid_nullable", uuid);
      records.add(rec);
    }
    return records;
  }

  private List<GenericRecord> createConstantRecordsForDate(Schema schema, LocalDateTime datetime) {
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS_PER_MONTH; i++) {
      GenericRecord rec = GenericRecord.create(schema);
      rec.setField("timestamp", datetime);
      rec.setField("timestamp_nullable", datetime);
      rec.setField("boolean", true);
      rec.setField("boolean_nullable", true);
      rec.setField("int", 1);
      rec.setField("int_nullable", 1);
      rec.setField("long", 2L);
      rec.setField("long_nullable", 2L);
      rec.setField("float", 3.0f);
      rec.setField("float_nullable", 3.0f);
      rec.setField("double", 4.0);
      rec.setField("double_nullable", 4.0);
      rec.setField("timestamp_tz", datetime.atOffset(ZoneOffset.UTC));
      rec.setField("timestamp_tz_nullable", datetime.atOffset(ZoneOffset.UTC));
      rec.setField("string", "String");
      rec.setField("string_nullable", "String");
      rec.setField("bytes", ByteBuffer.wrap("Bytes".getBytes(StandardCharsets.UTF_8)));
      rec.setField("bytes_nullable", ByteBuffer.wrap("Bytes".getBytes(StandardCharsets.UTF_8)));
      rec.setField("date", LocalDate.of(2020, 1, 1));
      rec.setField("date_nullable", LocalDate.of(2020, 1, 1));
      rec.setField("int_promotion", 1);
      rec.setField("time", LocalTime.of(11, 30));
      rec.setField("time_nullable", LocalTime.of(11, 30));
      ByteBuffer bb = UUIDUtil.convertToByteBuffer(UUID.fromString("abcd91cf-08d0-4223-b145-f64030b3077f"));
      byte[] uuid = bb.array();
      rec.setField("uuid", uuid);
      rec.setField("uuid_nullable", uuid);
      records.add(rec);
    }
    return records;
  }

  private DataFile writeParquetFile(Table table, List<GenericRecord> records) throws IOException {
    rowsWritten.addAll(records);
    File parquetFile = temp.newFile();
    assertTrue(parquetFile.delete());
    FileAppender<GenericRecord> appender = Parquet.write(Files.localOutput(parquetFile))
        .schema(table.schema())
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(new LocalDateTimeToLongMicros(records.get(0)));

    return DataFiles.builder(table.spec())
        .withPartition(partitionKey)
        .withInputFile(localInput(parquetFile))
        .withMetrics(appender.metrics())
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  private static long timestampToMicros(LocalDateTime value) {
    Instant instant = value.toInstant(ZoneOffset.UTC);
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static long timestampToMicros(OffsetDateTime value) {
    Instant instant = value.toInstant();
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static LocalDateTime timestampFromMicros(long micros) {
    return LocalDateTime.ofEpochSecond(
        TimeUnit.MICROSECONDS.toSeconds(micros),
        (int) TimeUnit.MICROSECONDS.toNanos(micros % 1000),
        ZoneOffset.UTC
    );
  }

  private static LocalDate dateFromDay(int day) {
    return LocalDate.ofEpochDay(day);
  }

  private void checkAllVectorTypes(VectorSchemaRoot root, Set<String> columnSet) {
    assertEqualsForField(root, columnSet, "timestamp", TimeStampMicroVector.class);
    assertEqualsForField(root, columnSet, "timestamp_nullable", TimeStampMicroVector.class);
    assertEqualsForField(root, columnSet, "boolean", BitVector.class);
    assertEqualsForField(root, columnSet, "boolean_nullable", BitVector.class);
    assertEqualsForField(root, columnSet, "int", IntVector.class);
    assertEqualsForField(root, columnSet, "int_nullable", IntVector.class);
    assertEqualsForField(root, columnSet, "long", BigIntVector.class);
    assertEqualsForField(root, columnSet, "long_nullable", BigIntVector.class);
    assertEqualsForField(root, columnSet, "float", Float4Vector.class);
    assertEqualsForField(root, columnSet, "float_nullable", Float4Vector.class);
    assertEqualsForField(root, columnSet, "double", Float8Vector.class);
    assertEqualsForField(root, columnSet, "double_nullable", Float8Vector.class);
    assertEqualsForField(root, columnSet, "timestamp_tz", TimeStampMicroTZVector.class);
    assertEqualsForField(root, columnSet, "timestamp_tz_nullable", TimeStampMicroTZVector.class);
    assertEqualsForField(root, columnSet, "string", VarCharVector.class);
    assertEqualsForField(root, columnSet, "string_nullable", VarCharVector.class);
    assertEqualsForField(root, columnSet, "bytes", VarBinaryVector.class);
    assertEqualsForField(root, columnSet, "bytes_nullable", VarBinaryVector.class);
    assertEqualsForField(root, columnSet, "date", DateDayVector.class);
    assertEqualsForField(root, columnSet, "date_nullable", DateDayVector.class);
    assertEqualsForField(root, columnSet, "time", TimeMicroVector.class);
    assertEqualsForField(root, columnSet, "time_nullable", TimeMicroVector.class);
    assertEqualsForField(root, columnSet, "uuid", FixedSizeBinaryVector.class);
    assertEqualsForField(root, columnSet, "uuid_nullable", FixedSizeBinaryVector.class);
    assertEqualsForField(root, columnSet, "int_promotion", IntVector.class);
  }

  private void assertEqualsForField(
      VectorSchemaRoot root, Set<String> columnSet, String columnName, Class<?> expected) {
    if (columnSet.contains(columnName)) {
      assertEquals(expected, root.getVector(columnName).getClass());
    }
  }

  private void checkAllVectorValues(
      int expectedNumRows,
      List<GenericRecord> expectedRows,
      VectorSchemaRoot root,
      Set<String> columnSet) {
    assertEquals(expectedNumRows, root.getRowCount());

    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "timestamp",
        (records, i) -> records.get(i).getField("timestamp"),
        (vector, i) -> timestampFromMicros(((TimeStampMicroVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "timestamp_nullable",
        (records, i) -> records.get(i).getField("timestamp_nullable"),
        (vector, i) -> timestampFromMicros(((TimeStampMicroVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "boolean",
        (records, i) -> records.get(i).getField("boolean"),
        (vector, i) -> ((BitVector) vector).get(i) == 1
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "boolean_nullable",
        (records, i) -> records.get(i).getField("boolean_nullable"),
        (vector, i) -> ((BitVector) vector).get(i) == 1
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "int",
        (records, i) -> records.get(i).getField("int"),
        (vector, i) -> ((IntVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "int_nullable",
        (records, i) -> records.get(i).getField("int_nullable"),
        (vector, i) -> ((IntVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "long",
        (records, i) -> records.get(i).getField("long"),
        (vector, i) -> ((BigIntVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "long_nullable",
        (records, i) -> records.get(i).getField("long_nullable"),
        (vector, i) -> ((BigIntVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "float",
        (records, i) -> Float.floatToIntBits((float) records.get(i).getField("float")),
        (vector, i) -> Float.floatToIntBits(((Float4Vector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "float_nullable",
        (records, i) -> Float.floatToIntBits((float) records.get(i).getField("float_nullable")),
        (vector, i) -> Float.floatToIntBits(((Float4Vector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "double",
        (records, i) -> Double.doubleToLongBits((double) records.get(i).getField("double")),
        (vector, i) -> Double.doubleToLongBits(((Float8Vector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "double_nullable",
        (records, i) -> Double.doubleToLongBits((double) records.get(i).getField("double_nullable")),
        (vector, i) -> Double.doubleToLongBits(((Float8Vector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "timestamp_tz",
        (records, i) -> timestampToMicros((OffsetDateTime) records.get(i).getField("timestamp_tz")),
        (vector, i) -> ((TimeStampMicroTZVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "timestamp_tz_nullable",
        (records, i) -> timestampToMicros((OffsetDateTime) records.get(i).getField("timestamp_tz_nullable")),
        (vector, i) -> ((TimeStampMicroTZVector) vector).get(i)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "string",
        (records, i) -> records.get(i).getField("string"),
        (vector, i) -> new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "string_nullable",
        (records, i) -> records.get(i).getField("string_nullable"),
        (vector, i) -> new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "bytes",
        (records, i) -> records.get(i).getField("bytes"),
        (vector, i) -> ByteBuffer.wrap(((VarBinaryVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "bytes_nullable",
        (records, i) -> records.get(i).getField("bytes_nullable"),
        (vector, i) -> ByteBuffer.wrap(((VarBinaryVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "date",
        (records, i) -> records.get(i).getField("date"),
        (vector, i) -> dateFromDay(((DateDayVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "date_nullable",
        (records, i) -> records.get(i).getField("date_nullable"),
        (vector, i) -> dateFromDay(((DateDayVector) vector).get(i))
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "int_promotion",
        (records, i) -> records.get(i).getField("int_promotion"),
        (vector, i) -> ((IntVector) vector).get(i)
    );

    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "uuid",
        (records, i) -> records.get(i).getField("uuid"),
        (vector, i) -> ((FixedSizeBinaryVector) vector).get(i)
    );

    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "uuid_nullable",
        (records, i) -> records.get(i).getField("uuid_nullable"),
        (vector, i) -> ((FixedSizeBinaryVector) vector).get(i)
    );

    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "time",
        (records, i) -> records.get(i).getField("time"),
        (vector, i) -> LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(i) * 1000)
    );
    checkVectorValues(
        expectedNumRows, expectedRows, root, columnSet, "time_nullable",
        (records, i) -> records.get(i).getField("time_nullable"),
        (vector, i) -> LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(i) * 1000)
    );
  }

  private static void checkVectorValues(
      int expectedNumRows,
      List<GenericRecord> expectedRows,
      VectorSchemaRoot root,
      Set<String> columnSet,
      String columnName,
      BiFunction<List<GenericRecord>, Integer, Object> expectedValueExtractor,
      BiFunction<FieldVector, Integer, Object> vectorValueExtractor) {
    if (columnSet.contains(columnName)) {
      FieldVector vector = root.getVector(columnName);
      assertEquals(expectedNumRows, vector.getValueCount());
      for (int i = 0; i < expectedNumRows; i++) {
        Object expectedValue = expectedValueExtractor.apply(expectedRows, i);
        Object actualValue = vectorValueExtractor.apply(vector, i);
        // we need to use assertThat() here because it does a java.util.Objects.deepEquals() and that
        // is relevant for byte[]
        Assertions.assertThat(actualValue).as("Row#" + i + " mismatches").isEqualTo(expectedValue);
      }
    }
  }

  private static final class LocalDateTimeToLongMicros implements StructLike {

    private final Record row;

    LocalDateTimeToLongMicros(Record row) {
      this.row = row;
    }

    @Override
    public int size() {
      return row.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value = row.get(pos);
      if (value instanceof LocalDateTime) {
        @SuppressWarnings("unchecked")
        T result = (T) (Long) timestampToMicros((LocalDateTime) value);
        return result;
      } else if (value instanceof OffsetDateTime) {
        @SuppressWarnings("unchecked")
        T result = (T) (Long) timestampToMicros(((OffsetDateTime) value).toLocalDateTime());
        return result;
      } else if (value != null) {
        throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
      } else {
        throw new IllegalArgumentException("Don't know how to handle null value");
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      row.set(pos, value);
    }
  }
}
