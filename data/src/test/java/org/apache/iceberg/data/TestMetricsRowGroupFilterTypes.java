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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestMetricsRowGroupFilterTypes {
  private static final Schema SCHEMA = new Schema(
      optional(1, "boolean", BooleanType.get()),
      optional(2, "int", IntegerType.get()),
      optional(3, "long", LongType.get()),
      optional(4, "float", FloatType.get()),
      optional(5, "double", DoubleType.get()),
      optional(6, "date", DateType.get()),
      optional(7, "time", TimeType.get()),
      optional(8, "timestamp", TimestampType.withoutZone()),
      optional(9, "timestamptz", TimestampType.withZone()),
      optional(10, "string", StringType.get()),
      optional(11, "uuid", UUIDType.get()),
      optional(12, "fixed", FixedType.ofLength(4)),
      optional(13, "binary", BinaryType.get()),
      optional(14, "int_decimal", Types.DecimalType.of(8, 2)),
      optional(15, "long_decimal", Types.DecimalType.of(14, 2)),
      optional(16, "fixed_decimal", Types.DecimalType.of(31, 2))
  );

  private static final Schema FILE_SCHEMA = new Schema(
      optional(1, "_boolean", BooleanType.get()),
      optional(2, "_int", IntegerType.get()),
      optional(3, "_long", LongType.get()),
      optional(4, "_float", FloatType.get()),
      optional(5, "_double", DoubleType.get()),
      optional(6, "_date", DateType.get()),
      optional(7, "_time", TimeType.get()),
      optional(8, "_timestamp", TimestampType.withoutZone()),
      optional(9, "_timestamptz", TimestampType.withZone()),
      optional(10, "_string", StringType.get()),
      optional(11, "_uuid", UUIDType.get()),
      optional(12, "_fixed", FixedType.ofLength(4)),
      optional(13, "_binary", BinaryType.get()),
      optional(14, "_int_decimal", Types.DecimalType.of(8, 2)),
      optional(15, "_long_decimal", Types.DecimalType.of(14, 2)),
      optional(16, "_fixed_decimal", Types.DecimalType.of(31, 2))
  );

  private static final File ORC_FILE = new File("/tmp/stats-row-group-filter-types-test.orc");

  private static final File PARQUET_FILE = new File("/tmp/stats-row-group-filter-types-test.parquet");
  private static MessageType parquetSchema = null;
  private static BlockMetaData rowGroupMetadata = null;

  private static final UUID uuid = UUID.randomUUID();
  private static final LocalDate date =  LocalDate.parse("2018-06-29", DateTimeFormatter.ISO_LOCAL_DATE);
  private static final LocalTime time = LocalTime.parse("10:02:34.000000", DateTimeFormatter.ISO_LOCAL_TIME);
  private static final OffsetDateTime timestamptz = OffsetDateTime.parse("2018-06-29T10:02:34.000000+00:00",
      DateTimeFormatter.ISO_DATE_TIME);
  private static final LocalDateTime timestamp = LocalDateTime.parse("2018-06-29T10:02:34.000000",
      DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  private static final byte[] fixed = "abcd".getBytes(StandardCharsets.UTF_8);

  @Before
  public void createInputFile() throws IOException {
    List<Record> records = new ArrayList<>();
    // create 50 records
    for (int i = 0; i < 50; i += 1) {
      Record record = GenericRecord.create(FILE_SCHEMA);
      record.setField("_boolean", false);
      record.setField("_int", i);
      record.setField("_long", 5_000_000_000L + i);
      record.setField("_float", ((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
      record.setField("_double", ((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
      record.setField("_date", date);
      record.setField("_time", time);
      record.setField("_timestamp", timestamp);
      record.setField("_timestamptz", timestamptz);
      record.setField("_string", "tapir");
      // record.setField("_uuid", uuid); // Disable writing UUID value as GenericParquetWriter does not handle UUID type
                                         // correctly; Also UUID tests are disabled for both ORC and Parquet anyway
      record.setField("_fixed", fixed);
      record.setField("_binary", ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8)));
      record.setField("_int_decimal", new BigDecimal("77.77"));
      record.setField("_long_decimal", new BigDecimal("88.88"));
      record.setField("_fixed_decimal", new BigDecimal("99.99"));
      records.add(record);
    }
    switch (format) {
      case ORC:
        createOrcInputFile(records);
        break;
      case PARQUET:
        createParquetInputFile(records);
        break;
      default:
        throw new UnsupportedOperationException("Row group filter types tests not supported for " + format);
    }
  }

  public void createOrcInputFile(List<Record> records) throws IOException {
    if (ORC_FILE.exists()) {
      Assert.assertTrue(ORC_FILE.delete());
    }

    OutputFile outFile = Files.localOutput(ORC_FILE);
    try (FileAppender<Record> appender = ORC.write(outFile)
        .schema(FILE_SCHEMA)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      appender.addAll(records);
    }

    InputFile inFile = Files.localInput(ORC_FILE);
    try (Reader reader = OrcFile.createReader(new Path(inFile.location()),
        OrcFile.readerOptions(new Configuration()))) {
      Assert.assertEquals("Should create only one stripe", 1, reader.getStripes().size());
    }

    ORC_FILE.deleteOnExit();
  }

  public void createParquetInputFile(List<Record> records) throws IOException {
    if (PARQUET_FILE.exists()) {
      Assert.assertTrue(PARQUET_FILE.delete());
    }

    OutputFile outFile = Files.localOutput(PARQUET_FILE);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      appender.addAll(records);
    }

    InputFile inFile = Files.localInput(PARQUET_FILE);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    PARQUET_FILE.deleteOnExit();
  }

  private final FileFormat format;
  private final String column;
  private final Object readValue;
  private final Object skipValue;

  @Parameterized.Parameters(name = "format = {0} column = {1} readValue = {2} skipValue = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "parquet", "boolean", false, true },
        { "parquet", "int", 5, 55 },
        { "parquet", "long", 5_000_000_049L, 5_000L },
        { "parquet", "float", 1.97f, 2.11f },
        { "parquet", "double", 2.11d, 1.97d },
        { "parquet", "date", "2018-06-29", "2018-05-03" },
        { "parquet", "time", "10:02:34.000000", "10:02:34.000001" },
        { "parquet", "timestamp", "2018-06-29T10:02:34.000000", "2018-06-29T15:02:34.000000" },
        { "parquet", "timestamptz", "2018-06-29T10:02:34.000000+00:00", "2018-06-29T10:02:34.000000-07:00" },
        { "parquet", "string", "tapir", "monthly" },
        // { "parquet", "uuid", uuid, UUID.randomUUID() }, // not supported yet
        { "parquet", "fixed", "abcd".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3 } },
        { "parquet", "binary", "xyz".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3, 4, 5 } },
        { "parquet", "int_decimal", "77.77", "12.34" },
        { "parquet", "long_decimal", "88.88", "12.34" },
        { "parquet", "fixed_decimal", "99.99", "12.34" },

        { "orc", "boolean", false, true },
        { "orc", "int", 5, 55 },
        { "orc", "long", 5_000_000_049L, 5_000L },
        { "orc", "float", 1.97f, 2.11f },
        { "orc", "double", 2.11d, 1.97d },
        { "orc", "date", "2018-06-29", "2018-05-03" },
        { "orc", "time", "10:02:34.000000", "10:02:34.000001" },
        // Temporarily disable filters on Timestamp columns due to ORC-611.
        // ORC-611 is closed with fix versions of ORC 1.6.4 and 1.7.0, but
        // testing on 1.6.4 still lead to failures and 1.7.0 is not published.
        //
        // See also HIVE-23036, which is still open.
        // { "orc", "timestamp", "2018-06-29T10:02:34.000000", "2018-06-29T15:02:34.000000" },
        // { "orc", "timestamptz", "2018-06-29T10:02:34.000000+00:00", "2018-06-29T10:02:34.000000-07:00" },
        { "orc", "string", "tapir", "monthly" },
        // uuid, fixed and binary types not supported yet
        // { "orc", "uuid", uuid, UUID.randomUUID() },
        // { "orc", "fixed", "abcd".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3 } },
        // { "orc", "binary", "xyz".getBytes(StandardCharsets.UTF_8), new byte[] { 0, 1, 2, 3, 4, 5 } },
        { "orc", "int_decimal", "77.77", "12.34" },
        { "orc", "long_decimal", "88.88", "12.34" },
        { "orc", "fixed_decimal", "99.99", "12.34" },
    };
  }

  public TestMetricsRowGroupFilterTypes(String format, String column, Object readValue, Object skipValue) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.column = column;
    this.readValue = readValue;
    this.skipValue = skipValue;
  }

  @Test
  public void testEq() {
    boolean shouldRead = shouldRead(readValue);
    Assert.assertTrue("Should read: value is in the row group: " + readValue, shouldRead);

    shouldRead = shouldRead(skipValue);
    Assert.assertFalse("Should skip: value is not in the row group: " + skipValue, shouldRead);
  }

  private boolean shouldRead(Object value) {
    switch (format) {
      case ORC:
        return shouldReadOrc(value);
      case PARQUET:
        return shouldReadParquet(value);
      default:
        throw new UnsupportedOperationException("Row group filter types tests not supported for " + format);
    }
  }

  private boolean shouldReadOrc(Object value) {
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(ORC_FILE))
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
        .filter(Expressions.equal(column, value))
        .build()) {
      return Lists.newArrayList(reader).size() > 0;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean shouldReadParquet(Object value) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, equal(column, value))
        .shouldRead(parquetSchema, rowGroupMetadata);
  }

  private org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inFile.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream stream = inFile.newStream();
        return new DelegatingSeekableInputStream(stream) {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }
        };
      }
    };
  }
}
