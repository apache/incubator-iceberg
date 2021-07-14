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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.TIMESTAMP_WITH_TZ_FORMATTER;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerTimezone {
  private static final Optional<ThreadLocal<DateFormat>> DATE_FORMAT =
      Optional.ofNullable((ThreadLocal<DateFormat>) DynFields.builder()
          .hiddenImpl(TimestampWritable.class, "threadLocalDateFormat")
          .defaultAlwaysNull()
          .buildStatic()
          .get());

  private static final Optional<ThreadLocal<TimeZone>> LOCAL_TIMEZONE =
      Optional.ofNullable((ThreadLocal<TimeZone>) DynFields.builder()
          .hiddenImpl(DateWritable.class, "LOCAL_TIMEZONE")
          .defaultAlwaysNull()
          .buildStatic()
          .get());

  private static final PrimitiveTypeInfo TIMESTAMP_LOCAL_TZ_TYPE_INFO =
      (PrimitiveTypeInfo) DynFields.builder().hiddenImpl(TypeInfoFactory.class, "timestampLocalTZTypeInfo")
          .defaultAlwaysNull()
          .buildStatic().get();

  private static final Optional<DynFields.BoundField<ZoneId>> ZONE_ID =
      Optional.ofNullable(TIMESTAMP_LOCAL_TZ_TYPE_INFO == null ? null : DynFields.builder()
          .hiddenImpl("org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo", "timeZone")
          .defaultAlwaysNull()
          .build(TIMESTAMP_LOCAL_TZ_TYPE_INFO));

  @Parameters(name = "timezone={0}")
  public static Collection<Object[]> parameters() {
    return ImmutableList.of(
        new String[] {"America/New_York"},
        new String[] {"Asia/Kolkata"},
        new String[] {"UTC/Greenwich"}
    );
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public String timezoneString;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone(timezoneString));

    // Magic to clean cached date format and local timezone for Hive where the default timezone is used/stored in the
    // cached object
    DATE_FORMAT.ifPresent(ThreadLocal::remove);
    LOCAL_TIMEZONE.ifPresent(ThreadLocal::remove);
    ZONE_ID.ifPresent(field -> field.set(TimeZone.getDefault().toZoneId()));

    this.testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    // Uses spark as an engine so we can detect if we unintentionally try to use any execution engines
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "spark");
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testDateQuery() throws IOException {
    Schema dateSchema = new Schema(optional(1, "d_date", Types.DateType.get()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(dateSchema)
        .add(LocalDate.of(2020, 1, 21))
        .add(LocalDate.of(2020, 1, 24))
        .build();

    testTables.createTable(shell, "date_test", dateSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-21'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-21", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date in ('2020-01-21', '2020-01-22')");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-21", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date > '2020-01-21'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-24", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-20'");
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testTimestampQuery() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withoutZone()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
        .add(LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000))
        .add(LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000))
        .build();

    testTables.createTable(shell, "ts_test", timestampSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2019-02-22 09:44:54.2'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

    result = shell.executeStatement(
        "SELECT * FROM ts_test WHERE d_ts in ('2017-01-01 22:30:57.1', '2019-02-22 09:44:54.2')");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

    result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts < '2019-02-22 09:44:54.2'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-01-22 09:44:54.1", result.get(0)[0]);

    result = shell.executeStatement("SELECT * FROM ts_test WHERE d_ts='2017-01-01 22:30:57.3'");
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testTimestampTzQuery() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withZone()));

    if (MetastoreUtil.hive3PresentOnClasspath()) {
      LocalDateTime localDateTime = LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000);
      ZoneOffset zoneOffSet = TimeZone.getDefault().toZoneId().getRules().getOffset(localDateTime);

      OffsetDateTime offsetDateTime1 = localDateTime.atOffset(zoneOffSet);

      localDateTime = LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000);
      zoneOffSet = TimeZone.getDefault().toZoneId().getRules().getOffset(localDateTime);

      OffsetDateTime offsetDateTime2 = localDateTime.atOffset(zoneOffSet);

      List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
          .add(offsetDateTime1)
          .add(offsetDateTime2)
          .build();

      testTables.createTable(shell, "ts_test", timestampSchema, FileFormat.PARQUET, records);

      List<Object[]> result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2019-02-22 09:44:54.2'");
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(offsetDateTime2.toInstant(),
          ZonedDateTime.parse(result.get(0)[0].toString(), TIMESTAMP_WITH_TZ_FORMATTER).toInstant());

      // Skip testing `in` as I was not able come up with the constants that would work (maybe some Hive bug?)

      result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts < '2019-02-22 09:44:54.2'");
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(offsetDateTime1.toInstant(),
          ZonedDateTime.parse(result.get(0)[0].toString(), TIMESTAMP_WITH_TZ_FORMATTER).toInstant());

      result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2017-01-01 22:30:57.3'");
      Assert.assertEquals(0, result.size());
    } else {
      List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
          .add(OffsetDateTime.of(
              LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000),
              ZoneOffset.UTC))
          .add(OffsetDateTime.of(
              LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000),
              ZoneOffset.UTC))
          .build();

      testTables.createTable(shell, "ts_test", timestampSchema, FileFormat.PARQUET, records);

      List<Object[]> result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2019-02-22 09:44:54.2'");
      Assert.assertEquals(1, result.size());
      Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

      result = shell.executeStatement(
          "SELECT d_ts FROM ts_test WHERE d_ts in ('2017-01-01 22:30:57.1', '2019-02-22 09:44:54.2')");
      Assert.assertEquals(1, result.size());
      Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

      result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts < '2019-02-22 09:44:54.2'");
      Assert.assertEquals(1, result.size());
      Assert.assertEquals("2019-01-22 09:44:54.1", result.get(0)[0]);

      result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2017-01-01 22:30:57.3'");
      Assert.assertEquals(0, result.size());
    }
  }
}
