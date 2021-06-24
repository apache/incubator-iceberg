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

package org.apache.iceberg.flink.data.vectorized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.FlinkTableOptions;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestVectorizedReads extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.ORC;

  public TestVectorizedReads(String catalogName, Namespace baseNamespace) {
    super(catalogName, baseNamespace);
  }

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(FlinkTableOptions.ENABLE_VECTORIZED_READ, true);
    return super.getTableEnv();
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
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testVectorizedReadsIntType() {
    sql("CREATE TABLE %s (id int) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1),(2)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1));
    expected.add(Row.of(2));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsLongType() {
    sql("CREATE TABLE %s (id BIGINT) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1),(2)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1L));
    expected.add(Row.of(2L));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsFloatType() {
    sql("CREATE TABLE %s (id FLOAT) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1.1),(2.2)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1.1F));
    expected.add(Row.of(2.2F));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsDoubleType() {
    sql("CREATE TABLE %s (id DOUBLE) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1.1),(2.2)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1.1));
    expected.add(Row.of(2.2));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsBooleanType() {
    sql("CREATE TABLE %s (id BOOLEAN) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(true),(false)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(true));
    expected.add(Row.of(false));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsDecimalType() {
    sql("CREATE TABLE %s (id DECIMAL(10,2)) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(12.34),(34.56)", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(BigDecimal.valueOf(12.34)));
    expected.add(Row.of(BigDecimal.valueOf(34.56)));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsDateType() {
    sql("CREATE TABLE %s (data DATE) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(DATE '2021-01-01'),(DATE '2021-01-02')", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(LocalDate.of(2021, 1, 1)));
    expected.add(Row.of(LocalDate.of(2021, 1, 2)));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsTimestampType() {
    sql("CREATE TABLE %s (data TIMESTAMP) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(TO_TIMESTAMP('2021-01-01 12:13:14')),(TO_TIMESTAMP('2021-01-02 15:16:17'))", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(LocalDateTime.parse("2021-01-01T12:13:14")));
    expected.add(Row.of(LocalDateTime.parse("2021-01-02T15:16:17")));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsStringType() {
    sql("CREATE TABLE %s (id STRING) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES('a'),('b')", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of("a"));
    expected.add(Row.of("b"));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsRowType() {
    sql("CREATE TABLE %s (id INT, data ROW<`a` DOUBLE, `b` STRING>) WITH ('write.format.default'='%s')", TABLE_NAME,
        format.name());
    sql("INSERT INTO %s VALUES(1,ROW(1.1,'aaa')),(2,ROW(2.2,'bbb'))", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1, Row.of(1.1, "aaa")));
    expected.add(Row.of(2, Row.of(2.2, "bbb")));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsArrayType() {
    sql("CREATE TABLE %s (id ARRAY<INT>) WITH ('write.format.default'='%s')", TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(ARRAY[1,1]),(ARRAY[2,2])", TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    Object[] objects = new Object[1];
    objects[0] = new Integer[] {1, 1};
    Object[] objects1 = new Object[1];
    objects1[0] = new Integer[] {2, 2};

    expected.add(Row.of(objects));
    expected.add(Row.of(objects1));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsMapType() {
    sql("CREATE TABLE %s (id INT, data MAP<STRING,STRING>) WITH ('write.format.default'='%s')", TABLE_NAME,
        format.name());
    sql("INSERT INTO %s VALUES(1, STR_TO_MAP('k1=v1,k2=v2'))", TABLE_NAME);

    AssertHelpers.assertThrows("Can read map type for vectorized read.",
        RuntimeException.class, () -> sql("SELECT * FROM %s", TABLE_NAME));
  }

  @Test
  public void testVectorizedReadsSupportedNestedType() {
    sql("CREATE TABLE %s (id INT, data ROW<`a` INT, `b` ARRAY<INT>>) WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1, ROW(2,ARRAY[10,20]))", TABLE_NAME);

    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);
    List<Row> expected = Lists.newArrayList();
    Integer[] ints = new Integer[] {10, 20};
    expected.add(Row.of(1, Row.of(2, ints)));
    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testVectorizedReadsUnsupportedNestedType() {
    sql("CREATE TABLE %s (id INT, data ARRAY<ROW<`a` INT>>) WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql("INSERT INTO %s VALUES(1, ARRAY[ROW(2),ROW(3)])", TABLE_NAME);

    AssertHelpers.assertThrows("Unsupported nested type for vectorized read.",
        RuntimeException.class, () -> sql("SELECT * FROM %s", TABLE_NAME));
  }

  @Test
  public void testVectorizedReadsConstant() {
    sql("CREATE TABLE %s (id INT, spec1 VARCHAR,spec2 INT,spec3 DOUBLE,spec4 FLOAT," +
            " spec5 BIGINT,spec6 BOOLEAN,spec7 DECIMAL(10,2),spec8 DATE,spec9 TIMESTAMP,spec10 TIME,spec11 BYTES) " +
            " PARTITIONED BY (spec1,spec2,spec3,spec4,spec5,spec6,spec7,spec8,spec9,spec10,spec11)" +
            " WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql("INSERT INTO %s SELECT 1, 'hello' ,2 , 1.1 , 2.2 , 3 , true," +
            " 12.34 ,DATE '2021-01-01',TO_TIMESTAMP('2021-01-01 12:13:14'),TIME '10:11:12',ENCODE('ab', 'UTF-8') ",
        TABLE_NAME);
    List<Row> result = sql("SELECT * FROM %s", TABLE_NAME);

    List<Row> expected = Lists.newArrayList();
    expected.add(Row.of(1, "hello", 2, 1.1D, 2.2F, 3L, true, BigDecimal.valueOf(12.34), LocalDate.of(2021, 1, 1),
        LocalDateTime.parse("2021-01-01T12:13:14"), LocalTime.parse("10:11:12"), new byte[] {'a', 'b'}));
    TestHelpers.assertRows(result, expected);
  }
}
