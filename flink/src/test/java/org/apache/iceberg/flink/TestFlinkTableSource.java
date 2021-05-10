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
import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFlinkTableSource extends FlinkTestBase {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private static String warehouse;

  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;

  public TestFlinkTableSource() {
    // register a scan event listener to validate pushdown
    Listeners.register(event -> {
      scanEventCount += 1;
      lastScanEvent = event;
    }, ScanEvent.class);
  }

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", CATALOG_NAME,
        warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql("CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('write.format.default'='%s')", TABLE_NAME,
        format.name());
    sql("INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)", TABLE_NAME);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
  }

  @Test
  public void testLimitPushDown() {
    String querySql = String.format("SELECT * FROM %s LIMIT 1", TABLE_NAME);
    String explain = getTableEnv().explainSql(querySql);
    String expectedExplain = "limit=[1]";
    Assert.assertTrue("Explain should contain LimitPushDown", explain.contains(expectedExplain));
    List<Row> result = sql(querySql);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertEquals("Should produce the expected records", Row.of(1, "iceberg", 10.0), result.get(0));

    AssertHelpers.assertThrows("Invalid limit number: -1 ", SqlParserException.class,
        () -> sql("SELECT * FROM %s LIMIT -1", TABLE_NAME));

    Assert.assertEquals("Should have 0 record", 0, sql("SELECT * FROM %s LIMIT 0", TABLE_NAME).size());

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 4", TABLE_NAME);
    List<Row> resultExceed = sql(sqlLimitExceed);
    Assert.assertEquals("Should have 3 records", 3, resultExceed.size());
    List<Row> expectedList = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertEquals("Should produce the expected records", expectedList, resultExceed);

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Row> mixedResult = sql(sqlMixed);
    Assert.assertEquals("Should have 1 record", 1, mixedResult.size());
    Assert.assertEquals("Should produce the expected records", Row.of(1, "iceberg", 10.0), mixedResult.get(0));
  }

  @Test
  public void testNoFilterPushDown() {
    String sql = String.format("SELECT * FROM %s ", TABLE_NAME);
    List<Row> result = sql(sql);
    List<Row> expectedRecords = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertArrayEquals("Should produce the expected record", expectedRecords.toArray(), result.toArray());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownEqual() {
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") == 1";

    List<Row> result = sql(sqlLiteralRight);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), result.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownEqualNull() {
    String sqlEqualNull = String.format("SELECT * FROM %s WHERE data = NULL ", TABLE_NAME);

    List<Row> result = sql(sqlEqualNull);
    Assert.assertEquals("Should have 0 record", 0, result.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownEqualLiteralOnLeft() {
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") == 1";

    List<Row> resultLeft = sql(sqlLiteralLeft);
    Assert.assertEquals("Should have 1 record", 1, resultLeft.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultLeft.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNoEqual() {
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") != 1";

    List<Row> resultNE = sql(sqlNE);
    Assert.assertEquals("Should have 2 records", 2, resultNE.size());

    List<Row> expectedNE = Lists.newArrayList(
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedNE, resultNE);
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNoEqualNull() {
    String sqlNotEqualNull = String.format("SELECT * FROM %s WHERE data <> NULL ", TABLE_NAME);

    List<Row> resultNE = sql(sqlNotEqualNull);
    Assert.assertEquals("Should have 0 records", 0, resultNE.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownAnd() {
    String sqlAnd = String.format("SELECT * FROM %s WHERE id = 1 AND data = 'iceberg' ", TABLE_NAME);

    List<Row> resultAnd = sql(sqlAnd);
    Assert.assertEquals("Should have 1 record", 1, resultAnd.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultAnd.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    String expected = "(ref(name=\"id\") == 1 and ref(name=\"data\") == \"iceberg\")";
    Assert.assertEquals("Should contain the push down filter", expected, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownOr() {
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"data\") == \"b\")";

    List<Row> resultOr = sql(sqlOr);
    Assert.assertEquals("Should have 2 record", 2, resultOr.size());

    List<Row> expectedOR = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedOR, resultOr);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThan() {
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") > 1";

    List<Row> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 2 record", 2, resultGT.size());

    List<Row> expectedGT = Lists.newArrayList(
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedGT, resultGT);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanNull() {
    String sqlGT = String.format("SELECT * FROM %s WHERE data > null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownGreaterThanLiteralOnLeft() {
    String sqlGT = String.format("SELECT * FROM %s WHERE 3 > id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") < 3";

    List<Row> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 2 records", 2, resultGT.size());

    List<Row> expectedGT = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedGT, resultGT);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanEqual() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") >= 2";

    List<Row> resultGTE = sql(sqlGTE);
    Assert.assertEquals("Should have 2 records", 2, resultGTE.size());

    List<Row> expectedGTE = Lists.newArrayList(
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedGTE, resultGTE);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanEqualNull() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE data >= null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlGTE);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownGreaterThanEqualLiteralOnLeft() {
    String sqlGTE = String.format("SELECT * FROM %s WHERE 2 >= id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") <= 2";

    List<Row> resultGTE = sql(sqlGTE);
    Assert.assertEquals("Should have 2 records", 2, resultGTE.size());

    List<Row> expectedGTE = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedGTE, resultGTE);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThan() {
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") < 2";

    List<Row> resultLT = sql(sqlLT);
    Assert.assertEquals("Should have 1 record", 1, resultLT.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultLT.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanNull() {
    String sqlLT = String.format("SELECT * FROM %s WHERE data < null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlLT);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownLessThanLiteralOnLeft() {
    String sqlLT = String.format("SELECT * FROM %s WHERE 2 < id ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") > 2";

    List<Row> resultLT = sql(sqlLT);
    Assert.assertEquals("Should have 1 record", 1, resultLT.size());
    Assert.assertEquals("Should produce the expected record", Row.of(3, null, 30.0), resultLT.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanEqual() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") <= 1";

    List<Row> resultLTE = sql(sqlLTE);
    Assert.assertEquals("Should have 1 record", 1, resultLTE.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultLTE.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanEqualNull() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE data <= null ", TABLE_NAME);

    List<Row> resultGT = sql(sqlLTE);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownLessThanEqualLiteralOnLeft() {
    String sqlLTE = String.format("SELECT * FROM %s WHERE 3 <= id  ", TABLE_NAME);
    String expectedFilter = "ref(name=\"id\") >= 3";

    List<Row> resultLTE = sql(sqlLTE);
    Assert.assertEquals("Should have 1 record", 1, resultLTE.size());
    Assert.assertEquals("Should produce the expected record", Row.of(3, null, 30.0), resultLTE.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownIn() {
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)";
    List<Row> resultIN = sql(sqlIN);
    Assert.assertEquals("Should have 2 records", 2, resultIN.size());

    List<Row> expectedIN = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedIN, resultIN);
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownInNull() {
    String sqlInNull = String.format("SELECT * FROM %s WHERE data IN ('iceberg',NULL) ", TABLE_NAME);

    List<Row> result = sql(sqlInNull);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), result.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownNotIn() {
    String sqlNotIn = String.format("SELECT * FROM %s WHERE id NOT IN (3,2) ", TABLE_NAME);

    List<Row> resultNotIn = sql(sqlNotIn);
    Assert.assertEquals("Should have 1 record", 1, resultNotIn.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultNotIn.get(0));
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    String expectedScan = "(ref(name=\"id\") != 2 and ref(name=\"id\") != 3)";
    Assert.assertEquals("Should contain the push down filter", expectedScan, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNotInNull() {
    String sqlNotInNull = String.format("SELECT * FROM %s WHERE id NOT IN (1,2,NULL) ", TABLE_NAME);
    List<Row> resultGT = sql(sqlNotInNull);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownIsNotNull() {
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String expectedFilter = "not_null(ref(name=\"data\"))";

    List<Row> resultNotNull = sql(sqlNotNull);
    Assert.assertEquals("Should have 2 record", 2, resultNotNull.size());

    List<Row> expected = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expected, resultNotNull);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownIsNull() {
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String expectedFilter = "is_null(ref(name=\"data\"))";

    List<Row> resultNull = sql(sqlNull);
    Assert.assertEquals("Should have 1 record", 1, resultNull.size());
    Assert.assertEquals("Should produce the expected record", Row.of(3, null, 30.0), resultNull.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNot() {
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT (id = 1 OR id = 2 ) ", TABLE_NAME);

    List<Row> resultNot = sql(sqlNot);
    Assert.assertEquals("Should have 1 record", 1, resultNot.size());
    Assert.assertEquals("Should produce the expected record", Row.of(3, null, 30.0), resultNot.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    String expectedFilter = "(ref(name=\"id\") != 1 and ref(name=\"id\") != 2)";
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownBetween() {
    String sqlBetween = String.format("SELECT * FROM %s WHERE id BETWEEN 1 AND 2 ", TABLE_NAME);

    List<Row> resultBetween = sql(sqlBetween);
    Assert.assertEquals("Should have 2 record", 2, resultBetween.size());

    List<Row> expectedBetween = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedBetween, resultBetween);

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    String expected = "(ref(name=\"id\") >= 1 and ref(name=\"id\") <= 2)";
    Assert.assertEquals("Should contain the push down filter", expected, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNotBetween() {
    String sqlNotBetween = String.format("SELECT * FROM %s WHERE id  NOT BETWEEN 2 AND 3 ", TABLE_NAME);
    String expectedFilter = "(ref(name=\"id\") < 2 or ref(name=\"id\") > 3)";

    List<Row> resultNotBetween = sql(sqlNotBetween);
    Assert.assertEquals("Should have 1 record", 1, resultNotBetween.size());
    Assert.assertEquals("Should produce the expected record", Row.of(1, "iceberg", 10.0), resultNotBetween.get(0));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLike() {
    String expectedFilter = "ref(name=\"data\") startsWith \"\"ice\"\"";

    String sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'ice%%' ";
    List<Row> resultLike = sql(sqlLike);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertEquals("The like result should produce the expected record",
        Row.of(1, "iceberg", 10.0), resultLike.get(0));
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterNotPushDownLike() {
    Row expectRecord = Row.of(1, "iceberg", 10.0);
    String sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i' ";
    List<Row> resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 0, resultLike.size());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i%%' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%ice%%g' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 3 records", 3, resultLike.size());
    List<Row> expectedRecords = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertEquals("Should produce the expected record", expectedRecords, resultLike);
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'iceber_' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'i%%g' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDown2Literal() {
    String sql2Literal = String.format("SELECT * FROM %s WHERE 1 > 0 ", TABLE_NAME);
    List<Row> result = sql(sql2Literal);
    List<Row> expectedRecords = Lists.newArrayList(
        Row.of(1, "iceberg", 10.0),
        Row.of(2, "b", 20.0),
        Row.of(3, null, 30.0)
    );
    Assert.assertArrayEquals("Should produce the expected record", expectedRecords.toArray(), result.toArray());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  /**
   * NaN is not supported by flink now, so we add the test case to assert the parse error, when we upgrade the flink
   * that supports NaN, we will delele the method, and add some test case to test NaN.
   */
  @Test
  public void testSqlParseError() {
    String sqlParseErrorEqual = String.format("SELECT * FROM %s WHERE d = CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorEqual));

    String sqlParseErrorNotEqual = String.format("SELECT * FROM %s WHERE d <> CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorNotEqual));

    String sqlParseErrorGT = String.format("SELECT * FROM %s WHERE d > CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorGT));

    String sqlParseErrorLT = String.format("SELECT * FROM %s WHERE d < CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorLT));

    String sqlParseErrorGTE = String.format("SELECT * FROM %s WHERE d >= CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorGTE));

    String sqlParseErrorLTE = String.format("SELECT * FROM %s WHERE d <= CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorLTE));
  }

  @Test
  public void testComputedColumnsWithConstant() {
    sql("CREATE TABLE test_computed_columns ( " +
        "     a INT," +
        "     b AS 'iceberg'" +
        " ) ");
    sql("INSERT INTO test_computed_columns SELECT 1");
    List<Object[]> result = sql("SELECT * FROM test_computed_columns ");
    Object[] expectRecord = new Object[] {1, "iceberg"};
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, result.get(0));
    sql("DROP TABLE test_computed_columns");
  }

  @Test
  public void testComputedColumnsWithFunction() {
    sql("CREATE TABLE test_computed_columns ( \n" +
        "     a INT," +
        "     c AS TO_TIMESTAMP('2021-01-01 12:13:14')" +
        " ) ");
    sql("INSERT INTO test_computed_columns SELECT 1");
    List<Object[]> result = sql("SELECT * FROM test_computed_columns ");
    Object[] expectRecord = new Object[] {1, LocalDateTime.parse("2021-01-01T12:13:14")};
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, result.get(0));
    sql("DROP TABLE test_computed_columns");
  }

  @Test
  public void testComputedColumnsWithExistColumn() {
    sql("CREATE TABLE test_computed_columns ( \n" +
        "     a INT," +
        "     d AS a + 10" +
        " ) ");
    sql("INSERT INTO test_computed_columns SELECT 1");
    List<Object[]> result = sql("SELECT * FROM test_computed_columns ");
    Object[] expectRecord = new Object[] {1, 11};
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, result.get(0));
    sql("DROP TABLE test_computed_columns");
  }
}
