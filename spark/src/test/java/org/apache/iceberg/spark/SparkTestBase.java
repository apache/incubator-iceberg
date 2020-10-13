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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class SparkTestBase {

  protected static final Object ANY = new Object();

  private static TestHiveMetastore metastore = null;
  private static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .enableHiveSupport()
        .getOrCreate();

    SparkTestBase.catalog = new HiveCatalog(spark.sessionState().newHadoopConf());

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    if (catalog != null) {
      catalog.close();
    }
    SparkTestBase.catalog = null;
    metastore.stop();
    SparkTestBase.metastore = null;
    spark.stop();
    SparkTestBase.spark = null;
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rows.stream()
        .map(row -> IntStream.range(0, row.size())
            .mapToObj(pos -> row.isNullAt(pos) ? null : row.get(pos))
            .toArray(Object[]::new)
        ).collect(Collectors.toList());
  }

  protected Object scalarSql(String query, Object... args) {
    List<Object[]> rows = sql(query, args);
    Assert.assertEquals("Scalar SQL should return one row", 1, rows.size());
    Object[] row = Iterables.getOnlyElement(rows);
    Assert.assertEquals("Scalar SQL should return one value", 1, row.length);
    return row[0];
  }

  protected Object[] row(Object... values) {
    return values;
  }

  protected void assertEquals(String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    Assert.assertEquals(context + ": number of results should match", expectedRows.size(), actualRows.size());
    for (int row = 0; row < expectedRows.size(); row += 1) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      Assert.assertEquals("Number of columns should match", expected.length, actual.length);
      for (int col = 0; col < actualRows.get(row).length; col += 1) {
        if (expected[col] != ANY) {
          Assert.assertEquals(context + ": row " + row + " col " + col + " contents should match",
              expected[col], actual[col]);
        }
      }
    }
  }

  protected static String dbPath(String dbName) {
    return metastore.getDatabasePath(dbName);
  }
}
