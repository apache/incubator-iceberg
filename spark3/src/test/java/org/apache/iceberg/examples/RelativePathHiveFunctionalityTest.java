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

package org.apache.iceberg.examples;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class RelativePathHiveFunctionalityTest extends HiveMetastoreTest {
  private static final Logger log = LoggerFactory.getLogger(RelativePathHiveFunctionalityTest.class);

  private Table table;
  private SparkSession spark = null;

  @Test
  public void testAbsoluteAndRelativePathSupport() throws Exception {
    String tableName = "default.iceberg_hive_test_table";
    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get())
    );

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = new HashMap<>();
    properties.put("write.metadata.use.relative-path", "true");

    TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
    table = catalog.createTable(tableIdentifier, schema, spec, properties);

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Iterator<Map.Entry<String, String>> ite = HiveMetastoreTest.hiveConf.iterator();
    SparkConf conf = new SparkConf();
    while (ite.hasNext()) {
      Map.Entry<String, String> entry = ite.next();
      conf.set(entry.getKey(), entry.getValue());
    }
    spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate();

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    for (int i = 0; i < 2; i++) {
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableName);
    }
    table.refresh();
//
    long oldId = table.history().get(0).snapshotId();

    table.rollback().toSnapshotId(oldId).commit();
    table.refresh();

    Dataset<Row> results = spark.read()
        .format("iceberg")
        .load(tableName);

    results.createOrReplaceTempView("table");
    spark.sql("select * from table").show();
  }
}
