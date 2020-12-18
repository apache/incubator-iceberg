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


import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class RelativePathHadoopFunctionalityTest {

  private static final Logger log = LoggerFactory.getLogger(RelativePathHadoopFunctionalityTest.class);

  private Table table;
  private File tableLocation;
  private SparkSession spark = null;

  @Test
  public void testAbsoluteAndRelativePathSupport() throws Exception {

    spark = SparkSession.builder().master("local[2]").getOrCreate();

    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get())
    );

    tableLocation = new File("/tmp/hive", "iceberg_test_table");

    if (tableLocation.exists()) {
      FileUtils.deleteDirectory(tableLocation);
    }

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = new HashMap<>();
    properties.put("write.metadata.use.relative-path", "true");

    table = tables.create(schema, spec, properties, tableLocation.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    for (int i = 0; i < 2; i++) {
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableLocation.toString());
    }
    table.refresh();

    long oldId = table.history().get(0).snapshotId();

    table.rollback().toSnapshotId(oldId).commit();
    table.refresh();

    table.refresh();

    int oldCount = spark.read()
        .format("iceberg")
        .load(tableLocation.toString()).collectAsList().size();

    DataFile dataFile = table.newScan().planFiles().iterator().next().file();
    table.newDelete().deleteFile(dataFile).commit();
    table.refresh();

    Dataset<Row> results = spark.read()
        .format("iceberg")
        .load(tableLocation.toString());

    results.createOrReplaceTempView("table");
    Assert.assertEquals("Should have one less row after successful delete",
        (long) oldCount - 1, scalarSql("select count(*) from table"));
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
}
