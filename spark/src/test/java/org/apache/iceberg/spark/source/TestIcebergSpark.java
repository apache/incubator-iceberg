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

package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestIcebergSpark {

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestIcebergSpark.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestIcebergSpark.spark;
    TestIcebergSpark.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testRegisterBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_int_16", DataTypes.IntegerType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_int_16(1)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals((int) Transforms.bucket(Types.IntegerType.get(), 16).apply(1),
        results.get(0).getInt(0));

    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_long_16", DataTypes.LongType, 16);
    List<Row> results2 = spark.sql("SELECT iceberg_bucket_long_16(1L)").collectAsList();
    Assert.assertEquals(1, results2.size());
    Assert.assertEquals((int) Transforms.bucket(Types.LongType.get(), 16).apply(1L),
        results2.get(0).getInt(0));

    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_string_16", DataTypes.StringType, 16);
    List<Row> results3 = spark.sql("SELECT iceberg_bucket_string_16('hello')").collectAsList();
    Assert.assertEquals(1, results3.size());
    Assert.assertEquals((int) Transforms.bucket(Types.StringType.get(), 16).apply("hello"),
        results3.get(0).getInt(0));
  }
}
