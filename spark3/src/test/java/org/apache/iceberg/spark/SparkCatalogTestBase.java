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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class SparkCatalogTestBase extends SparkTestBase {
  private static File warehouse = null;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    SparkCatalogTestBase.warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
  }

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "testhive", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"
         ) },
        { "testhadoop", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hadoop"
        ) },
        { "spark_catalog", SparkSessionCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
        ) },
        { "spark_catalog_with_unique_location", SparkCatalog.class.getName(),
           ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            HiveCatalog.APPEND_UUID_SUFFIX_TO_TABLE_LOCATION, "true"
        ) }
    };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected final String catalogName;
  protected final Catalog validationCatalog;
  protected final SupportsNamespaces validationNamespaceCatalog;
  protected final TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected final String tableName;

  public SparkCatalogTestBase(String catalogName, String implementation, Map<String, String> config) {
    this.catalogName = catalogName;
    this.validationCatalog = catalogName.equals("testhadoop") ?
        new HadoopCatalog(spark.sessionState().newHadoopConf(), "file:" + warehouse) :
        catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    config.forEach((key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if (config.get("type").equalsIgnoreCase("hadoop")) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".warehouse", "file:" + warehouse);
    }

    this.tableName = (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default.table";

    sql("CREATE NAMESPACE IF NOT EXISTS default");
  }

  protected String tableName(String name) {
    return (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default." + name;
  }
}
