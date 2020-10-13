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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class FlinkCatalogTestBase extends FlinkTestBase {

  protected static final String DATABASE = "db";
  private static File warehouse = null;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    FlinkCatalogTestBase.warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
  }

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @Parameterized.Parameters(name = "catalogName = {0} baseNamespace = {1}")
  // baseNamespace comes out as a String[] memory reference due to lack
  // of a meaningful toString method. We should convert baseNamespace to
  // use Namespace instead: https://github.com/apache/iceberg/issues/1541
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {"testhive", new String[0]},
        new Object[] {"testhadoop", new String[0]},
        new Object[] {"testhadoop_basenamespace", new String[] {"l0", "l1"}}
    );
  }

  private volatile TableEnvironment tEnv = null;

  protected final String catalogName;
  protected final String[] baseNamespace;
  protected final Catalog validationCatalog;
  protected final SupportsNamespaces validationNamespaceCatalog;

  protected final String flinkDatabase;
  protected final Namespace icebergNamespace;
  protected final boolean isHadoopCatalog;

  public FlinkCatalogTestBase(String catalogName, String[] baseNamespace) {
    this.catalogName = catalogName;
    this.baseNamespace = baseNamespace;
    this.isHadoopCatalog = catalogName.startsWith("testhadoop");
    this.validationCatalog = isHadoopCatalog ?
        new HadoopCatalog(hiveConf, "file:" + warehouse) :
        catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    Map<String, String> config = Maps.newHashMap();
    config.put("type", "iceberg");
    config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, isHadoopCatalog ? "hadoop" : "hive");
    config.put(FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION, "file:" + warehouse);
    if (baseNamespace.length > 0) {
      config.put(FlinkCatalogFactory.BASE_NAMESPACE, Joiner.on(".").join(baseNamespace));
    }

    FlinkCatalogFactory factory = new FlinkCatalogFactory() {
      @Override
      protected org.apache.flink.table.catalog.Catalog createCatalog(
          String name, Map<String, String> properties, Configuration hadoopConf) {
        return super.createCatalog(name, properties, hiveConf);
      }
    };
    getTableEnv().registerCatalog(
        catalogName,
        flinkCatalogs.computeIfAbsent(catalogName, k -> factory.createCatalog(k, config)));

    this.flinkDatabase = catalogName + "." + DATABASE;
    this.icebergNamespace = Namespace.of(ArrayUtils.concat(baseNamespace, new String[] { DATABASE }));
  }

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv = TableEnvironment.create(EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  public List<Object[]> sql(String query, Object... args) {
    TableResult tableResult = getTableEnv().executeSql(String.format(query, args));
    tableResult.getJobClient().ifPresent(c -> {
      try {
        c.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    CloseableIterator<Row> iter = tableResult.collect();
    List<Object[]> results = Lists.newArrayList();
    while (iter.hasNext()) {
      Row row = iter.next();
      results.add(IntStream.range(0, row.getArity()).mapToObj(row::getField).toArray(Object[]::new));
    }
    return results;
  }
}
