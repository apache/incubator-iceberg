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

import java.io.IOException;
import java.util.Map;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class FlinkCatalogTestBase extends FlinkTestBase {

  protected static final String DATABASE = "db";
  private static TemporaryFolder hiveWarehouse = new TemporaryFolder();
  private static TemporaryFolder hadoopWarehouse = new TemporaryFolder();

  @BeforeClass
  public static void createWarehouse() throws IOException {
    hiveWarehouse.create();
    hadoopWarehouse.create();
  }

  @AfterClass
  public static void dropWarehouse() {
    hiveWarehouse.delete();
    hadoopWarehouse.delete();
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH %s", catalogName, toWithClause(config));
  }

  @After
  public void clean() {
    sql("DROP CATALOG IF EXISTS %s", catalogName);
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

  protected final String catalogName;
  protected final String[] baseNamespace;
  protected final Catalog validationCatalog;
  protected final SupportsNamespaces validationNamespaceCatalog;
  protected final Map<String, String> config = Maps.newHashMap();

  protected final String flinkDatabase;
  protected final Namespace icebergNamespace;
  protected final boolean isHadoopCatalog;

  public FlinkCatalogTestBase(String catalogName, String[] baseNamespace) {
    this.catalogName = catalogName;
    this.baseNamespace = baseNamespace;
    this.isHadoopCatalog = catalogName.startsWith("testhadoop");
    this.validationCatalog = isHadoopCatalog ?
        new HadoopCatalog(hiveConf, "file:" + hadoopWarehouse.getRoot()) :
        catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    config.put("type", "iceberg");
    if (baseNamespace.length > 0) {
      config.put(FlinkCatalogFactory.BASE_NAMESPACE, Joiner.on(".").join(baseNamespace));
    }
    if (isHadoopCatalog) {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop");
      config.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + hadoopWarehouse.getRoot());
    } else {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
      config.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + hiveWarehouse.getRoot());
      config.put(CatalogProperties.HIVE_URI, getURI(hiveConf));
    }

    this.flinkDatabase = catalogName + "." + DATABASE;
    this.icebergNamespace = Namespace.of(ArrayUtils.concat(baseNamespace, new String[] {DATABASE}));
  }

  static String getURI(HiveConf conf) {
    return conf.get(HiveConf.ConfVars.METASTOREURIS.varname);
  }

  static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder.append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(entry.getValue()).append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }

  static TableEnvironment createTableEnv(boolean isStreamingJob) {
    EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner();
    if (isStreamingJob) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
      env.enableCheckpointing(400);
      return StreamTableEnvironment.create(env, settingsBuilder.inStreamingMode().build());
    } else {
      return TableEnvironment.create(settingsBuilder.inBatchMode().build());
    }
  }
}
