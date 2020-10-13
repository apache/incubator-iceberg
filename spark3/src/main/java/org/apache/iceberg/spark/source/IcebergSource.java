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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class IcebergSource implements DataSourceRegister, TableProvider {
  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public SparkTable getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    // Get Iceberg table from options
    Configuration conf = SparkSession.active().sessionState().newHadoopConf();
    Table icebergTable = getTableAndResolveHadoopConfiguration(options, conf);

    // Build Spark table based on Iceberg table, and return it
    // Eagerly refresh the table before reading to ensure views containing this table show up-to-date data
    return new SparkTable(icebergTable, schema, true);
  }

  protected Table findTable(Map<String, String> options, Configuration conf) {
    Preconditions.checkArgument(options.containsKey("path"), "Cannot open table: path is not set");
    String path = options.get("path");

    if (path.contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path);
    } else {
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path);
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }

  private Table getTableAndResolveHadoopConfiguration(Map<String, String> options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options);

    Table table = findTable(options, conf);

    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());

    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options);

    return table;
  }

  private static void mergeIcebergHadoopConfs(Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
  }
}
