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

package org.apache.iceberg.flink.actions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;

public class Actions {

  public static final Configuration CONFIG = new Configuration()
      // disable classloader check as Avro may cache class/object in the serializers.
      .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  private StreamExecutionEnvironment env;
  private Table table;

  private Actions(StreamExecutionEnvironment env, Table table) {
    this.env = env;
    this.table = table;
  }

  public static Actions forTable(StreamExecutionEnvironment env, Table table) {
    return new Actions(env, table);
  }

  public static Actions forTable(Table table) {
    return new Actions(StreamExecutionEnvironment.getExecutionEnvironment(CONFIG), table);
  }

  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(env, table);
  }

  /**
   * Migrate an exist hive table into iceberg table
   *
   * @param flinkHiveCatalog    the HiveCatalog of flink, we get hive table info from this
   * @param hiveSourceDatabaseName    source hive database name
   * @param hiveSourceTableName source hive table name
   * @param icebergCatalog      target iceberg catalog
   * @param baseNamespace       target iceberg Namespace
   * @param icebergDbName       target iceberg database name
   * @param icebergTableName    target iceberg table name
   * @return the MigrateAction
   */
  public static MigrateAction migrateHive2Iceberg(HiveCatalog flinkHiveCatalog, String hiveSourceDatabaseName,
                                                  String hiveSourceTableName, Catalog icebergCatalog,
                                                  Namespace baseNamespace,
                                                  String icebergDbName, String icebergTableName) {
    return migrateHive2Iceberg(StreamExecutionEnvironment.getExecutionEnvironment(), flinkHiveCatalog,
        hiveSourceDatabaseName,
        hiveSourceTableName, icebergCatalog, baseNamespace,
        icebergDbName, icebergTableName);
  }

  public static MigrateAction migrateHive2Iceberg(StreamExecutionEnvironment env, HiveCatalog flinkHiveCatalog,
                                                  String hiveSourceDatabaseName, String hiveSourceTableName,
                                                  Catalog icebergCatalog, Namespace baseNamespace,
                                                  String icebergDbName, String icebergTableName) {
    return new MigrateAction(env, flinkHiveCatalog, hiveSourceDatabaseName, hiveSourceTableName, icebergCatalog,
        baseNamespace == null ? Namespace.empty() : baseNamespace,
        icebergDbName, icebergTableName);
  }
}
