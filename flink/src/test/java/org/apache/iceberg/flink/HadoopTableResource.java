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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class HadoopTableResource extends ExternalResource {

  private final TemporaryFolder temporaryFolder;
  private final String database;
  private final String tableName;
  private final Schema schema;

  private HadoopCatalog catalog;
  private TableLoader tableLoader;
  private Table table;

  public HadoopTableResource(TemporaryFolder temporaryFolder, String database, String tableName, Schema schema) {
    this.temporaryFolder = temporaryFolder;
    this.database = database;
    this.tableName = tableName;
    this.schema = schema;
  }

  @Override
  protected void before() throws Throwable {
    File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    this.catalog = new HadoopCatalog(hadoopConf, warehouse);
    String location = String.format("%s/%s/%s", warehouse, database, tableName);
    this.tableLoader = TableLoader.fromHadoopTable(location);
    this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema);
    tableLoader.open();
  }

  @Override
  protected void after() {
    try {
      catalog.dropTable(TableIdentifier.of(database, tableName));
      catalog.close();
      tableLoader.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close catalog resource");
    }
  }

  public TableLoader tableLoader() {
    return tableLoader;
  }

  public Table table() {
    return table;
  }
}
