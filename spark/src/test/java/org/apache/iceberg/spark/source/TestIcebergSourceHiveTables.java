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

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.PlanScanAction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.After;
import org.junit.BeforeClass;

public abstract class TestIcebergSourceHiveTables extends TestIcebergSourceTablesBase {

  private static TableIdentifier currentIdentifier;

  public TestIcebergSourceHiveTables(PlanScanAction.PlanMode planMode) {
    super(planMode);
  }

  @BeforeClass
  public static void start() {
    Namespace db = Namespace.of("db");
    if (!catalog.namespaceExists(db)) {
      catalog.createNamespace(db);
    }
  }

  @After
  public void dropTable() throws IOException {
    Table table = catalog.loadTable(currentIdentifier);
    Path tablePath = new Path(table.location());
    FileSystem fs = tablePath.getFileSystem(spark.sessionState().newHadoopConf());
    fs.delete(tablePath, true);
    catalog.dropTable(currentIdentifier, false);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    TestIcebergSourceHiveTables.currentIdentifier = ident;
    return TestIcebergSourceHiveTables.catalog.createTable(ident, schema, spec);
  }

  @Override
  public Table loadTable(TableIdentifier ident, String entriesSuffix) {
    TableIdentifier identifier = TableIdentifier.of(ident.namespace().level(0), ident.name(), entriesSuffix);
    return TestIcebergSourceHiveTables.catalog.loadTable(identifier);
  }

  @Override
  public String loadLocation(TableIdentifier ident, String entriesSuffix) {
    return String.format("%s.%s", loadLocation(ident), entriesSuffix);
  }

  @Override
  public String loadLocation(TableIdentifier ident) {
    return ident.toString();
  }
}
