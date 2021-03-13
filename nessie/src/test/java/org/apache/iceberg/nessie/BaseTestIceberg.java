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

package org.apache.iceberg.nessie;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class BaseTestIceberg {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTestIceberg.class);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected NessieCatalog catalog;
  protected Configuration hadoopConfig;
  protected final String branch;
  private String path;
  protected NessieClient client;

  public BaseTestIceberg(String branch) {
    this.branch = branch;
  }

  private void resetData() {

  }

  @Before
  public void beforeEach() throws IOException {
    String port = System.getProperty("quarkus.http.test-port", "19120");
    path = String.format("http://localhost:%s/api/v1", port);
    hadoopConfig = new Configuration();
    catalog = initCatalog("main");
    client = NessieClient.none(path);

    catalog.listReferences().forEach(catalog::deleteReference);
    catalog.createBranch("main");
    if (!branch.equals("main")) {
      catalog.createBranch(branch);
    }
    catalog.setCurrentReference(branch);

  }

  NessieCatalog initCatalog(String ref) {
    NessieCatalog newCatalog = new NessieCatalog();
    newCatalog.setConf(hadoopConfig);
    newCatalog.initialize("nessie", ImmutableMap.of("ref", ref,
        "url", path,
        "auth_type", "NONE",
        CatalogProperties.WAREHOUSE_LOCATION, temp.getRoot().toURI().toString()
        ));
    return newCatalog;
  }

  protected Table createTable(TableIdentifier tableIdentifier, int count) {
    try {
      return catalog.createTable(tableIdentifier, schema(count));
    } catch (Throwable t) {
      LOGGER.error("unable to do create " + tableIdentifier.toString(), t);
      throw t;
    }
  }

  protected void createTable(TableIdentifier tableIdentifier) {
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get()))
                                         .fields());
    catalog.createTable(tableIdentifier, schema).location();
  }

  protected static Schema schema(int count) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  void createBranch(String name, String hash) throws NessieNotFoundException, NessieConflictException {
    catalog.createBranch(name, hash);
  }

  @After
  public void afterEach() throws Exception {
    catalog.close();
    catalog = null;
    hadoopConfig = null;
  }

  static String metadataLocation(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations icebergOps = (NessieTableOperations) ops;
    return icebergOps.currentMetadataLocation();
  }

}
