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

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBranchVisibility extends BaseTestIceberg {


  private final TableIdentifier tableIdentifier1 = TableIdentifier.of("test-ns", "table1");
  private final TableIdentifier tableIdentifier2 = TableIdentifier.of("test-ns", "table2");
  private NessieCatalog testCatalog;
  private int schemaCounter = 1;

  public TestBranchVisibility() {
    super("main");
  }


  @Before
  public void before() throws NessieNotFoundException, NessieConflictException {
    createTable(tableIdentifier1, 1); // table 1
    createTable(tableIdentifier2, 1); // table 2
    catalog.refresh();
    createBranch("test", catalog.currentHash());
    testCatalog = initCatalog("test");
  }

  @After
  public void after() throws NessieNotFoundException, NessieConflictException {
    catalog.dropTable(tableIdentifier1);
    catalog.dropTable(tableIdentifier2);
    catalog.refresh();
    catalog.getTreeApi().deleteBranch("test",
        catalog.getTreeApi().getReferenceByName("test").getHash());
    testCatalog = null;
  }

  @Test
  public void testBranchNoChange() {
    testCatalogEquality(catalog, testCatalog, true, true);
  }

  @Test
  public void testUpdateCatalogs() {
    // ensure catalogs can't see each others updates
    updateSchema(catalog, tableIdentifier1);

    testCatalogEquality(catalog, testCatalog, false, true);

    String initialMetadataLocation = metadataLocation(testCatalog, tableIdentifier2);
    updateSchema(testCatalog, tableIdentifier2);

    testCatalogEquality(catalog, testCatalog, false, false);

    // points to the previous metadata location
    Assert.assertEquals(initialMetadataLocation, metadataLocation(catalog, tableIdentifier2));
  }

  @Test
  public void testCatalogOnReference() {
    updateSchema(catalog, tableIdentifier1);
    updateSchema(testCatalog, tableIdentifier2);
    String mainHash = catalog.referenceByName("main").hash();

    // catalog created with ref points to same catalog as above
    NessieCatalog refCatalog = initCatalog("test");
    testCatalogEquality(refCatalog, testCatalog, true, true);

    // catalog created with hash points to same catalog as above
    NessieCatalog refHashCatalog = initCatalog(mainHash);
    testCatalogEquality(refHashCatalog, catalog, true, true);
  }

  @Test
  public void testCatalogWithTableNames() {
    updateSchema(testCatalog, tableIdentifier2);
    String mainHash = catalog.referenceByName("main").hash();

    // asking for table@branch gives expected regardless of catalog
    Assert.assertEquals(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@test")),
        metadataLocation(testCatalog, tableIdentifier1));

    // asking for table@branch#hash gives expected regardless of catalog
    Assert.assertEquals(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@" + mainHash)),
        metadataLocation(testCatalog, tableIdentifier1));
  }

  @Test
  public void testConcurrentChanges() {
    NessieCatalog emptyTestCatalog = initCatalog("test");
    updateSchema(testCatalog, tableIdentifier1);
    // Updating table with out of date hash. We expect this to succeed because of retry despite the conflict.
    updateSchema(emptyTestCatalog, tableIdentifier1);
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier) {
    catalog.loadTable(identifier).updateSchema().addColumn("id" + schemaCounter++, Types.LongType.get()).commit();
  }

  private void testCatalogEquality(NessieCatalog catalog,
                                   NessieCatalog compareCatalog,
                                   boolean table1Equal,
                                   boolean table2Equal) {
    String testTable1 = metadataLocation(compareCatalog, tableIdentifier1);
    String table1 = metadataLocation(catalog, tableIdentifier1);
    String testTable2 = metadataLocation(compareCatalog, tableIdentifier2);
    String table2 = metadataLocation(catalog, tableIdentifier2);

    String msg1 = String.format("Table %s on ref %s should%s equal table %s on ref %s", tableIdentifier1.name(),
        tableIdentifier2.name(), table1Equal ? "" : " not", catalog.currentRefName(), testCatalog.currentRefName());
    Assert.assertEquals(msg1, table1Equal, table1.equals(testTable1));
    String msg2 = String.format("Table %s on ref %s should%s equal table %s on ref %s", tableIdentifier1.name(),
        tableIdentifier2.name(), table1Equal ? "" : " not", catalog.currentRefName(), testCatalog.currentRefName());
    Assert.assertEquals(msg2, table2Equal, table2.equals(testTable2));
  }

}
