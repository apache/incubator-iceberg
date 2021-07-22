/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EcsCatalogTest {
  /**
   * ecs catalog
   */
  private EcsCatalog ecsCatalog;

  // namespaces
  private Namespace namespaceL1 = Namespace.of("namespace");
  private Namespace namespaceL2 = Namespace.of("namespace", "namespace");
  private Namespace namespaceL2N2 = Namespace.of("namespace", "namespace2");

  // tables
  private TableIdentifier tableIdL0 = TableIdentifier.of("table");
  private TableIdentifier tableIdL1 = TableIdentifier.of(namespaceL1, "table");
  private TableIdentifier tableIdL2 = TableIdentifier.of(namespaceL2, "table");
  private TableIdentifier tableIdL2T2 = TableIdentifier.of(namespaceL2, "table2");

  @Before
  public void init() {
    ecsCatalog = new EcsCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put(EcsCatalogProperties.ECS_CLIENT_FACTORY, "org.apache.iceberg.dell.MemoryEcsClient#create");
    properties.put(EcsCatalogProperties.BASE_KEY, "test");
    ecsCatalog.initialize("test", properties);
  }

  public void prepareData() {
    ecsCatalog.createNamespace(namespaceL1);
    ecsCatalog.createNamespace(namespaceL2);
    ecsCatalog.createNamespace(namespaceL2N2);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

    ecsCatalog.createTable(tableIdL0, schema);
    ecsCatalog.createTable(tableIdL1, schema);
    ecsCatalog.createTable(tableIdL2, schema);
    ecsCatalog.createTable(tableIdL2T2, schema);
  }

  @Test
  public void listTables() {
    assertTrue("no table in empty namespace", ecsCatalog.listTables(Namespace.empty()).isEmpty());

    prepareData();

    assertEquals(
        "tables in empty namespace",
        Collections.singletonList(tableIdL0),
        ecsCatalog.listTables(Namespace.empty()));

    assertEquals(
        "tables in l1 namespace",
        Collections.singletonList(tableIdL1),
        ecsCatalog.listTables(namespaceL1));

    assertEquals(
        "tables in l2 namespace",
        Arrays.asList(tableIdL2, tableIdL2T2),
        ecsCatalog.listTables(namespaceL2));
  }

  @Test(expected = NoSuchNamespaceException.class)
  public void listTables_invalidInput() {
    ecsCatalog.listTables(Namespace.of("l1"));
  }

  @Test
  public void dropTable() {
    prepareData();

    assertTrue("drop table success", ecsCatalog.dropTable(tableIdL0));
    assertFalse("re-drop table failed", ecsCatalog.dropTable(tableIdL0));

    assertTrue("no table in empty namespace", ecsCatalog.listTables(Namespace.empty()).isEmpty());
  }

  @Test
  public void renameTable() {
    prepareData();
    TableIdentifier toTable = TableIdentifier.of(namespaceL2, "to");

    ecsCatalog.renameTable(tableIdL0, toTable);

    assertTrue("no table in empty namespace", ecsCatalog.listTables(Namespace.empty()).isEmpty());

    assertEquals(
        "tables in l2 namespace",
        Arrays.asList(tableIdL2, tableIdL2T2, toTable),
        ecsCatalog.listTables(namespaceL2));
  }

  @Test(expected = NoSuchTableException.class)
  public void renameTable_invalidFrom() {
    ecsCatalog.renameTable(tableIdL0, tableIdL1);
  }

  @Test(expected = AlreadyExistsException.class)
  public void renameTable_invalidTo() {
    prepareData();

    ecsCatalog.renameTable(tableIdL0, tableIdL1);
  }

  @Test(expected = AlreadyExistsException.class)
  public void createNamespace_empty() {
    ecsCatalog.createNamespace(Namespace.empty());
  }

  @Test(expected = AlreadyExistsException.class)
  public void createNamespace_invalidInput() {
    prepareData();

    ecsCatalog.createNamespace(namespaceL1);
  }

  @Test
  public void listNamespaces() {
    assertTrue("no namespace in empty namespace", ecsCatalog.listNamespaces().isEmpty());
    assertTrue("no namespace in empty namespace", ecsCatalog.listNamespaces(Namespace.empty()).isEmpty());

    prepareData();

    assertEquals(
        "namespaces in empty namespace",
        Collections.singletonList(namespaceL1),
        ecsCatalog.listNamespaces(Namespace.empty()));

    assertEquals(
        "namespace in l1 namespace",
        Arrays.asList(namespaceL2, namespaceL2N2),
        ecsCatalog.listNamespaces(namespaceL1));
  }

  @Test(expected = NoSuchNamespaceException.class)
  public void listNamespaces_invalidInput() {
    ecsCatalog.listNamespaces(Namespace.of("l1"));
  }

  @Test
  public void loadNamespaceMetadata() {
    assertEquals("empty properties of empty namespace", Collections.emptyMap(),
        ecsCatalog.loadNamespaceMetadata(Namespace.empty()));

    ecsCatalog.createNamespace(namespaceL1, Collections.singletonMap("k1", "v1"));

    Map<String, String> resultMetadata = ecsCatalog.loadNamespaceMetadata(namespaceL1);
    assertEquals("metadata contains k1", "v1", resultMetadata.get("k1"));
    assertTrue("metadata contains eTag", resultMetadata.containsKey(EcsClient.E_TAG_KEY));
  }

  @Test(expected = NoSuchNamespaceException.class)
  public void loadNamespaceMetadata_invalidInput() {
    ecsCatalog.loadNamespaceMetadata(namespaceL1);
  }

  @Test
  public void dropNamespace() {
    assertFalse("can't drop empty namespace", ecsCatalog.dropNamespace(Namespace.empty()));

    ecsCatalog.createNamespace(namespaceL1);

    assertEquals(
        "namespaces in empty namespace",
        Collections.singletonList(namespaceL1),
        ecsCatalog.listNamespaces(Namespace.empty()));

    assertTrue("drop namespace", ecsCatalog.dropNamespace(namespaceL1));

    assertTrue(
        "no namespace in empty namespace",
        ecsCatalog.listNamespaces(Namespace.empty()).isEmpty());
  }

  @Test(expected = NamespaceNotEmptyException.class)
  public void dropNamespace_invalidInput() {
    prepareData();

    ecsCatalog.dropNamespace(namespaceL1);
  }

  @Test
  public void setProperties() {
    ecsCatalog.createNamespace(namespaceL1, Collections.singletonMap("k1", "v1"));

    assertTrue(
        "set properties",
        ecsCatalog.setProperties(namespaceL1, Collections.singletonMap("k2", "v2")));

    Map<String, String> resultMap = ecsCatalog.loadNamespaceMetadata(namespaceL1);
    assertEquals("metadata contains k1", "v1", resultMap.get("k1"));
    assertEquals("metadata contains k2", "v2", resultMap.get("k2"));
  }

  @Test
  public void removeProperties() {
    Map<String, String> inputMetadata = new HashMap<>();
    inputMetadata.put("k1", "v1");
    inputMetadata.put("k2", "v2");
    ecsCatalog.createNamespace(namespaceL1, inputMetadata);

    assertTrue("remove properties", ecsCatalog.removeProperties(namespaceL1, Collections.singleton("k1")));

    Map<String, String> resultMap = ecsCatalog.loadNamespaceMetadata(namespaceL1);
    assertFalse("k1 is removed", resultMap.containsKey("k1"));
    assertEquals("metadata contains k2", "v2", resultMap.get("k2"));
  }
}