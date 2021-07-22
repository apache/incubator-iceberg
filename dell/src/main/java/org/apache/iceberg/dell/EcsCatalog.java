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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECS catalog implementation
 */
public class EcsCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Cloneable {

  private static final Logger log = LoggerFactory.getLogger(EcsCatalog.class);

  private String catalogName;
  private EcsClient ecs;

  /**
   * @param name       a custom name for the catalog
   * @param properties catalog properties
   */
  @Override
  public void initialize(String name, Map<String, String> properties) {
    catalogName = name;
    ecs = EcsClient.create(properties);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return ecs.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    ObjectKey tableMetadataKey = ecs.getKeys().getMetadataKey(identifier);
    if (purge) {
      // if re-use the same instance, current() will throw exception.
      TableOperations ops = newTableOps(identifier);
      TableMetadata current = ops.current();
      if (current == null) {
        return false;
      }
      CatalogUtil.dropTableData(ops.io(), current);
    }
    ecs.deleteObject(tableMetadataKey);
    return true;
  }

  /**
   * rename table only move table object, the data and metadata will still be in-place.
   *
   * @param from identifier of the table to rename
   * @param to   new table name
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    ObjectKey fromKey = ecs.getKeys().getMetadataKey(from);
    ObjectKey toKey = ecs.getKeys().getMetadataKey(to);
    Optional<ObjectHeadInfo> fromHeadOpt = ecs.head(fromKey);
    if (!fromHeadOpt.isPresent()) {
      throw new NoSuchTableException("table %s(%s) is absent", from, fromKey);
    }
    String eTag = fromHeadOpt.get().getETag();
    if (!ecs.copyObjectIfAbsent(fromKey, eTag, toKey)) {
      throw new AlreadyExistsException("table %s is present", to);
    }
    log.info("rename table {} to {}", from, to);
    ecs.deleteObject(fromKey);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException("namespace %s has already existed", namespace);
    }
    ObjectKey metadataKey = ecs.getKeys().getMetadataKey(namespace);
    if (ecs.head(metadataKey).isPresent()) {
      throw new AlreadyExistsException("namespace %s(%s) has already existed", namespace, metadataKey);
    }
    if (!ecs.writePropertiesIfAbsent(metadataKey, metadata)) {
      throw new AlreadyExistsException("namespace %s(%s) has already existed", namespace, metadataKey);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return ecs.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    ecs.assertNamespaceExist(namespace);
    ObjectKey metadataKey = ecs.getKeys().getMetadataKey(namespace);
    if (namespace.isEmpty()) {
      if (!ecs.head(metadataKey).isPresent()) {
        return Collections.emptyMap();
      }
    }
    return ecs.readProperties(metadataKey);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (namespace.isEmpty()) {
      // empty namespace can't be dropped
      return false;
    }
    ObjectKey metadataKey = ecs.getKeys().getMetadataKey(namespace);
    if (!listNamespaces(namespace).isEmpty() || !listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("namespace %s is not empty", namespace);
    }
    ecs.deleteObject(metadataKey);
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    Map<String, String> namespaceMetadata = loadNamespaceMetadata(namespace);
    String eTag = namespaceMetadata.get(EcsClient.E_TAG_KEY);
    if (eTag == null) {
      throw new UnsupportedOperationException("eTag isn't in properties");
    }
    ObjectKey metadataKey = ecs.getKeys().getMetadataKey(namespace);
    Map<String, String> newProperties = new HashMap<>(namespaceMetadata);
    newProperties.putAll(properties);
    newProperties.remove(EcsClient.E_TAG_KEY);
    return ecs.replaceProperties(metadataKey, eTag, newProperties);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    Map<String, String> namespaceMetadata = loadNamespaceMetadata(namespace);
    String eTag = namespaceMetadata.get(EcsClient.E_TAG_KEY);
    if (eTag == null) {
      throw new UnsupportedOperationException("eTag isn't in properties");
    }
    ObjectKey metadataKey = ecs.getKeys().getMetadataKey(namespace);
    Map<String, String> newProperties = new HashMap<>(namespaceMetadata);
    newProperties.keySet().removeAll(properties);
    newProperties.remove(EcsClient.E_TAG_KEY);
    return ecs.replaceProperties(metadataKey, eTag, newProperties);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    if (namespace.isEmpty()) {
      return true;
    }
    return ecs.head(ecs.getKeys().getMetadataKey(namespace)).isPresent();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new EcsTableOperations(
        catalogName + "." + tableIdentifier,
        tableIdentifier,
        ecs,
        new EcsFileIO(ecs.copy()));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return ecs.getKeys().toString(ecs.getKeys().warehouseLocation(tableIdentifier));
  }
}
