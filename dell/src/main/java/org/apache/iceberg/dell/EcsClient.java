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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

/**
 * ECS client and its util-methods.
 */
public interface EcsClient extends AutoCloseable {

  /**
   * utils of object key that provide methods to convert {@link ObjectKey} to other objects declared in Iceberg.
   *
   * @return utils class
   */
  ObjectKeys getKeys();

  /**
   * utils of properties that provide methods to convert object to properties.
   *
   * @return utils class
   */
  default PropertiesSerDes getPropertiesSerDes() {
    return PropertiesSerDes.useJdk();
  }

  /**
   * get original properties.
   * <p>
   * when try to implement this interface, make this properties same as input of {@link EcsClient#create(Map)}
   *
   * @return properties of client
   */
  Map<String, String> getProperties();

  /**
   * get object info of specific key. If object is absent, return {@link Optional#empty()}
   *
   * @param key object key
   * @return head info if present
   */
  Optional<ObjectHeadInfo> head(ObjectKey key);

  /**
   * get object input stream of specific key.
   *
   * @param key object key
   * @return input stream
   */
  InputStream inputStream(ObjectKey key, long pos);

  /**
   * get object output stream of specific key.
   *
   * @param key object key
   * @return output stream
   */
  OutputStream outputStream(ObjectKey key);

  /**
   * return tuple of {@link #readAll(ObjectKey)}
   */
  interface ContentAndETag {
    ObjectHeadInfo getHeadInfo();

    byte[] getContent();
  }

  /**
   * read all bytes of object
   *
   * @return bytes and e-tag
   */
  ContentAndETag readAll(ObjectKey key);

  /**
   * a util method of {@link #readAll(ObjectKey)}
   */
  default Map<String, String> readProperties(ObjectKey key) {
    ContentAndETag contentAndETag = readAll(key);
    return getPropertiesSerDes().readProperties(
        contentAndETag.getContent(),
        contentAndETag.getHeadInfo().getETag(),
        contentAndETag.getHeadInfo().getUserMetadata().getOrDefault(
            PROPERTY_VERSION_KEY,
            PropertiesSerDes.CURRENT_VERSION));
  }

  /**
   * CAS operation
   * <p>
   * we assume that E-Tag can distinct content of the objects.
   * <p>
   * if current object's eTag is not matched, the method will return false, and the object won't be changed
   */
  boolean replace(ObjectKey key, String eTag, byte[] bytes, Map<String, String> userMetadata);

  /**
   * a util method of {@link #replace(ObjectKey, String, byte[], Map)}
   */
  default boolean replaceProperties(ObjectKey key, String eTag, Map<String, String> properties) {
    return replace(key, eTag, getPropertiesSerDes().toBytes(properties),
        Collections.singletonMap(PROPERTY_VERSION_KEY, PropertiesSerDes.CURRENT_VERSION));
  }

  /**
   * compare-and-swap operation
   * <p>
   * if current key is not existed, the method will return false, and the key is still absent.
   */
  boolean writeIfAbsent(ObjectKey key, byte[] bytes, Map<String, String> userMetadata);

  /**
   * a util method of {@link #writeIfAbsent(ObjectKey, byte[], Map)}
   */
  default boolean writePropertiesIfAbsent(ObjectKey key, Map<String, String> properties) {
    return writeIfAbsent(key, getPropertiesSerDes().toBytes(properties),
        Collections.singletonMap(PROPERTY_VERSION_KEY, PropertiesSerDes.CURRENT_VERSION));
  }

  /**
   * compare-and-swap operation
   * <p>
   * we assume that E-Tag can distinct content of the objects.
   */
  boolean copyObjectIfAbsent(ObjectKey fromKey, String eTag, ObjectKey toKey);

  /**
   * delete object
   *
   * @param key is object key
   */
  void deleteObject(ObjectKey key);

  /**
   * list all objects with delimiter.
   * <p>
   * for example: there are objects like:
   * <ul>
   * <li>namespace1/namespace2.namespace</li>
   * <li>namespace1/namespace2/table1.table</li>
   * <li>namespace1/table1.table</li>
   * <li>namespace1/table2.table</li>
   * </ul>
   * if prefix is namespace1 and delimiter is /, then return value will be
   * <ul>
   * <li>namespace1/table1.table</li>
   * <li>namespace1/table2.table</li>
   * </ul>
   * <p>
   * The function will can filter and convert to the object that user want to use
   * <p>
   * note: the common prefixes, such as namespace1/namespace2/, won't return by this method.
   *
   * @param prefix          prefix key
   * @param filterAndMapper map object key to specify item
   * @param <T>             search item type
   * @return all items with given prefix
   */
  <T> List<T> listDelimiterAll(ObjectKey prefix, Function<ObjectKey, Optional<T>> filterAndMapper);

  /**
   * list all tables under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for tables
   */
  default List<TableIdentifier> listTables(Namespace namespace) throws NoSuchNamespaceException {
    assertNamespaceExist(namespace);
    return listDelimiterAll(getKeys().getPrefix(namespace), key -> getKeys().extractTableIdentifier(key, namespace));
  }

  /**
   * list all namespaces under this namespace.
   *
   * @param namespace a namespace
   * @return a list of namespace
   */
  default List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    assertNamespaceExist(namespace);
    return listDelimiterAll(getKeys().getPrefix(namespace), key -> getKeys().extractNamespace(key, namespace));
  }

  /**
   * check namespace object existence. If not exist, throw exception.
   *
   * @param namespace is a non-null namespace
   * @throws NoSuchNamespaceException if namespace object is absent
   */
  default void assertNamespaceExist(Namespace namespace) throws NoSuchNamespaceException {
    if (namespace.isEmpty()) {
      return;
    }
    ObjectKey key = getKeys().getMetadataKey(namespace);
    if (!head(key).isPresent()) {
      throw new NoSuchNamespaceException("namespace %s(%s) is not found", namespace, key);
    }
  }

  /**
   * copy a client
   *
   * @return a new ecs client
   */
  default EcsClient copy() {
    return EcsClient.create(getProperties());
  }

  /**
   * ETag property name in results
   *
   * @see #readProperties(ObjectKey)
   */
  String E_TAG_KEY = "ecs-object-e-tag";

  /**
   * version property name in results
   *
   * @see #readProperties(ObjectKey)
   */
  String PROPERTY_VERSION_KEY = "ecs-object-property-version";

  /**
   * static factory method of {@link EcsClient}
   *
   * @param properties is properties
   * @return ecs client
   */
  static EcsClient create(Map<String, String> properties) {
    return EcsCatalogProperties.getEcsClientFromFactory(properties)
        .orElseGet(() -> EcsCatalogProperties.getBuiltInEcsClient(properties));
  }
}
