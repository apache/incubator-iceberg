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

import java.io.Serializable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Serializable loader to load an Iceberg {@link Catalog}.
 */
public interface CatalogLoader extends Serializable {

  /**
   * Create a new catalog with the provided properties. NOTICE: for flink, we may initialize the {@link CatalogLoader}
   * at flink sql client side or job manager side, and then serialize this catalog loader to task manager, finally
   * deserialize it and create a new catalog at task manager side.
   *
   * @return a newly created {@link Catalog}
   */
  Catalog loadCatalog();

  static CatalogLoader hadoop(String name, Configuration hadoopConf, Map<String, String> properties) {
    return new HadoopCatalogLoader(name, hadoopConf, properties);
  }

  static CatalogLoader hive(String name, Configuration hadoopConf, Map<String, String> properties) {
    String hiveConfDir = properties.get(FlinkCatalogFactory.HIVE_CONF_DIR);
    Configuration newHadoopConf = FlinkCatalogFactory.mergeHiveConf(hadoopConf, hiveConfDir);
    return new HiveCatalogLoader(name, newHadoopConf, properties);
  }

  static CatalogLoader custom(String name, Map<String, String> properties, Configuration hadoopConf, String impl) {
    return new CustomCatalogLoader(name, properties, hadoopConf, impl);
  }

  class HadoopCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final SerializableConfiguration hadoopConf;
    private final String warehouseLocation;
    private final Map<String, String> properties;

    private HadoopCatalogLoader(
        String catalogName,
        Configuration conf,
        Map<String, String> properties) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), catalogName, properties, hadoopConf.get());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("warehouseLocation", warehouseLocation)
          .toString();
    }
  }

  class HiveCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final SerializableConfiguration hadoopConf;
    private final String uri;
    private final String warehouse;
    private final int clientPoolSize;
    private final Map<String, String> properties;

    private HiveCatalogLoader(String catalogName, Configuration conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.uri = properties.get(CatalogProperties.URI);
      this.warehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.clientPoolSize = properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE) ?
          Integer.parseInt(properties.get(CatalogProperties.CLIENT_POOL_SIZE)) :
          CatalogProperties.CLIENT_POOL_SIZE_DEFAULT;
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(HiveCatalog.class.getName(), catalogName, properties, hadoopConf.get());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("uri", uri)
          .add("warehouse", warehouse)
          .add("clientPoolSize", clientPoolSize)
          .toString();
    }
  }

  class CustomCatalogLoader implements CatalogLoader {

    private final SerializableConfiguration hadoopConf;
    private final Map<String, String> properties;
    private final String name;
    private final String impl;

    private CustomCatalogLoader(
        String name,
        Map<String, String> properties,
        Configuration conf,
        String impl) {
      this.hadoopConf = new SerializableConfiguration(conf);
      this.properties = Maps.newHashMap(properties); // wrap into a hashmap for serialization
      this.name = name;
      this.impl = Preconditions.checkNotNull(impl, "Cannot initialize custom Catalog, impl class name is null");
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(impl, name, properties, hadoopConf.get());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("impl", impl)
          .toString();
    }
  }

}
