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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class FlinkCatalogFactoryOptions {
  private FlinkCatalogFactoryOptions() {
  }

  public static final String IDENTIFIER = "iceberg";

  public static final ConfigOption<String> CATALOG_TYPE =
      ConfigOptions.key("catalog-type")
          .stringType()
          .noDefaultValue()
          .withDescription("Iceberg catalog type, 'hive' or 'hadoop'");

  public static final ConfigOption<String> PROPERTY_VERSION =
      ConfigOptions.key("property-version")
          .stringType()
          .defaultValue("1")
          .withDescription(
              "Version number to describe the property version. This property can be used for backwards " +
                  "compatibility in case the property format changes. The current property version is `1`. (Optional)");

  public static final ConfigOption<String> URI =
      ConfigOptions.key("uri")
          .stringType()
          .noDefaultValue()
          .withDescription("The Hive Metastore URI (Hive catalog only)");

  public static final ConfigOption<String> CLIENTS =
      ConfigOptions.key("clients")
          .stringType()
          .noDefaultValue()
          .withDescription("The Hive Client Pool Size (Hive catalog only)");

  public static final ConfigOption<String> HIVE_CONF_DIF =
      ConfigOptions.key("hive-conf-dir")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Path to a directory containing a hive-site.xml configuration file which will be used to provide " +
                  "custom Hive configuration values. The value of hive.metastore.warehouse.dir from" +
                  " <hive-conf-dir>/hive-site.xml (or hive configure file from classpath) will be overwrote with " +
                  "the warehouse value if setting both hive-conf-dir and warehouse when creating iceberg catalog.");

  public static final ConfigOption<String> BASE_NAMESPACE =
      ConfigOptions.key("base-namespace")
          .stringType()
          .noDefaultValue()
          .withDescription("A base namespace as the prefix for all databases (Hadoop catalog only)");

  public static final ConfigOption<String> WAREHOUSE =
      ConfigOptions.key("warehouse")
          .stringType()
          .noDefaultValue()
          .withDescription("The warehouse path (Hadoop catalog only)");

  public static final ConfigOption<String> CACHE_ENABLED =
      ConfigOptions.key("cache-enabled")
          .stringType()
          .defaultValue("true")
          .withDescription("Whether to cache the catalog in FlinkCatalog.");
}
