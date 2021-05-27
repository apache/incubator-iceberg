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

package org.apache.iceberg.mr.hive;

import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.util.SerializationUtil;

public final class IcebergSerializationUtil {
  private static final Splitter TABLE_NAME_SPLITTER = Splitter.on("..");

  private IcebergSerializationUtil() {
  }

  /**
   * Returns the Table serialized to the configuration based on the table name.
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table table(Configuration config, String name) {
    return SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
  }

  /**
   * Returns the names of the output tables stored in the configuration.
   * @param config The configuration used to get the data from
   * @return The collection of the table names as returned by TableDesc.getTableName()
   */
  public static Collection<String> outputTables(Configuration config) {
    return TABLE_NAME_SPLITTER.splitToList(config.get(InputFormatConfig.OUTPUT_TABLES));
  }

  /**
   * Returns the catalog name serialized to the configuration.
   * @param config The configuration used to get the data from
   * @param name The name of the table we neeed as returned by TableDesc.getTableName()
   * @return catalog name
   */
  public static String catalogName(Configuration config, String name) {
    return config.get(InputFormatConfig.TABLE_CATALOG_PREFIX + name);
  }

  /**
   * Returns the Table Schema serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table Schema object
   */
  public static Schema schema(Configuration config) {
    return SchemaParser.fromJson(config.get(InputFormatConfig.TABLE_SCHEMA));
  }
}
