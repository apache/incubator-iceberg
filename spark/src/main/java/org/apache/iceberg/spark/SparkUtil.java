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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;

public class SparkUtil {

  public static final String HANDLE_TIMESTAMP_WITHOUT_TIMEZONE =
          "spark.sql.iceberg.handle-timestamp-without-timezone";
  public static final String TIMESTAMP_WITHOUT_TIMEZONE_ERROR = String.format("Cannot handle timestamp without" +
          " timezone fields in Spark. Spark does not natively support this type but if you would like to handle all" +
          " timestamps as timestamp with timezone set '%s' to true. This will not change the underlying values stored" +
          " but will change their displayed values in Spark. For more information please see" +
          " https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#ansi-sql-and" +
          "-spark-sql-timestamps", HANDLE_TIMESTAMP_WITHOUT_TIMEZONE);
  public static final String USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES =
          "spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables";

  private static final String SPARK_CATALOG_CONF_PREFIX = "spark.sql.catalog";
  // Format string used as the prefix for spark configuration keys to override hadoop configuration values
  // for Iceberg tables from a given catalog. These keys can be specified as `spark.sql.catalog.$catalogName.hadoop.*`,
  // similar to using `spark.hadoop.*` to override hadoop configurations globally for a given spark session.
  private static final String SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR = SPARK_CATALOG_CONF_PREFIX + ".%s.hadoop.";

  private SparkUtil() {
  }

  public static FileIO serializableFileIO(Table table) {
    if (table.io() instanceof HadoopConfigurable) {
      // we need to use Spark's SerializableConfiguration to avoid issues with Kryo serialization
      ((HadoopConfigurable) table.io()).serializeConfWith(conf -> new SerializableConfiguration(conf)::value);
    }

    return table.io();
  }

  /**
   * Check whether the partition transforms in a spec can be used to write data.
   *
   * @param spec a PartitionSpec
   * @throws UnsupportedOperationException if the spec contains unknown partition transforms
   */
  public static void validatePartitionTransforms(PartitionSpec spec) {
    if (spec.fields().stream().anyMatch(field -> field.transform() instanceof UnknownTransform)) {
      String unsupported = spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .map(Transform::toString)
          .collect(Collectors.joining(", "));

      throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
    }
  }

  /**
   * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply
   * Attempts to find the catalog and identifier a multipart identifier represents
   * @param nameParts Multipart identifier representing a table
   * @return The CatalogPlugin and Identifier for the table
   */
  public static <C, T> Pair<C, T> catalogAndIdentifier(List<String> nameParts,
                                                       Function<String, C> catalogProvider,
                                                       BiFunction<String[], String, T> identifierProvider,
                                                       C currentCatalog,
                                                       String[] currentNamespace) {
    Preconditions.checkArgument(!nameParts.isEmpty(),
        "Cannot determine catalog and identifier from empty name");

    int lastElementIndex = nameParts.size() - 1;
    String name = nameParts.get(lastElementIndex);

    if (nameParts.size() == 1) {
      // Only a single element, use current catalog and namespace
      return Pair.of(currentCatalog, identifierProvider.apply(currentNamespace, name));
    } else {
      C catalog = catalogProvider.apply(nameParts.get(0));
      if (catalog == null) {
        // The first element was not a valid catalog, treat it like part of the namespace
        String[] namespace =  nameParts.subList(0, lastElementIndex).toArray(new String[0]);
        return Pair.of(currentCatalog, identifierProvider.apply(namespace, name));
      } else {
        // Assume the first element is a valid catalog
        String[] namespace = nameParts.subList(1, lastElementIndex).toArray(new String[0]);
        return Pair.of(catalog, identifierProvider.apply(namespace, name));
      }
    }
  }

  /**
   * Responsible for checking if the table schema has a timestamp without timezone column
   * @param schema table schema to check if it contains a timestamp without timezone column
   * @return boolean indicating if the schema passed in has a timestamp field without a timezone
   */
  public static boolean hasTimestampWithoutZone(Schema schema) {
    return TypeUtil.find(schema, t -> Types.TimestampType.withoutZone().equals(t)) != null;
  }

  /**
   * Allow reading/writing timestamp without time zone as timestamp with time zone. Generally,
   * this is not safe as timestamp without time zone is supposed to represent wall clock time semantics,
   * i.e. no matter the reader/writer timezone 3PM should always be read as 3PM,
   * but timestamp with time zone represents instant semantics, i.e the timestamp
   * is adjusted so that the corresponding time in the reader timezone is displayed.
   * When set to false (default), we throw an exception at runtime
   * "Spark does not support timestamp without time zone fields" if reading timestamp without time zone fields
   *
   * @param readerConfig table read options
   * @param sessionConf spark session configurations
   * @return boolean indicating if reading timestamps without timezone is allowed
   */
  public static boolean canHandleTimestampWithoutZone(Map<String, String> readerConfig, RuntimeConfig sessionConf) {
    String readerOption = readerConfig.get(HANDLE_TIMESTAMP_WITHOUT_TIMEZONE);
    if (readerOption != null) {
      return Boolean.parseBoolean(readerOption);
    }
    String sessionConfValue = sessionConf.get(HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, null);
    if (sessionConfValue != null) {
      return Boolean.parseBoolean(sessionConfValue);
    }
    return false;
  }

  /**
   * Check whether the spark session config contains a {@link SparkUtil#USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES}
   * property.
   * Default value - false
   * If true in new table all timestamp fields will be stored as {@link Types.TimestampType#withoutZone()},
   * otherwise {@link Types.TimestampType#withZone()} will be used
   *
   * @param sessionConf a spark runtime config
   * @return true if the session config has {@link SparkUtil#USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES} property
   * and this property is set to true
   */
  public static boolean useTimestampWithoutZoneInNewTables(RuntimeConfig sessionConf) {
    String sessionConfValue = sessionConf.get(USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES, null);
    if (sessionConfValue != null) {
      return Boolean.parseBoolean(sessionConfValue);
    }
    return false;
  }

  /**
   * Pulls any Catalog specific overrides for the Hadoop conf from the current SparkSession, which can be
   * set via `spark.sql.catalog.$catalogName.hadoop.*`
   *
   * Mirrors the override of hadoop configurations for a given spark session using `spark.hadoop.*`.
   *
   * The SparkCatalog allows for hadoop configurations to be overridden per catalog, by setting
   * them on the SQLConf, where the following will add the property "fs.default.name" with value
   * "hdfs://hanksnamenode:8020" to the catalog's hadoop configuration.
   *   SparkSession.builder()
   *     .config(s"spark.sql.catalog.$catalogName.hadoop.fs.default.name", "hdfs://hanksnamenode:8020")
   *     .getOrCreate()
   * @param spark The current Spark session
   * @param catalogName Name of the catalog to find overrides for.
   * @return the Hadoop Configuration that should be used for this catalog, with catalog specific overrides applied.
   */
  public static Configuration hadoopConfCatalogOverrides(SparkSession spark, String catalogName) {
    // Find keys for the catalog intended to be hadoop configurations
    final String hadoopConfCatalogPrefix = hadoopConfPrefixForCatalog(catalogName);
    final Configuration conf = spark.sessionState().newHadoopConf();
    spark.sqlContext().conf().settings().forEach((k, v) -> {
      // These checks are copied from `spark.sessionState().newHadoopConfWithOptions()`, which we
      // avoid using to not have to convert back and forth between scala / java map types.
      if (v != null && k != null && k.startsWith(hadoopConfCatalogPrefix)) {
        conf.set(k.substring(hadoopConfCatalogPrefix.length()), v);
      }
    });
    return conf;
  }

  private static String hadoopConfPrefixForCatalog(String catalogName) {
    return String.format(SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR, catalogName);
  }
}
