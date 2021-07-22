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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The IcebergSource loads/writes tables with format "iceberg". It can load paths and tables.
 *
 * How paths/tables are loaded when using spark.read().format("iceberg").path(table)
 *
 *  table = "file:/path/to/table" -&gt; loads a HadoopTable at given path
 *  table = "tablename" -&gt; loads currentCatalog.currentNamespace.tablename
 *  table = "catalog.tablename" -&gt; load "tablename" from the specified catalog.
 *  table = "namespace.tablename" -&gt; load "namespace.tablename" from current catalog
 *  table = "catalog.namespace.tablename" -&gt; "namespace.tablename" from the specified catalog.
 *  table = "namespace1.namespace2.tablename" -&gt; load "namespace1.namespace2.tablename" from current catalog
 *
 * The above list is in order of priority. For example: a matching catalog will take priority over any namespace
 * resolution.
 */
public class IcebergSource implements DataSourceRegister, SupportsCatalogOptions {
  private static final String DEFAULT_CATALOG_NAME = "default_iceberg";
  private static final String DEFAULT_CATALOG = "spark.sql.catalog." + DEFAULT_CATALOG_NAME;

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    Spark3Util.CatalogAndIdentifier catalogIdentifier = catalogAndIdentifier(new CaseInsensitiveStringMap(options));
    CatalogPlugin catalog = catalogIdentifier.catalog();
    Identifier ident = catalogIdentifier.identifier();

    try {
      if (catalog instanceof TableCatalog) {
        return ((TableCatalog) catalog).loadTable(ident);
      }
    } catch (NoSuchTableException e) {
      // throwing an iceberg NoSuchTableException because the Spark one is typed and cant be thrown from this interface
      throw new org.apache.iceberg.exceptions.NoSuchTableException(e, "Cannot find table for %s.", ident);
    }

    // throwing an iceberg NoSuchTableException because the Spark one is typed and cant be thrown from this interface
    throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot find table for %s.", ident);
  }

  private Spark3Util.CatalogAndIdentifier catalogAndIdentifier(CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(options.containsKey("path"), "Cannot open table: path is not set");
    SparkSession spark = SparkSession.active();
    setupDefaultSparkCatalog(spark);
    String path = options.get("path");
    Long snapshotId = Spark3Util.propertyAsLong(options, SparkReadOptions.SNAPSHOT_ID, null);
    Long asOfTimestamp = Spark3Util.propertyAsLong(options, SparkReadOptions.AS_OF_TIMESTAMP, null);
    CatalogManager catalogManager = spark.sessionState().catalogManager();

    if (path.contains("/")) {
      // contains a path. Return iceberg default catalog and a PathIdentifier
      return new Spark3Util.CatalogAndIdentifier(catalogManager.catalog(DEFAULT_CATALOG_NAME),
          new PathIdentifier(path, snapshotId, asOfTimestamp));
    }

    final Spark3Util.CatalogAndIdentifier catalogAndIdentifier = Spark3Util.catalogAndIdentifier(
        "path or identifier", spark, path, snapshotId, asOfTimestamp);

    if (catalogAndIdentifier.catalog().name().equals("spark_catalog") &&
        !(catalogAndIdentifier.catalog() instanceof SparkSessionCatalog)) {
      // catalog is a session catalog but does not support Iceberg. Use Iceberg instead.
      return new Spark3Util.CatalogAndIdentifier(catalogManager.catalog(DEFAULT_CATALOG_NAME),
          catalogAndIdentifier.identifier());
    } else {
      return catalogAndIdentifier;
    }
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).identifier();
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).catalog().name();
  }

  private static void setupDefaultSparkCatalog(SparkSession spark) {
    if (spark.conf().contains(DEFAULT_CATALOG)) {
      return;
    }
    ImmutableMap<String, String> config = ImmutableMap.of(
        "type", "hive",
        "default-namespace", "default",
        "cache-enabled", "false" // the source should not use a cache
    );
    String catalogName = "org.apache.iceberg.spark.SparkCatalog";
    spark.conf().set(DEFAULT_CATALOG, catalogName);
    config.forEach((key, value) -> spark.conf().set(DEFAULT_CATALOG + "." + key, value));
  }
}
