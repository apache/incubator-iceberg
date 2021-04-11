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

package org.apache.iceberg.actions;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Creates a new Iceberg table based on a source Spark table. The new Iceberg table will
 * have a different data and metadata directory allowing it to exist independently of the
 * source table.
 */
public class Spark3SnapshotAction extends Spark3CreateAction implements SnapshotAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3SnapshotAction.class);

  private String destTableLocation = null;

  public Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog,
                              Identifier sourceTableIdent, CatalogPlugin destCatalog,
                              Identifier destTableIdent) {
    super(spark, sourceCatalog, sourceTableIdent, destCatalog, destTableIdent);
  }

  private Long doExecute() {
    LOG.info("Staging a new Iceberg table {} as a snapshot of {}", destTableIdent(), sourceTableIdent());
    StagedSparkTable stagedTable = stageDestTable();
    Table icebergTable = stagedTable.table();
    // TODO Check table location here against source location

    boolean threw = true;
    try {
      LOG.info("Ensuring {} has a valid name mapping", destTableIdent());
      ensureNameMappingPresent(icebergTable);

      String stagingLocation = getMetadataLocation(icebergTable);
      TableIdentifier v1TableIdentifier = v1SourceTable().identifier();
      LOG.info("Generating Iceberg metadata for {} in {}", destTableIdent(), stagingLocation);
      SparkTableUtil.importSparkTable(spark(), v1TableIdentifier, icebergTable, stagingLocation);

      LOG.info("Committing staged changes to {}", destTableIdent());
      stagedTable.commitStagedChanges();
      threw = false;
    } finally {
      if (threw) {
        LOG.error("Error when attempting to commit snapshot changes, rolling back");

        try {
          stagedTable.abortStagedChanges();
        } catch (Exception abortException) {
          LOG.error("Cannot abort staged changes", abortException);
        }
      }
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long numMigratedFiles = Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files to {}", numMigratedFiles, destTableIdent());
    return numMigratedFiles;
  }

  @Override
  public Long execute() {
    JobGroupInfo info = new JobGroupInfo("SNAPSHOT", "SNAPSHOT", false);
    return withJobGroupInfo(info, this::doExecute);
  }

  @Override
  protected Map<String, String> targetTableProps() {
    Map<String, String> properties = Maps.newHashMap();

    // copy over relevant source table props
    properties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
    EXCLUDED_PROPERTIES.forEach(properties::remove);

    // Remove any possible location properties from origin properties
    properties.remove(LOCATION);
    properties.remove(TableProperties.WRITE_METADATA_LOCATION);
    properties.remove(TableProperties.WRITE_NEW_DATA_LOCATION);

    // set default and user-provided props
    properties.put(TableCatalog.PROP_PROVIDER, "iceberg");
    properties.putAll(additionalProperties());

    // make sure we mark this table as a snapshot table
    properties.put(TableProperties.GC_ENABLED, "false");
    properties.put("snapshot", "true");

    // Don't use the default location for the destination table if an alternate has be set
    if (destTableLocation != null) {
      properties.put(LOCATION, destTableLocation);
    }

    return properties;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // Currently the Import code relies on being able to look up the table in the session code
    Preconditions.checkArgument(catalog.name().equals("spark_catalog"),
        "Cannot snapshot a table that isn't in spark_catalog, the session catalog. Found source catalog %s",
        catalog.name());

    Preconditions.checkArgument(catalog instanceof TableCatalog,
        "Cannot snapshot a table from a non-table catalog %s. Catalog has class of %s.", catalog.name(),
        catalog.getClass().toString());

    return (TableCatalog) catalog;
  }

  @Override
  public SnapshotAction withLocation(String location) {
    String qualifiedLocation = makeQualifiedLocation(location);
    Preconditions.checkArgument(!sourceTableLocation().equals(qualifiedLocation),
        "Cannot create snapshot where destination location is the same as the source location." +
            " This would cause a mixing of original table created and snapshot created files.");
    this.destTableLocation = location;
    return this;
  }

  private String makeQualifiedLocation(String location) {
    try {
      Path path = new Path(location);
      Configuration conf = spark().sessionState().newHadoopConf();
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      return CatalogUtils.URIToString(fs.makeQualified(path).toUri());
    } catch (IOException ioe) {
      LOG.error("Failed to initialize hadoop filesystem.", ioe);
      return location;
    }
  }
}
