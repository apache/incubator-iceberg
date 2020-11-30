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

import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;

/**
 * Takes a Spark table in the sourceCatalog and attempts to transform it into an Iceberg
 * Table in the same location with the same identifier. Once complete the identifier which
 * previously referred to a non-iceberg table will refer to the newly migrated iceberg
 * table.
 */
class Spark3MigrateAction extends Spark3CreateAction {
  private static final Logger LOG = LoggerFactory.getLogger(Spark3MigrateAction.class);
  private static final String BACKUP_SUFFIX = "_BACKUP_";

  Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName) {
    super(spark, sourceCatalog, sourceTableName, sourceCatalog, sourceTableName);
  }

  @Override
  public Long execute() {
    Table icebergTable;

    // Move source table to a new name, halting all modifications and allowing us to stage
    // the creation of a new Iceberg table in its place
    String backupName = sourceTableName().name() + BACKUP_SUFFIX;
    Identifier backupIdentifier = Identifier.of(sourceTableName().namespace(), backupName);
    try {
      destCatalog().renameTable(sourceTableName(), backupIdentifier);
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException("Cannot find table to migrate", e);
    } catch (TableAlreadyExistsException e) {
      throw new IllegalArgumentException("Cannot rename migration source to backup name", e);
    }

    StagedTable stagedTable = null;
    try {
      Map<String, String> newTableProperties = new ImmutableMap.Builder<String, String>()
          .put(TableCatalog.PROP_PROVIDER, "iceberg")
          .putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava())
          .putAll(tableLocationProperties(sourceTableLocation()))
          .putAll(additionalProperties())
          .build();

      stagedTable = destCatalog().stageCreate(Identifier.of(
          destTableName().namespace(),
          destTableName().name()),
          v1SourceTable().schema(),
          sourcePartitionSpec(),
          newTableProperties);

      icebergTable = ((SparkTable) stagedTable).table();

      if (!icebergTable.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
        applyDefaultTableNameMapping(icebergTable);
      }

      String stagingLocation = icebergTable.location() + "/" + ICEBERG_METADATA_FOLDER;

      LOG.info("Beginning migration of {} using metadata location {}", sourceTableName(), stagingLocation);

      Some<String> backupNamespace = Some.apply(backupIdentifier.namespace()[0]);
      TableIdentifier v1BackupIdentifier = new TableIdentifier(backupIdentifier.name(), backupNamespace);
      SparkTableUtil.importSparkTable(spark(), v1BackupIdentifier, icebergTable, stagingLocation);

      stagedTable.commitStagedChanges();
    } catch (Exception e) {
      LOG.error("Error when attempting perform migration changes, aborting table creation and restoring backup", e);

      try {
        if (stagedTable != null) {
          stagedTable.abortStagedChanges();
        }
      } catch (Exception abortException) {
        LOG.error("Unable to abort staged changes", abortException);
      }

      try {
        destCatalog().renameTable(backupIdentifier, sourceTableName());
      } catch (NoSuchTableException nstException) {
        throw new IllegalArgumentException("Cannot restore backup, the backup cannot be found", nstException);
      } catch (TableAlreadyExistsException taeException) {
        throw new IllegalArgumentException(String.format("Cannot restore backup, a table with the original name " +
            "exists. The backup can be found with the name %s", backupIdentifier.toString()), taeException);
      }

      throw new RuntimeException(e);
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long numMigratedFiles = Long.valueOf(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files", numMigratedFiles);
    return numMigratedFiles;
  }
}