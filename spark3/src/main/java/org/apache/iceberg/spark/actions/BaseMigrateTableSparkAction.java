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

package org.apache.iceberg.spark.actions;

import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseMigrateTableActionResult;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;

/**
 * Takes a Spark table in the source catalog and attempts to transform it into an Iceberg
 * table in the same location with the same identifier. Once complete the identifier which
 * previously referred to a non-Iceberg table will refer to the newly migrated Iceberg
 * table.
 */
public class BaseMigrateTableSparkAction
    extends BaseTableCreationSparkAction<MigrateTable, MigrateTable.Result>
    implements MigrateTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseMigrateTableSparkAction.class);
  private static final String BACKUP_SUFFIX = "_BACKUP_";

  private final StagingTableCatalog destCatalog;
  private final Identifier destTableIdent;
  private final Identifier backupIdent;

  public BaseMigrateTableSparkAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
    super(spark, sourceCatalog, sourceTableIdent);
    this.destCatalog = checkDestinationCatalog(sourceCatalog);
    this.destTableIdent = sourceTableIdent;
    String backupName = sourceTableIdent.name() + BACKUP_SUFFIX;
    this.backupIdent = Identifier.of(sourceTableIdent.namespace(), backupName);
  }

  @Override
  protected MigrateTable self() {
    return this;
  }

  @Override
  protected StagingTableCatalog destCatalog() {
    return destCatalog;
  }

  @Override
  protected Identifier destTableIdent() {
    return destTableIdent;
  }

  @Override
  public MigrateTable tableProperties(Map<String, String> properties) {
    setProperties(properties);
    return this;
  }

  @Override
  public MigrateTable tableProperty(String property, String value) {
    setProperty(property, value);
    return this;
  }

  @Override
  public MigrateTable.Result execute() {
    String desc = String.format("Migrating table %s", destTableIdent().toString());
    JobGroupInfo info = newJobGroupInfo("MIGRATE-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private MigrateTable.Result doExecute() {
    LOG.info("Starting the migration of {} to Iceberg", sourceTableIdent());

    // move the source table to a new name, halting all modifications and allowing us to stage
    // the creation of a new Iceberg table in its place
    renameAndBackupSourceTable();

    StagedSparkTable stagedTable = null;
    Table icebergTable;
    boolean threw = true;
    try {
      LOG.info("Staging a new Iceberg table {}", destTableIdent());
      stagedTable = stageDestTable();
      icebergTable = stagedTable.table();

      LOG.info("Ensuring {} has a valid name mapping", destTableIdent());
      ensureNameMappingPresent(icebergTable);

      Some<String> backupNamespace = Some.apply(backupIdent.namespace()[0]);
      TableIdentifier v1BackupIdent = new TableIdentifier(backupIdent.name(), backupNamespace);
      String stagingLocation = getMetadataLocation(icebergTable);
      LOG.info("Generating Iceberg metadata for {} in {}", destTableIdent(), stagingLocation);
      SparkTableUtil.importSparkTable(spark(), v1BackupIdent, icebergTable, stagingLocation);

      LOG.info("Committing staged changes to {}", destTableIdent());
      stagedTable.commitStagedChanges();
      threw = false;
    } finally {
      if (threw) {
        LOG.error("Failed to perform the migration, aborting table creation and restoring the original table");

        restoreSourceTable();

        if (stagedTable != null) {
          try {
            stagedTable.abortStagedChanges();
          } catch (Exception abortException) {
            LOG.error("Cannot abort staged changes", abortException);
          }
        }
      }
    }

    Snapshot snapshot = icebergTable.currentSnapshot();
    long migratedDataFilesCount = Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files to {}", migratedDataFilesCount, destTableIdent());
    return new BaseMigrateTableActionResult(migratedDataFilesCount);
  }

  @Override
  protected Map<String, String> destTableProps() {
    Map<String, String> properties = Maps.newHashMap();

    // copy over relevant source table props
    properties.putAll(JavaConverters.mapAsJavaMapConverter(v1SourceTable().properties()).asJava());
    EXCLUDED_PROPERTIES.forEach(properties::remove);

    // set default and user-provided props
    properties.put(TableCatalog.PROP_PROVIDER, "iceberg");
    properties.putAll(additionalProperties());

    // make sure we mark this table as migrated
    properties.put("migrated", "true");

    // inherit the source table location
    properties.putIfAbsent(LOCATION, sourceTableLocation());

    return properties;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(catalog instanceof SparkSessionCatalog,
        "Cannot migrate a table from a non-Iceberg Spark Session Catalog. Found %s of class %s as the source catalog.",
        catalog.name(), catalog.getClass().getName());

    return (TableCatalog) catalog;
  }

  private void renameAndBackupSourceTable() {
    try {
      LOG.info("Renaming {} as {} for backup", sourceTableIdent(), backupIdent);
      destCatalog().renameTable(sourceTableIdent(), backupIdent);

    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new NoSuchTableException("Cannot find source table %s", sourceTableIdent());

    } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
      throw new AlreadyExistsException(
          "Cannot rename %s as %s for backup. The backup table already exists.",
          sourceTableIdent(), backupIdent);
    }
  }

  private void restoreSourceTable() {
    try {
      LOG.info("Restoring {} from {}", sourceTableIdent(), backupIdent);
      destCatalog().renameTable(backupIdent, sourceTableIdent());

    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      LOG.error("Cannot restore the original table, the backup table {} cannot be found", backupIdent, e);

    } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
      LOG.error("Cannot restore the original table, a table with the original name exists. " +
          "Use the backup table {} to restore the original table manually.", backupIdent, e);
    }
  }
}
