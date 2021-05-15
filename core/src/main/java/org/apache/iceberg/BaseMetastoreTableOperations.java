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

package org.apache.iceberg;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;

public abstract class BaseMetastoreTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreTableOperations.class);

  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
  public static final String METADATA_LOCATION_PROP = "metadata_location";
  public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";

  private static final String METADATA_FOLDER_NAME = "metadata";

  private static final int COMMIT_STATUS_CHECK_WAIT_MS = 1000;

  private TableMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  protected BaseMetastoreTableOperations() {
  }

  /**
   * The full name of the table used for logging purposes only. For example for HiveTableOperations it is
   * catalogName + "." + database + "." + table.
   * @return The full name
   */
  protected String tableName() {
    return null;
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  public String currentMetadataLocation() {
    return currentMetadataLocation;
  }

  public int currentVersion() {
    return version;
  }

  @Override
  public TableMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      doRefresh();
    } catch (NoSuchTableException e) {
      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
        shouldRefresh = true;
      }

      currentMetadata = null;
      currentMetadataLocation = null;
      version = -1;
      throw e;
    }
    return current();
  }

  protected void doRefresh() {
    throw new UnsupportedOperationException("Not implemented: doRefresh");
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Cannot commit: stale table metadata");
    }
    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();
    doCommit(base, metadata);
    deleteRemovedMetadataFiles(base, metadata);
    requestRefresh();

    LOG.info("Successfully committed to table {} in {} ms",
        tableName(),
        System.currentTimeMillis() - start);
  }

  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Not implemented: doCommit");
  }

  protected void requestRefresh() {
    this.shouldRefresh = true;
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    TableMetadataParser.overwrite(metadata, newMetadataLocation);

    return newMetadataLocation.location();
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    refreshFromMetadataLocation(newLocation, null, 20);
  }

  protected void refreshFromMetadataLocation(String newLocation, int numRetries) {
    refreshFromMetadataLocation(newLocation, null, numRetries);
  }

  protected void refreshFromMetadataLocation(String newLocation, Predicate<Exception> shouldRetry,
                                             int numRetries) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Refreshing table metadata from new version: {}", newLocation);

      AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newLocation)
          .retry(numRetries).exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .throwFailureWhenFinished()
          .shouldRetryTest(shouldRetry)
          .run(metadataLocation -> newMetadata.set(
              TableMetadataParser.read(io(), encryption(), metadataLocation)));

      String newUUID = newMetadata.get().uuid();
      if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
        Preconditions.checkState(newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
      }

      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newLocation;
      this.version = parseVersion(newLocation);
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties()
        .get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", metadataLocation, filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException("Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return BaseMetastoreTableOperations.this.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return BaseMetastoreTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return BaseMetastoreTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return BaseMetastoreTableOperations.this.newSnapshotId();
      }
    };
  }

  protected enum CommitStatus {
    FAILURE,
    SUCCESS,
    UNKNOWN
  }

  /**
   * Attempt to load the table and see if any current or past metadata location matches the one we were attempting
   * to set. This is used as a last resort when we are dealing with exceptions that may indicate the commit has
   * failed but are not proof that this is the case. Past locations must also be searched on the chance that a second
   * committer was able to successfully commit on top of our commit.
   *
   * @param newMetadataLocation the path of the new commit file
   * @param config metadata to use for configuration
   * @return Commit Status of Success, Failure or Unknown
   */
  protected CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata config) {
    int maxAttempts = PropertyUtil.propertyAsInt(config.properties(), COMMIT_NUM_STATUS_CHECKS,
        COMMIT_NUM_STATUS_CHECKS_DEFAULT);

    AtomicReference<CommitStatus> status = new AtomicReference<>(CommitStatus.UNKNOWN);

    Tasks.foreach(newMetadataLocation)
        .retry(maxAttempts)
        .suppressFailureWhenFinished()
        .exponentialBackoff(COMMIT_STATUS_CHECK_WAIT_MS, COMMIT_STATUS_CHECK_WAIT_MS, Long.MAX_VALUE, 2.0)
        .onFailure((location, checkException) ->
            LOG.error("Cannot check if commit to {} exists.", tableName(), checkException))
        .run(location -> {
          TableMetadata metadata = refresh();
          String currentMetadataFileLocation = metadata.metadataFileLocation();
          boolean commitSuccess = currentMetadataFileLocation.equals(newMetadataLocation) ||
              metadata.previousFiles().stream().anyMatch(log -> log.file().equals(newMetadataLocation));
          if (commitSuccess) {
            LOG.info("Commit status check: Commit to {} of {} succeeded", tableName(), newMetadataLocation);
            status.set(CommitStatus.SUCCESS);
          } else {
            LOG.info("Commit status check: Commit to {} of {} failed", tableName(), newMetadataLocation);
            status.set(CommitStatus.FAILURE);
          }
        });

    if (status.get() == CommitStatus.UNKNOWN) {
      LOG.error("Cannot determine commit state to {}. Failed during checking {} times. " +
              "Treating commit state as unknown.", tableName(), maxAttempts);
    }
    return status.get();
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName = meta.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    try {
      return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  /**
   * Deletes the oldest metadata files if {@link TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base     table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit = metadata.propertyAsBoolean(
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadataFiles.removeAll(metadata.previousFiles());

    if (deleteAfterCommit) {
      Tasks.foreach(removedPreviousMetadataFiles)
          .noRetry().suppressFailureWhenFinished()
          .onFailure((previousMetadataFile, exc) ->
              LOG.warn("Delete failed for previous metadata file: {}", previousMetadataFile, exc))
          .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }
}
