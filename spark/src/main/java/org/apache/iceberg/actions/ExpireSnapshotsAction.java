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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action which performs the same operation as {@link ExpireSnapshots} but uses Spark
 * to determine the delta in files between the pre and post-expiration table metadata. All of the same
 * restrictions of Remove Snapshots also apply to this action.
 * <p>
 * This implementation uses the metadata tables for the table being expired to list all Manifest and DataFiles. This
 * is made into a Dataframe which are anti-joined with the same list read after the expiration. This operation will
 * require a shuffle so parallelism can be controlled through spark.sql.shuffle.partitions. The expiration is done
 * locally using a direct call to RemoveSnapshots. The snapshot expiration will be fully committed before any deletes
 * are issued. Deletes are still performed locally after retrieving the results from the Spark executors.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class ExpireSnapshotsAction extends BaseAction<ExpireSnapshotsActionResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = null;

  private final SparkSession spark;
  private final Table table;
  private final TableOperations ops;
  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private Set<Long> expireSnapshotIdValues = Sets.newHashSet();
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;
  private Dataset<Row> expiredFiles = null;
  private boolean streamResults = false;

  ExpireSnapshotsAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  protected Table table() {
    return table;
  }

  /**
   * By default, all files to delete are brought to the driver at once which may be an issue with very long file lists.
   * Set this to true to use toLocalIterator if you are running into memory issues when collecting
   * the list of files to be deleted.
   * @param stream whether to use toLocalIterator to stream results instead of collect.
   * @return this for method chaining
   */
  public ExpireSnapshotsAction streamDeleteResults(boolean stream) {
    this.streamResults = stream;
    return this;
  }

  /**
   * An executor service used when deleting files. Only used during the local delete phase of this Spark action.
   * Similar to {@link ExpireSnapshots#executeDeleteWith(ExecutorService)}
   * @param executorService the service to use
   * @return this for method chaining
   */
  public ExpireSnapshotsAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  /**
   * A specific snapshot to expire.
   * Identical to {@link ExpireSnapshots#expireSnapshotId(long)}
   * @param expireSnapshotId Id of the snapshot to expire
   * @return this for method chaining
   */
  public ExpireSnapshotsAction expireSnapshotId(long expireSnapshotId) {
    this.expireSnapshotIdValues.add(expireSnapshotId);
    return this;
  }

  /**
   * Expire all snapshots older than a given timestamp.
   * Identical to {@link ExpireSnapshots#expireOlderThan(long)}
   * @param timestampMillis all snapshots before this time will be expired
   * @return this for method chaining
   */
  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  /**
   * Retain at least x snapshots when expiring
   * Identical to {@link ExpireSnapshots#retainLast(int)}
   * @param numSnapshots number of snapshots to leave
   * @return this for method chaining
   */
  public ExpireSnapshotsAction retainLast(int numSnapshots) {
    Preconditions.checkArgument(1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s", numSnapshots);
    this.retainLastValue = numSnapshots;
    return this;
  }

  /**
   * The Consumer used on files which have been determined to be expired. By default uses a filesystem delete.
   * Identical to {@link ExpireSnapshots#deleteWith(Consumer)}
   * @param newDeleteFunc Consumer which takes a path and deletes it
   * @return this for method chaining
   */
  public ExpireSnapshotsAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  /**
   * Expires snapshots and commits the changes to the table, returning a Dataset of files to delete.
   * <p>
   * This does not delete data files. To delete data files, run {@link #execute()}.
   * <p>
   * This may be called before or after {@link #execute()} is called to return the expired file list.
   *
   * @return a Dataset of files that are no longer referenced by the table
   */
  public Dataset<Row> expire() {
    if (expiredFiles == null) {
      // Metadata before Expiration
      Dataset<Row> originalFiles = buildValidFileDF(ops.current());

      // Perform Expiration
      ExpireSnapshots expireSnaps = table.expireSnapshots().cleanExpiredFiles(false);
      for (final Long id : expireSnapshotIdValues) {
        expireSnaps = expireSnaps.expireSnapshotId(id);
      }

      if (expireOlderThanValue != null) {
        expireSnaps = expireSnaps.expireOlderThan(expireOlderThanValue);
      }

      if (retainLastValue != null) {
        expireSnaps = expireSnaps.retainLast(retainLastValue);
      }

      expireSnaps.commit();

      // Metadata after Expiration
      Dataset<Row> validFiles = buildValidFileDF(ops.refresh());

      this.expiredFiles = originalFiles.except(validFiles);
    }

    return expiredFiles;
  }

  @Override
  public ExpireSnapshotsActionResult execute() {
    if (streamResults) {
      return deleteFiles(expire().toLocalIterator());
    } else {
      return deleteFiles(expire().collectAsList().iterator());
    }
  }

  private Dataset<Row> appendTypeString(Dataset<Row> ds, String type) {
    return ds.select(new Column("file_path"), functions.lit(type).as("file_type"));
  }

  private Dataset<Row> buildValidFileDF(TableMetadata metadata) {
    return appendTypeString(buildValidDataFileDF(spark, metadata.metadataFileLocation()), DATA_FILE)
        .union(appendTypeString(buildManifestFileDF(spark, metadata.metadataFileLocation()), MANIFEST))
        .union(appendTypeString(buildManifestListDF(spark, metadata.metadataFileLocation()), MANIFEST_LIST));
  }

  /**
   * Deletes files passed to it based on their type.
   * @param expired an Iterator of Spark Rows of the structure (path: String, type: String)
   * @return Statistics on which files were deleted
   */
  private ExpireSnapshotsActionResult deleteFiles(Iterator<Row> expired) {
    AtomicLong dataFileCount = new AtomicLong(0L);
    AtomicLong manifestCount = new AtomicLong(0L);
    AtomicLong manifestListCount = new AtomicLong(0L);

    Tasks.foreach(expired)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(deleteExecutorService)
        .onFailure((fileInfo, exc) ->
            LOG.warn("Delete failed for {}: {}", fileInfo.getString(1), fileInfo.getString(0), exc))
        .run(fileInfo -> {
          String file = fileInfo.getString(0);
          String type = fileInfo.getString(1);
          deleteFunc.accept(file);
          switch (type) {
            case DATA_FILE:
              dataFileCount.incrementAndGet();
              LOG.trace("Deleted Data File: {}", file);
              break;
            case MANIFEST:
              manifestCount.incrementAndGet();
              LOG.debug("Deleted Manifest: {}", file);
              break;
            case MANIFEST_LIST:
              manifestListCount.incrementAndGet();
              LOG.debug("Deleted Manifest List: {}", file);
              break;
          }
        });
    LOG.info("Deleted {} total files", dataFileCount.get() + manifestCount.get() + manifestListCount.get());
    return new ExpireSnapshotsActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get());
  }
}
