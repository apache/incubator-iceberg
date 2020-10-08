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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class BaseTransaction implements Transaction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTransaction.class);

  enum TransactionType {
    CREATE_TABLE,
    REPLACE_TABLE,
    CREATE_OR_REPLACE_TABLE,
    SIMPLE
  }

  private final String tableName;
  private final TableOperations ops;
  private final TransactionTable transactionTable;
  private final TableOperations transactionOps;
  private final List<PendingUpdate> updates;
  private final Set<Long> intermediateSnapshotIds;
  private final Set<String> deletedFiles = Sets.newHashSet(); // keep track of files deleted in the most recent commit
  private final Consumer<String> enqueueDelete = deletedFiles::add;
  private TransactionType type;
  private TableMetadata base;
  private TableMetadata current;
  private boolean hasLastOpCommitted;

  BaseTransaction(String tableName, TableOperations ops, TransactionType type, TableMetadata start) {
    this.tableName = tableName;
    this.ops = ops;
    this.transactionTable = new TransactionTable();
    this.current = start;
    this.transactionOps = new TransactionTableOperations();
    this.updates = Lists.newArrayList();
    this.intermediateSnapshotIds = Sets.newHashSet();
    this.base = ops.current();
    this.type = type;
    this.hasLastOpCommitted = true;
  }

  @Override
  public Table table() {
    return transactionTable;
  }

  private void checkLastOperationCommitted(String operation) {
    Preconditions.checkState(hasLastOpCommitted,
        "Cannot create new %s: last operation has not committed", operation);
    this.hasLastOpCommitted = false;
  }

  @Override
  public UpdateSchema updateSchema() {
    checkLastOperationCommitted("UpdateSchema");
    UpdateSchema schemaChange = new SchemaUpdate(transactionOps);
    updates.add(schemaChange);
    return schemaChange;
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    checkLastOperationCommitted("UpdateSpec");
    UpdatePartitionSpec partitionSpecChange = new BaseUpdatePartitionSpec(transactionOps);
    updates.add(partitionSpecChange);
    return partitionSpecChange;
  }

  @Override
  public UpdateProperties updateProperties() {
    checkLastOperationCommitted("UpdateProperties");
    UpdateProperties props = new PropertiesUpdate(transactionOps);
    updates.add(props);
    return props;
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    checkLastOperationCommitted("ReplaceSortOrder");
    ReplaceSortOrder replaceSortOrder = new BaseReplaceSortOrder(transactionOps);
    updates.add(replaceSortOrder);
    return replaceSortOrder;
  }

  @Override
  public UpdateLocation updateLocation() {
    checkLastOperationCommitted("UpdateLocation");
    UpdateLocation setLocation = new SetLocation(transactionOps);
    updates.add(setLocation);
    return setLocation;
  }

  @Override
  public AppendFiles newAppend() {
    checkLastOperationCommitted("AppendFiles");
    AppendFiles append = new MergeAppend(tableName, transactionOps);
    append.deleteWith(enqueueDelete);
    updates.add(append);
    return append;
  }

  @Override
  public AppendFiles newFastAppend() {
    checkLastOperationCommitted("AppendFiles");
    AppendFiles append = new FastAppend(tableName, transactionOps);
    updates.add(append);
    return append;
  }

  @Override
  public RewriteFiles newRewrite() {
    checkLastOperationCommitted("RewriteFiles");
    RewriteFiles rewrite = new BaseRewriteFiles(tableName, transactionOps);
    rewrite.deleteWith(enqueueDelete);
    updates.add(rewrite);
    return rewrite;
  }

  @Override
  public RewriteManifests rewriteManifests() {
    checkLastOperationCommitted("RewriteManifests");
    RewriteManifests rewrite = new BaseRewriteManifests(transactionOps);
    rewrite.deleteWith(enqueueDelete);
    updates.add(rewrite);
    return rewrite;
  }

  @Override
  public OverwriteFiles newOverwrite() {
    checkLastOperationCommitted("OverwriteFiles");
    OverwriteFiles overwrite = new BaseOverwriteFiles(tableName, transactionOps);
    overwrite.deleteWith(enqueueDelete);
    updates.add(overwrite);
    return overwrite;
  }

  @Override
  public RowDelta newRowDelta() {
    checkLastOperationCommitted("RowDelta");
    RowDelta delta = new BaseRowDelta(tableName, transactionOps);
    delta.deleteWith(enqueueDelete);
    updates.add(delta);
    return delta;
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    checkLastOperationCommitted("ReplacePartitions");
    ReplacePartitions replacePartitions = new BaseReplacePartitions(tableName, transactionOps);
    replacePartitions.deleteWith(enqueueDelete);
    updates.add(replacePartitions);
    return replacePartitions;
  }

  @Override
  public DeleteFiles newDelete() {
    checkLastOperationCommitted("DeleteFiles");
    DeleteFiles delete = new StreamingDelete(tableName, transactionOps);
    delete.deleteWith(enqueueDelete);
    updates.add(delete);
    return delete;
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    checkLastOperationCommitted("ExpireSnapshots");
    ExpireSnapshots expire = new RemoveSnapshots(transactionOps);
    expire.deleteWith(enqueueDelete);
    updates.add(expire);
    return expire;
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(hasLastOpCommitted,
        "Cannot commit transaction: last operation has not committed");

    switch (type) {
      case CREATE_TABLE:
        commitCreateTransaction();
        break;

      case REPLACE_TABLE:
        commitReplaceTransaction(false);
        break;

      case CREATE_OR_REPLACE_TABLE:
        commitReplaceTransaction(true);
        break;

      case SIMPLE:
        commitSimpleTransaction();
        break;
    }
  }

  private void commitCreateTransaction() {
    // fix up the snapshot log, which should not contain intermediate snapshots
    TableMetadata createMetadata = current.removeSnapshotLogEntries(intermediateSnapshotIds);

    // this operation creates the table. if the commit fails, this cannot retry because another
    // process has created the same table.
    try {
      ops.commit(null, createMetadata);

    } catch (RuntimeException e) {
      // the commit failed and no files were committed. clean up each update.
      Tasks.foreach(updates)
          .suppressFailureWhenFinished()
          .run(update -> {
            if (update instanceof SnapshotProducer) {
              ((SnapshotProducer) update).cleanAll();
            }
          });

      throw e;

    } finally {
      // create table never needs to retry because the table has no previous state. because retries are not a
      // concern, it is safe to delete all of the deleted files from individual operations
      Tasks.foreach(deletedFiles)
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.warn("Failed to delete uncommitted file: {}", file, exc))
          .run(ops.io()::deleteFile);
    }
  }

  private void commitReplaceTransaction(boolean orCreate) {
    // fix up the snapshot log, which should not contain intermediate snapshots
    TableMetadata replaceMetadata = current.removeSnapshotLogEntries(intermediateSnapshotIds);
    Map<String, String> props = base != null ? base.properties() : replaceMetadata.properties();

    try {
      Tasks.foreach(ops)
          .retry(PropertyUtil.propertyAsInt(props, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              PropertyUtil.propertyAsInt(props, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(props, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(props, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(underlyingOps -> {

            try {
              underlyingOps.refresh();
            } catch (NoSuchTableException e) {
              if (!orCreate) {
                throw e;
              }
            }

            // because this is a replace table, it will always completely replace the table
            // metadata. even if it was just updated.
            if (base != underlyingOps.current()) {
              this.base = underlyingOps.current(); // just refreshed
            }

            underlyingOps.commit(base, replaceMetadata);
          });

    } catch (RuntimeException e) {
      // the commit failed and no files were committed. clean up each update.
      Tasks.foreach(updates)
          .suppressFailureWhenFinished()
          .run(update -> {
            if (update instanceof SnapshotProducer) {
              ((SnapshotProducer) update).cleanAll();
            }
          });

      throw e;

    } finally {
      // replace table never needs to retry because the table state is completely replaced. because retries are not
      // a concern, it is safe to delete all of the deleted files from individual operations
      Tasks.foreach(deletedFiles)
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.warn("Failed to delete uncommitted file: {}", file, exc))
          .run(ops.io()::deleteFile);
    }
  }

  private void commitSimpleTransaction() {
    // if there were no changes, don't try to commit
    if (base == current) {
      return;
    }

    // this is always set to the latest commit attempt's snapshot id.
    AtomicLong currentSnapshotId = new AtomicLong(-1L);

    try {
      Tasks.foreach(ops)
          .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(underlyingOps -> {
            if (base != underlyingOps.refresh()) {
              this.base = underlyingOps.current(); // just refreshed
              this.current = base;
              for (PendingUpdate update : updates) {
                // re-commit each update in the chain to apply it and update current
                update.commit();
              }
            }

            if (current.currentSnapshot() != null) {
              currentSnapshotId.set(current.currentSnapshot().snapshotId());
            }

            // fix up the snapshot log, which should not contain intermediate snapshots
            underlyingOps.commit(base, current.removeSnapshotLogEntries(intermediateSnapshotIds));
          });

    } catch (RuntimeException e) {
      // the commit failed and no files were committed. clean up each update.
      Tasks.foreach(updates)
          .suppressFailureWhenFinished()
          .run(update -> {
            if (update instanceof SnapshotProducer) {
              ((SnapshotProducer) update).cleanAll();
            }
          });

      // delete all files that were cleaned up
      Tasks.foreach(deletedFiles)
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.warn("Failed to delete uncommitted file: {}", file, exc))
          .run(ops.io()::deleteFile);

      throw e;
    }

    // the commit succeeded

    try {
      if (currentSnapshotId.get() != -1) {
        intermediateSnapshotIds.add(currentSnapshotId.get());
      }

      // clean up the data files that were deleted by each operation. first, get the list of committed manifests to
      // ensure that no committed manifest is deleted. a manifest could be deleted in one successful operation
      // commit, but reused in another successful commit of that operation if the whole transaction is retried.
      Set<String> committedFiles = committedFiles(ops, intermediateSnapshotIds);
      if (committedFiles != null) {
        // delete all of the files that were deleted in the most recent set of operation commits
        Tasks.foreach(deletedFiles)
            .suppressFailureWhenFinished()
            .onFailure((file, exc) -> LOG.warn("Failed to delete uncommitted file: {}", file, exc))
            .run(path -> {
              if (!committedFiles.contains(path)) {
                ops.io().deleteFile(path);
              }
            });
      } else {
        LOG.warn("Failed to load metadata for a committed snapshot, skipping clean-up");
      }

    } catch (RuntimeException e) {
      LOG.warn("Failed to load committed metadata, skipping clean-up", e);
    }
  }

  private static Set<String> committedFiles(TableOperations ops, Set<Long> snapshotIds) {
    if (snapshotIds.isEmpty()) {
      return null;
    }

    Set<String> committedFiles = Sets.newHashSet();

    for (long snapshotId : snapshotIds) {
      Snapshot snap = ops.current().snapshot(snapshotId);
      if (snap != null) {
        committedFiles.add(snap.manifestListLocation());
        snap.allManifests()
            .forEach(manifest -> committedFiles.add(manifest.path()));
      } else {
        return null;
      }
    }

    return committedFiles;
  }

  private static Long currentId(TableMetadata meta) {
    if (meta != null) {
      if (meta.currentSnapshot() != null) {
        return meta.currentSnapshot().snapshotId();
      }
    }
    return null;
  }

  public class TransactionTableOperations implements TableOperations {
    private TableOperations tempOps = ops.temp(current);

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata underlyingBase, TableMetadata metadata) {
      if (underlyingBase != current) {
        // trigger a refresh and retry
        throw new CommitFailedException("Table metadata refresh is required");
      }

      // track the intermediate snapshot ids for rewriting the snapshot log
      // an id is intermediate if it isn't the base snapshot id and it is replaced by a new current
      Long oldId = currentId(current);
      if (oldId != null && !oldId.equals(currentId(metadata)) && !oldId.equals(currentId(base))) {
        intermediateSnapshotIds.add(oldId);
      }

      BaseTransaction.this.current = metadata;

      this.tempOps = ops.temp(metadata);

      BaseTransaction.this.hasLastOpCommitted = true;
    }

    @Override
    public FileIO io() {
      return tempOps.io();
    }

    @Override
    public EncryptionManager encryption() {
      return tempOps.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return tempOps.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return tempOps.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return tempOps.newSnapshotId();
    }
  }

  public class TransactionTable implements Table, HasTableOperations, Serializable {

    @Override
    public TableOperations operations() {
      return transactionOps;
    }

    @Override
    public String name() {
      return tableName;
    }

    @Override
    public void refresh() {
    }

    @Override
    public TableScan newScan() {
      throw new UnsupportedOperationException("Transaction tables do not support scans");
    }

    @Override
    public Schema schema() {
      return current.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
      return current.schemasById();
    }

    @Override
    public PartitionSpec spec() {
      return current.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
      return current.specsById();
    }

    @Override
    public SortOrder sortOrder() {
      return current.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
      return current.sortOrdersById();
    }

    @Override
    public Map<String, String> properties() {
      return current.properties();
    }

    @Override
    public String location() {
      return current.location();
    }

    @Override
    public Snapshot currentSnapshot() {
      return current.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
      return current.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
      return current.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
      return current.snapshotLog();
    }

    @Override
    public UpdateSchema updateSchema() {
      return BaseTransaction.this.updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return BaseTransaction.this.updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return BaseTransaction.this.updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return BaseTransaction.this.replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return BaseTransaction.this.updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return BaseTransaction.this.newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return BaseTransaction.this.newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return BaseTransaction.this.newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return BaseTransaction.this.rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return BaseTransaction.this.newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return BaseTransaction.this.newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return BaseTransaction.this.newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return BaseTransaction.this.newDelete();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return BaseTransaction.this.expireSnapshots();
    }

    @Override
    public Rollback rollback() {
      throw new UnsupportedOperationException("Transaction tables do not support rollback");
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      throw new UnsupportedOperationException("Transaction tables do not support managing snapshots");
    }

    @Override
    public Transaction newTransaction() {
      throw new UnsupportedOperationException("Cannot create a transaction within a transaction");
    }

    @Override
    public FileIO io() {
      return transactionOps.io();
    }

    @Override
    public EncryptionManager encryption() {
      return transactionOps.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
      return transactionOps.locationProvider();
    }

    @Override
    public String toString() {
      return name();
    }

    Object writeReplace() {
      return SerializableTable.copyOf(this);
    }
  }

  @VisibleForTesting
  TableOperations ops() {
    return ops;
  }

  @VisibleForTesting
  Set<String> deletedFiles() {
    return deletedFiles;
  }
}
