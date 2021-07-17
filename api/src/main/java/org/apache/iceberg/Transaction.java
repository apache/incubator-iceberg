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

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * A transaction for performing multiple updates to a table.
 */
public interface Transaction {
  /**
   * Return the {@link Table} that this transaction will update.
   *
   * @return this transaction's table
   */
  Table table();

  /**
   * Create a new {@link UpdateSchema} to alter the columns of this table.
   *
   * @return a new {@link UpdateSchema}
   */
  UpdateSchema updateSchema();

  /**
   * Create a new {@link UpdatePartitionSpec} to alter the partition spec of this table.
   *
   * @return a new {@link UpdatePartitionSpec}
   */
  UpdatePartitionSpec updateSpec();

  /**
   * Create a new {@link UpdateProperties} to update table properties.
   *
   * @return a new {@link UpdateProperties}
   */
  UpdateProperties updateProperties();

  /**
   * Create a new {@link ReplaceSortOrder} to set a table sort order and commit the change.
   *
   * @return a new {@link ReplaceSortOrder}
   */
  ReplaceSortOrder replaceSortOrder();

  /**
   * Create a new {@link UpdateLocation} to update table location.
   *
   * @return a new {@link UpdateLocation}
   */
  UpdateLocation updateLocation();

  /**
   * Create a new {@link AppendFiles append API} to add files to this table.
   *
   * @return a new {@link AppendFiles}
   */
  AppendFiles newAppend();

  /**
   * Create a new {@link AppendFiles append API} to add files to this table.
   * <p>
   * Using this method signals to the underlying implementation that the append should not perform
   * extra work in order to commit quickly. Fast appends are not recommended for normal writes
   * because the fast commit may cause split planning to slow down over time.
   * <p>
   * Implementations may not support fast appends, in which case this will return the same appender
   * as {@link #newAppend()}.
   *
   * @return a new {@link AppendFiles}
   */
  default AppendFiles newFastAppend() {
    return newAppend();
  }

  /**
   * Create a new {@link RewriteFiles rewrite API} to replace files in this table.
   *
   * @return a new {@link RewriteFiles}
   */
  RewriteFiles newRewrite();

  /**
   * Create a new {@link RewriteManifests rewrite manifests API} to replace manifests for this table.
   *
   * @return a new {@link RewriteManifests}
   */
  RewriteManifests rewriteManifests();

  /**
   * Create a new {@link OverwriteFiles overwrite API} to overwrite files by a filter expression.
   *
   * @return a new {@link OverwriteFiles}
   */
  OverwriteFiles newOverwrite();

  /**
   * Create a new {@link RowDelta row-level delta API} to remove or replace rows in existing data files.
   *
   * @return a new {@link RowDelta}
   */
  RowDelta newRowDelta();

  /**
   * Not recommended: Create a new {@link ReplacePartitions replace partitions API} to dynamically
   * overwrite partitions in the table with new data.
   * <p>
   * This is provided to implement SQL compatible with Hive table operations but is not recommended.
   * Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite data.
   *
   * @return a new {@link ReplacePartitions}
   */
  ReplacePartitions newReplacePartitions();

  /**
   * Create a new {@link DeleteFiles delete API} to replace files in this table.
   *
   * @return a new {@link DeleteFiles}
   */
  DeleteFiles newDelete();

  /**
   * Create a new {@link ExpireSnapshots expire API} to manage snapshots in this table.
   *
   * @return a new {@link ExpireSnapshots}
   */
  ExpireSnapshots expireSnapshots();

  /**
   * Apply the pending changes from all actions and commit.
   *
   * <p>If this is a nested transaction within a {@link TransactionalCatalog}, this will only
   * validate that the transaction can still apply. The changes are not completed until the
   * the commit is called on the {@link TransactionalCatalog}.
   *
   * @throws ValidationException If any update cannot be applied to the current table metadata.
   * @throws CommitFailedException If the updates cannot be committed due to conflicts.
   */
  void commitTransaction();
}
