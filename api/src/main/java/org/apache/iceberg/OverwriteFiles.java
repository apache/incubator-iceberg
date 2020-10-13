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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;

/**
 * API for overwriting files in a table.
 * <p>
 * This API accumulates file additions and produces a new {@link Snapshot} of the table by replacing
 * all the deleted files with the set of additions. This operation is used to implement idempotent
 * writes that always replace a section of a table with new data or update/delete operations that
 * eagerly overwrite files.
 * <p>
 * Overwrites can be validated. The default validation mode is idempotent, meaning the overwrite is
 * correct and should be committed out regardless of other concurrent changes to the table.
 * For example, this can be used for replacing all the data for day D with query results.
 * Alternatively, this API can be configured for overwriting certain files with their filtered
 * versions while ensuring no new data that would need to be filtered has been added.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface OverwriteFiles extends SnapshotUpdate<OverwriteFiles> {
  /**
   * Delete files that match an {@link Expression} on data rows from the table.
   * <p>
   * A file is selected to be deleted by the expression if it could contain any rows that match the
   * expression (candidate files are selected using an
   * {@link Projections#inclusive(PartitionSpec) inclusive projection}). These candidate files are
   * deleted if all of the rows in the file must match the expression (the partition data matches
   * the expression's {@link Projections#strict(PartitionSpec)} strict projection}). This guarantees
   * that files are deleted if and only if all rows in the file must match the expression.
   * <p>
   * Files that may contain some rows that match the expression and some rows that do not will
   * result in a {@link ValidationException}.
   *
   * @param expr an expression on rows in the table
   * @return this for method chaining
   * @throws ValidationException If a file can contain both rows that match and rows that do not
   */
  OverwriteFiles overwriteByRowFilter(Expression expr);

  /**
   * Add a {@link DataFile} to the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  OverwriteFiles addFile(DataFile file);

  /**
   * Delete a {@link DataFile} from the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  OverwriteFiles deleteFile(DataFile file);

  /**
   * Signal that each file added to the table must match the overwrite expression.
   * <p>
   * If this method is called, each added file is validated on commit to ensure that it matches the
   * overwrite row filter. This is used to ensure that writes are idempotent: that files cannot
   * be added during a commit that would not be removed if the operation were run a second time.
   *
   * @return this for method chaining
   */
  OverwriteFiles validateAddedFilesMatchOverwriteFilter();

  /**
   * Set the snapshot ID used in any reads for this operation.
   * <p>
   * Validations will check changes after this snapshot ID. If the from snapshot is not set, all ancestor snapshots
   * through the table's initial snapshot are validated.
   *
   * @param snapshotId a snapshot ID
   * @return this for method chaining
   */
  OverwriteFiles validateFromSnapshot(long snapshotId);

  /**
   * Enables or disables case sensitive expression binding for validations that accept expressions.
   *
   * @param caseSensitive whether expression binding should be case sensitive
   * @return this for method chaining
   */
  OverwriteFiles caseSensitive(boolean caseSensitive);

  /**
   * Enables validation that files added concurrently do not conflict with this commit's operation.
   * <p>
   * This method should be called when the table is queried to determine which files to delete/append.
   * If a concurrent operation commits a new file after the data was read and that file might
   * contain rows matching the specified conflict detection filter, the overwrite operation
   * will detect this during retries and fail.
   * <p>
   * Calling this method with a correct conflict detection filter is required to maintain
   * serializable isolation for eager update/delete operations. Otherwise, the isolation level
   * will be snapshot isolation.
   * <p>
   * Validation applies to files added to the table since the snapshot passed to {@link #validateFromSnapshot(long)}.
   *
   * @param conflictDetectionFilter an expression on rows in the table
   * @return this for method chaining
   */
  OverwriteFiles validateNoConflictingAppends(Expression conflictDetectionFilter);

  /**
   * Enables validation that files added concurrently do not conflict with this commit's operation.
   * <p>
   * This method should be called when the table is queried to determine which files to delete/append.
   * If a concurrent operation commits a new file after the data was read and that file might
   * contain rows matching the specified conflict detection filter, the overwrite operation
   * will detect this during retries and fail.
   * <p>
   * Calling this method with a correct conflict detection filter is required to maintain
   * serializable isolation for eager update/delete operations. Otherwise, the isolation level
   * will be snapshot isolation.
   *
   * @param readSnapshotId the snapshot id that was used to read the data or null if the table was empty
   * @param conflictDetectionFilter an expression on rows in the table
   * @return this for method chaining
   * @deprecated this will be removed in 0.11.0;
   *             use {@link #validateNoConflictingAppends(Expression)} and {@link #validateFromSnapshot(long)} instead
   */
  @Deprecated
  OverwriteFiles validateNoConflictingAppends(Long readSnapshotId, Expression conflictDetectionFilter);
}
