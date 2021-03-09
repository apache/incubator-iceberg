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
import org.apache.iceberg.exceptions.ValidationException;

/**
 * API for replacing files in a table.
 * <p>
 * This API accumulates file additions and deletions, produces a new {@link Snapshot} of the
 * changes, and commits that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * If any of the deleted files are no longer in the latest snapshot when reattempting, the commit
 * will throw a {@link ValidationException}.
 */
public interface RewriteFiles extends SnapshotUpdate<RewriteFiles> {
  /**
   * Add a rewrite that replaces one set of files with another set that contains the same data.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null or empty.
   * @param filesToAdd files that will be added, cannot be null or empty.
   * @return this for method chaining
   */
  RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd);

  /**
   * Add a rewrite that replaces one set of deletes with another that contains the same deleted rows.
   *
   * @param deletesToDelete files that will be replaced, cannot be null or empty.
   * @param deletesToAdd files that will be added, cannot be null or empty.
   * @return this for method chaining
   */
  RewriteFiles rewriteDeletes(Set<DeleteFile> deletesToDelete, Set<DeleteFile> deletesToAdd);
}
