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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {
  BaseRewriteFiles(String tableName, TableOperations ops) {
    super(tableName, ops);

    // replace files must fail if any of the deleted paths is missing and cannot be deleted
    failMissingDeletePaths();
  }

  @Override
  protected RewriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    Preconditions.checkArgument(filesToDelete != null && !filesToDelete.isEmpty(),
        "Files to delete cannot be null or empty");
    Preconditions.checkArgument(filesToAdd != null && !filesToAdd.isEmpty(),
        "Files to add can not be null or empty");

    for (DataFile toDelete : filesToDelete) {
      delete(toDelete);
    }

    for (DataFile toAdd : filesToAdd) {
      add(toAdd);
    }

    return this;
  }

  @Override
  public RewriteFiles rewriteDeletes(Set<DeleteFile> deletesToDelete, Set<DeleteFile> deletesToAdd) {
    Preconditions.checkArgument(deletesToDelete != null && !deletesToDelete.isEmpty(),
        "Files to delete cannot be null or empty");
    Preconditions.checkArgument(deletesToAdd != null && !deletesToAdd.isEmpty(),
        "Files to add can not be null or empty");

    for (DeleteFile toDelete : deletesToDelete) {
      delete(toDelete);
    }

    for (DeleteFile toAdd : deletesToAdd) {
      add(toAdd);
    }

    return this;
  }
}
