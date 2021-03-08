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

  private void checkFilesToDelete(Set<DataFile> dataFilesToDelete,
                                  Set<DeleteFile> deleteFilesToDelete) {
    int filesToDelete = 0;
    if (dataFilesToDelete != null) {
      filesToDelete += dataFilesToDelete.size();
    }

    if (deleteFilesToDelete != null) {
      filesToDelete += deleteFilesToDelete.size();
    }

    Preconditions.checkArgument(filesToDelete > 0, "Files to delete cannot be null or empty");
  }

  private void checkFilesToAdd(Set<DataFile> dataFilesToAdd,
                               Set<DeleteFile> deleteFilesToAdd) {
    int filesToAdd = 0;
    if (dataFilesToAdd != null) {
      filesToAdd += dataFilesToAdd.size();
    }

    if (deleteFilesToAdd != null) {
      filesToAdd += deleteFilesToAdd.size();
    }

    Preconditions.checkArgument(filesToAdd > 0, "Files to add can not be null or empty");
  }

  @Override
  public RewriteFiles rewriteFiles(Set<DataFile> dataFilesToDelete, Set<DeleteFile> deleteFilesToDelete,
                                   Set<DataFile> dataFilesToAdd, Set<DeleteFile> deleteFilesToAdd) {
    checkFilesToDelete(dataFilesToDelete, deleteFilesToDelete);
    checkFilesToAdd(dataFilesToAdd, deleteFilesToAdd);

    if (dataFilesToDelete != null) {
      for (DataFile dataFile : dataFilesToDelete) {
        delete(dataFile);
      }
    }

    if (deleteFilesToDelete != null) {
      for (DeleteFile deleteFile : deleteFilesToDelete) {
        delete(deleteFile);
      }
    }

    if (dataFilesToAdd != null) {
      for (DataFile dataFile : dataFilesToAdd) {
        add(dataFile);
      }
    }

    if (deleteFilesToAdd != null) {
      for (DeleteFile deleteFile : deleteFilesToAdd) {
        add(deleteFile);
      }
    }

    return this;
  }
}
