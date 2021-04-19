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

package org.apache.iceberg.io;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class RewriteResult implements Serializable {
  // Files to delete.
  private final DataFile[] dataFilesToDelete;
  private final DeleteFile[] deleteFilesToDelete;

  // Files to add.
  private final DataFile[] dataFilesToAdd;
  private final DeleteFile[] deleteFilesToAdd;

  private RewriteResult(Set<DataFile> dataFilesToDelete,
                        Set<DeleteFile> deleteFilesToDelete,
                        Set<DataFile> dataFilesToAdd,
                        Set<DeleteFile> deleteFilesToAdd) {
    this.dataFilesToDelete = dataFilesToDelete.toArray(new DataFile[0]);
    this.deleteFilesToDelete = deleteFilesToDelete.toArray(new DeleteFile[0]);
    this.dataFilesToAdd = dataFilesToAdd.toArray(new DataFile[0]);
    this.deleteFilesToAdd = deleteFilesToAdd.toArray(new DeleteFile[0]);
  }

  public DataFile[] dataFilesToDelete() {
    return dataFilesToDelete;
  }

  public DeleteFile[] deleteFilesToDelete() {
    return deleteFilesToDelete;
  }

  public DataFile[] dataFilesToAdd() {
    return dataFilesToAdd;
  }

  public DeleteFile[] deleteFilesToAdd() {
    return deleteFilesToAdd;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Set<DataFile> dataFilesToDelete = Sets.newHashSet();
    private final Set<DeleteFile> deleteFilesToDelete = Sets.newHashSet();
    private final Set<DataFile> dataFilesToAdd = Sets.newHashSet();
    private final Set<DeleteFile> deleteFilesToAdd = Sets.newHashSet();

    public Builder addDataFilesToDelete(DataFile... dataFiles) {
      Collections.addAll(dataFilesToDelete, dataFiles);
      return this;
    }

    public Builder addDataFilesToDelete(Iterable<DataFile> dataFiles) {
      Iterables.addAll(dataFilesToDelete, dataFiles);
      return this;
    }

    public Builder addDeleteFilesToDelete(DeleteFile... deleteFiles) {
      Collections.addAll(deleteFilesToDelete, deleteFiles);
      return this;
    }

    public Builder addDeleteFilesToDelete(Iterable<DeleteFile> deleteFiles) {
      Iterables.addAll(deleteFilesToDelete, deleteFiles);
      return this;
    }

    public Builder addDataFilesToAdd(DataFile... dataFiles) {
      Collections.addAll(dataFilesToAdd, dataFiles);
      return this;
    }

    public Builder addDataFilesToAdd(Iterable<DataFile> dataFiles) {
      Iterables.addAll(dataFilesToAdd, dataFiles);
      return this;
    }

    public Builder addDeleteFilesToAdd(Iterable<DeleteFile> deleteFiles) {
      Iterables.addAll(deleteFilesToAdd, deleteFiles);
      return this;
    }

    public Builder addDeleteFilesToAdd(DeleteFile... deleteFiles) {
      Collections.addAll(deleteFilesToAdd, deleteFiles);
      return this;
    }

    public Builder merge(Iterable<RewriteResult> results) {
      for (RewriteResult result : results) {
        Collections.addAll(dataFilesToDelete, result.dataFilesToDelete);
        Collections.addAll(deleteFilesToDelete, result.deleteFilesToDelete);
        Collections.addAll(dataFilesToAdd, result.dataFilesToAdd);
        Collections.addAll(deleteFilesToAdd, result.deleteFilesToAdd);
      }
      return this;
    }

    public Builder merge(RewriteResult... results) {
      for (RewriteResult result : results) {
        Collections.addAll(dataFilesToDelete, result.dataFilesToDelete);
        Collections.addAll(deleteFilesToDelete, result.deleteFilesToDelete);
        Collections.addAll(dataFilesToAdd, result.dataFilesToAdd);
        Collections.addAll(deleteFilesToAdd, result.deleteFilesToAdd);
      }
      return this;
    }

    public RewriteResult build() {
      return new RewriteResult(dataFilesToDelete, deleteFilesToDelete, dataFilesToAdd, deleteFilesToAdd);
    }
  }
}
