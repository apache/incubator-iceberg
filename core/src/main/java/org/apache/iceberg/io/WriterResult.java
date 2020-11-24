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
import java.util.List;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class WriterResult implements Serializable {
  private static final DataFile[] EMPTY_DATA_FILES = new DataFile[0];
  private static final DeleteFile[] EMPTY_DELETE_FILES = new DeleteFile[0];

  private DataFile[] dataFiles;
  private DeleteFile[] deleteFiles;

  private WriterResult(DataFile[] dataFiles, DeleteFile[] deleteFiles) {
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
  }

  private WriterResult(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
  }

  public DataFile[] dataFiles() {
    return dataFiles;
  }

  public DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  public static WriterResult create(DataFile dataFile) {
    return new WriterResult(new DataFile[] {dataFile}, EMPTY_DELETE_FILES);
  }

  public static WriterResult create(DeleteFile deleteFile) {
    return new WriterResult(EMPTY_DATA_FILES, new DeleteFile[] {deleteFile});
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;

    private Builder() {
      this.dataFiles = Lists.newArrayList();
      this.deleteFiles = Lists.newArrayList();
    }

    public Builder add(WriterResult result) {
      for (DataFile dataFile : result.dataFiles()) {
        add(dataFile);
      }
      for (DeleteFile deleteFile : result.deleteFiles()) {
        add(deleteFile);
      }

      return this;
    }

    public Builder addDataFiles(Iterable<DataFile> iterable) {
      for (DataFile dataFile : iterable) {
        add(dataFile);
      }

      return this;
    }

    public Builder addDeleteFiles(Iterable<DeleteFile> iterable) {
      for (DeleteFile deleteFile : iterable) {
        add(deleteFile);
      }

      return this;
    }

    public void add(ContentFile<?> contentFile) {
      Preconditions.checkNotNull(contentFile, "Content file shouldn't be null.");
      switch (contentFile.content()) {
        case DATA:
          this.dataFiles.add((DataFile) contentFile);
          break;

        case EQUALITY_DELETES:
        case POSITION_DELETES:
          this.deleteFiles.add((DeleteFile) contentFile);
          break;

        default:
          throw new UnsupportedOperationException("Unknown content file: " + contentFile.content());
      }
    }

    public WriterResult build() {
      return new WriterResult(dataFiles, deleteFiles);
    }
  }
}
