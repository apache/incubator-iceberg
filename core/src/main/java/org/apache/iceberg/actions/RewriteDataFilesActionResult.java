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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class RewriteDataFilesActionResult {

  private static final RewriteDataFilesActionResult EMPTY =
      new RewriteDataFilesActionResult(ImmutableList.of(), ImmutableList.of());

  private List<DataFile> deletedDataFiles;
  private List<DeleteFile> deletedDeleteFiles;

  private List<DataFile> addedDataFiles;
  private List<DeleteFile> addedDeleteFiles;

  public RewriteDataFilesActionResult(List<DataFile> deletedDataFiles, List<DataFile> addedDataFiles) {
    this(deletedDataFiles, ImmutableList.of(), addedDataFiles, ImmutableList.of());
  }

  public RewriteDataFilesActionResult(List<DataFile> deletedDataFiles, List<DeleteFile> deletedDeleteFiles,
                                      List<DataFile> addedDataFiles, List<DeleteFile> addedDeleteFiles) {
    this.deletedDataFiles = deletedDataFiles;
    this.deletedDeleteFiles = deletedDeleteFiles;
    this.addedDataFiles = addedDataFiles;
    this.addedDeleteFiles = addedDeleteFiles;
  }

  static RewriteDataFilesActionResult empty() {
    return EMPTY;
  }

  public List<DataFile> deletedDataFiles() {
    return deletedDataFiles;
  }

  public List<DeleteFile> deletedDeleteFiles() {
    return deletedDeleteFiles;
  }

  public List<DataFile> addedDataFiles() {
    return addedDataFiles;
  }

  public List<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }
}
