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

@Deprecated
public class ExpireSnapshotsActionResult {

  private final Long dataFilesDeleted;
  private final Long deleteFilesDeleted;
  private final Long manifestFilesDeleted;
  private final Long manifestListsDeleted;

  static ExpireSnapshotsActionResult wrap(ExpireSnapshots.Result result) {
    return new ExpireSnapshotsActionResult(
        result.deletedDataFilesCount(),
        result.deletedDeleteFilesCount(),
        result.deletedManifestsCount(),
        result.deletedManifestListsCount());
  }

  public ExpireSnapshotsActionResult(Long dataFilesDeleted, Long deleteFilesDeleted,
                                     Long manifestFilesDeleted, Long manifestListsDeleted) {
    this.dataFilesDeleted = dataFilesDeleted;
    this.deleteFilesDeleted = deleteFilesDeleted;
    this.manifestFilesDeleted = manifestFilesDeleted;
    this.manifestListsDeleted = manifestListsDeleted;
  }

  public Long dataFilesDeleted() {
    return dataFilesDeleted;
  }

  public Long deleteFiledDeleted() {
    return  deleteFilesDeleted;
  }

  public Long manifestFilesDeleted() {
    return manifestFilesDeleted;
  }

  public Long manifestListsDeleted() {
    return manifestListsDeleted;
  }

}
