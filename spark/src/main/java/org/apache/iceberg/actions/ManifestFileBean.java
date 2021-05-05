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
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

public class ManifestFileBean implements ManifestFile {
  private String path = null;
  private Long length = null;
  private Integer partitionSpecId = null;
  private Integer content = null;
  private Long addedSnapshotId = null;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Long getLength() {
    return length;
  }

  public void setLength(Long length) {
    this.length = length;
  }

  public Integer getPartitionSpecId() {
    return partitionSpecId;
  }

  public void setPartitionSpecId(Integer partitionSpecId) {
    this.partitionSpecId = partitionSpecId;
  }

  public Long getAddedSnapshotId() {
    return addedSnapshotId;
  }

  public void setAddedSnapshotId(Long addedSnapshotId) {
    this.addedSnapshotId = addedSnapshotId;
  }

  public Integer getContent() {
    return content;
  }

  public void setContent(Integer content) {
    this.content = content;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public int partitionSpecId() {
    return partitionSpecId;
  }

  @Override
  public ManifestContent content() {
    return (content != null && content == ManifestContent.DELETES.id()) ?
      ManifestContent.DELETES : ManifestContent.DATA;
  }

  @Override
  public long sequenceNumber() {
    return 0;
  }

  @Override
  public long minSequenceNumber() {
    return 0;
  }

  @Override
  public Long snapshotId() {
    return addedSnapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    return null;
  }

  @Override
  public Long addedRowsCount() {
    return null;
  }

  @Override
  public Integer existingFilesCount() {
    return null;
  }

  @Override
  public Long existingRowsCount() {
    return null;
  }

  @Override
  public Integer deletedFilesCount() {
    return null;
  }

  @Override
  public Long deletedRowsCount() {
    return null;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return null;
  }

  @Override
  public ManifestFile copy() {
    throw new UnsupportedOperationException("Cannot copy");
  }
}
