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

package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * A mutable class that defines the read position
 * <ul>
 *   <li>file offset in the list of files in a {@link CombinedScanTask}</li>
 *   <li>record offset within a file</li>
 * </ul>
 */
public class Position implements Serializable {

  private static final long serialVersionUID = 1L;

  private long fileOffset;
  private long recordOffset;

  public Position(long fileOffset, long recordOffset) {
    this.fileOffset = fileOffset;
    this.recordOffset = recordOffset;
  }

  void advanceFile() {
    this.fileOffset += 1;
    this.recordOffset = 0L;
  }

  void advanceRecord() {
    this.recordOffset += 1L;
  }

  public void update(long newFileOffset, long newRecordOffset) {
    this.fileOffset = newFileOffset;
    this.recordOffset = newRecordOffset;
  }

  public long fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Position that = (Position) o;
    return Objects.equals(fileOffset, that.fileOffset) &&
        Objects.equals(recordOffset, that.recordOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileOffset, recordOffset);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fileOffset", fileOffset)
        .add("recordOffset", recordOffset)
        .toString();
  }
}
