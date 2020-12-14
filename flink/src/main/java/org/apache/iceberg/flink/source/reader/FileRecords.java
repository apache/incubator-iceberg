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

package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.io.CloseableIterator;

/**
 * A batch of recrods for one split
 */
class FileRecords<T> implements RecordsWithSplitIds<RecordAndPosition<T>> {

  @Nullable
  private final CloseableIterator<RecordAndPosition<T>> recordsForSplit;
  private final Set<String> finishedSplits;

  @Nullable
  private String splitId;
  @Nullable
  private CloseableIterator<RecordAndPosition<T>> recordsForSplitCurrent;

  private FileRecords(
      @Nullable String splitId,
      @Nullable CloseableIterator<RecordAndPosition<T>> recordsForSplit,
      Set<String> finishedSplits) {

    this.splitId = splitId;
    this.recordsForSplit = recordsForSplit;
    this.finishedSplits = finishedSplits;
  }

  @Nullable
  @Override
  public String nextSplit() {
    // move the split one (from current value to null)
    final String nextSplit = this.splitId;
    this.splitId = null;

    // move the iterator, from null to value (if first move) or to null (if second move)
    this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

    return nextSplit;
  }

  @Nullable
  @Override
  public RecordAndPosition<T> nextRecordFromSplit() {
    if (recordsForSplitCurrent != null) {
      return recordsForSplitCurrent.next();
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public void recycle() {
    if (recordsForSplit != null) {
      try {
        recordsForSplit.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close the record batch");
      }
    }
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  public static <T> FileRecords<T> forRecords(
      final String splitId, final CloseableIterator<RecordAndPosition<T>> recordsForSplit) {
    return new FileRecords<>(splitId, recordsForSplit, Collections.emptySet());
  }

  public static <T> FileRecords<T> finishedSplit(String splitId) {
    return new FileRecords<>(null, null, Collections.singleton(splitId));
  }
}
