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

import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Similar to the {@link ArrayResultIterator}.
 * Main difference is the records array can be recycled back to a pool.
 */
final class RecyclableArrayIterator<E> implements CloseableIterator<RecordAndPosition<E>> {

  private final Pool.Recycler<E[]> recycler;
  private final E[] records;
  private final int num;
  private final MutableRecordAndPosition<E> recordAndPosition;

  private int pos;

  RecyclableArrayIterator(Pool.Recycler<E[]> recycler) {
    this(recycler, null, 0, CheckpointedPosition.NO_OFFSET, 0L);
  }

  /**
   * Each record's {@link RecordAndPosition} will have the same fileOffset (for {@link RecordAndPosition#getOffset()}.
   * The first returned record will have a records-to-skip count of {@code recordOffset + 1}, following
   * the contract that each record needs to point to the position AFTER itself
   * (because a checkpoint taken after the record was emitted needs to resume from after that record).
   */
  RecyclableArrayIterator(
      Pool.Recycler<E[]> recycler, final E[] newRecords,
      final int newNum, final long fileOffset, final long recordOffset) {
    this.recycler = recycler;
    this.records = newRecords;
    this.num = newNum;
    this.recordAndPosition = new MutableRecordAndPosition<>();
    this.recordAndPosition.set(null, fileOffset, recordOffset);

    this.pos = 0;
  }

  @Override
  public boolean hasNext() {
    return pos < num;
  }

  @Override
  @Nullable
  public RecordAndPosition<E> next() {
    if (pos < num) {
      recordAndPosition.setNext(records[pos++]);
      return recordAndPosition;
    } else {
      return null;
    }
  }

  @Override
  public void close() {
    recycler.recycle(records);
  }
}
