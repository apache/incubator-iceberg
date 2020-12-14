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
import java.io.UncheckedIOException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.Position;
import org.apache.iceberg.io.CloseableIterator;

class ArrayPoolDataIteratorBatcher<T> implements DataIteratorBatcher<T> {

  private final Configuration config;
  private final RecordFactory<T> recordFactory;

  ArrayPoolDataIteratorBatcher(Configuration config, RecordFactory<T> recordFactory) {
    this.config = config;
    this.recordFactory = recordFactory;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> apply(
      String splitId, DataIterator<T> inputIterator) {
    return new ArrayPoolBatchIterator(splitId, inputIterator);
  }

  private class ArrayPoolBatchIterator implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> {

    private final String splitId;
    private final DataIterator<T> inputIterator;
    private final int batchSize;
    private final Pool<T[]> pool;

    ArrayPoolBatchIterator(String splitId, DataIterator<T> inputIterator) {
      this.splitId = splitId;
      this.inputIterator = inputIterator;
      this.batchSize = config.getInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_SIZE);
      this.pool = createPoolOfBatches(config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY));
    }

    @Override
    public boolean hasNext() {
      return inputIterator.hasNext();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<T>> next() {
      final T[] batch = getCachedEntry();
      int num = 0;
      while (inputIterator.hasNext() && num < batchSize) {
        T nextRecord = inputIterator.next();
        recordFactory.clone(nextRecord, batch[num]);
        num++;
        if (inputIterator.isCurrentIteratorDone()) {
          // break early so that records in the ArrayResultIterator
          // have the same fileOffset.
          break;
        }
      }
      if (num == 0) {
        return null;
      } else {
        Position position = inputIterator.position();
        return FileRecords.forRecords(splitId, new RecyclableArrayIterator<>(
            pool.recycler(), batch, num, position.fileOffset(), position.recordOffset() - num));
      }
    }

    @Override
    public void close() throws IOException {
      if (inputIterator != null) {
        inputIterator.close();
      }
    }

    private Pool<T[]> createPoolOfBatches(int numBatches) {
      final Pool<T[]> poolOfBatches = new Pool<>(numBatches);
      for (int batchId = 0; batchId < numBatches; batchId++) {
        T[] batch = recordFactory.createBatch(batchSize);
        poolOfBatches.add(batch);
      }
      return poolOfBatches;
    }

    private T[] getCachedEntry() {
      try {
        return pool.pollEntry();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedIOException(new IOException("Interrupted"));
      }
    }
  }
}
