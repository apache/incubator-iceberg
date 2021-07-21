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
import java.util.ArrayDeque;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergSourceSplitReader<T> implements SplitReader<RecordAndPosition<T>, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final ReaderFunction<T> readerFunction;
  private final Queue<IcebergSourceSplit> splits;

  @Nullable
  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> currentReader;
  @Nullable
  private String currentSplitId;

  IcebergSourceSplitReader(ReaderFunction<T> readerFunction) {
    this.readerFunction = readerFunction;
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws IOException {
    checkSplitOrStartNext();
    if (currentReader.hasNext()) {
      // Because Iterator#next() doesn't support checked exception,
      // we need to wrap and unwrap the checked IOException with UncheckedIOException
      try {
        return currentReader.next();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    } else {
      return finishSplit();
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<IcebergSourceSplit> splitsChanges) {
    LOG.debug("Add splits to reader: {}", splitsChanges.splits());
    splits.addAll(splitsChanges.splits());
  }

  @Override
  public void wakeUp() {
  }

  @Override
  public void close() throws Exception {
    currentSplitId = null;
    if (currentReader != null) {
      currentReader.close();
    }
  }

  private void checkSplitOrStartNext() throws IOException {
    if (currentReader != null) {
      return;
    }
    final IcebergSourceSplit nextSplit = splits.poll();
    if (nextSplit == null) {
      throw new IOException("No split remaining");
    }
    currentSplitId = nextSplit.splitId();
    currentReader = readerFunction.apply(nextSplit);
  }

  private FileRecords<T> finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }
    final FileRecords<T> finishRecords = FileRecords.finishedSplit(currentSplitId);
    currentSplitId = null;
    return finishRecords;
  }
}
