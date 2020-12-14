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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Base class of Flink iterators.
 *
 * @param <T> is the Java class returned by this iterator whose objects contain one or more rows.
 */
public abstract class DataIterator<T> implements CloseableIterator<T> {

  private final CombinedScanTask combinedTask;
  private final Map<String, InputFile> inputFiles;

  private Iterator<FileScanTask> tasks;
  private CloseableIterator<T> currentIterator;
  private Position position;

  DataIterator(CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption) {
    this.combinedTask = combinedTask;
    Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();
    combinedTask.files().stream()
        .flatMap(fileScanTask -> Stream.concat(Stream.of(fileScanTask.file()), fileScanTask.deletes().stream()))
        .forEach(file -> keyMetadata.put(file.path().toString(), file.keyMetadata()));
    Stream<EncryptedInputFile> encrypted = keyMetadata.entrySet().stream()
        .map(entry -> EncryptedFiles.encryptedInput(io.newInputFile(entry.getKey()), entry.getValue()));

    // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
    Iterable<InputFile> decryptedFiles = encryption.decrypt(encrypted::iterator);

    Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(combinedTask.files().size());
    decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
    this.inputFiles = ImmutableMap.copyOf(files);

    this.tasks = combinedTask.files().iterator();
    this.currentIterator = CloseableIterator.empty();
    // fileOffset starts at -1 because we started
    // from an empty iterator that is not from the split files.
    this.position = new Position(-1L, 0L);
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");

    return inputFiles.get(task.file().path().toString());
  }

  InputFile getInputFile(String location) {
    return inputFiles.get(location);
  }

  public void seek(Position startingPosition) {
    // skip files
    Preconditions.checkArgument(startingPosition.fileOffset() < combinedTask.files().size(),
        "Checkpointed file offset is %d, while CombinedScanTask has %d files",
        startingPosition.fileOffset(), combinedTask.files().size());
    for (long i = 0L; i < startingPosition.fileOffset(); ++i) {
      tasks.next();
    }
    updateCurrentIterator();
    // skip records within the file
    for (long i = 0; i < startingPosition.recordOffset(); ++i) {
      if (hasNext()) {
        next();
      } else {
        throw new IllegalStateException("Not enough records to skip: " +
            startingPosition.recordOffset());
      }
    }
    this.position.update(startingPosition.fileOffset(), startingPosition.recordOffset());
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    position.advanceRecord();
    return currentIterator.next();
  }

  public boolean isCurrentIteratorDone() {
    return !currentIterator.hasNext();
  }

  /**
   * Updates the current iterator field to ensure that the current Iterator
   * is not exhausted.
   */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(tasks.next());
        position.advanceFile();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  abstract CloseableIterator<T> openTaskIterator(FileScanTask scanTask) throws IOException;

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
    tasks = null;
  }

  public Position position() {
    return position;
  }
}
