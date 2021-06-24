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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceWrapper;

public class SortedPosDeleteWriter<T> implements Closeable {
  private static final long DEFAULT_RECORDS_NUM_THRESHOLD = 100_000L;

  private final Map<CharSequenceWrapper, List<PosRow<T>>> posDeletes = Maps.newHashMap();
  private final List<DeleteFile> completedFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private final CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(null);

  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileFormat format;
  private final StructLike partition;
  private final long recordsNumThreshold;

  private int records = 0;

  SortedPosDeleteWriter(FileAppenderFactory<T> appenderFactory,
                        OutputFileFactory fileFactory,
                        FileFormat format,
                        StructLike partition,
                        long recordsNumThreshold) {
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.format = format;
    this.partition = partition;
    this.recordsNumThreshold = recordsNumThreshold;
  }

  public SortedPosDeleteWriter(FileAppenderFactory<T> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileFormat format,
                               StructLike partition) {
    this(appenderFactory, fileFactory, format, partition, DEFAULT_RECORDS_NUM_THRESHOLD);
  }

  public void delete(CharSequence path, long pos) {
    delete(path, pos, null);
  }

  public void delete(CharSequence path, long pos, T row) {
    List<PosRow<T>> posRows = posDeletes.get(wrapper.set(path));
    if (posRows != null) {
      posRows.add(PosRow.of(pos, row));
    } else {
      posDeletes.put(CharSequenceWrapper.wrap(path), Lists.newArrayList(PosRow.of(pos, row)));
    }

    records += 1;

    // TODO Flush buffer based on the policy that checking whether whole heap memory size exceed the threshold.
    if (records >= recordsNumThreshold) {
      flushDeletes();
    }
  }

  public List<DeleteFile> complete() throws IOException {
    close();

    return completedFiles;
  }

  public CharSequenceSet referencedDataFiles() {
    return referencedDataFiles;
  }

  @Override
  public void close() throws IOException {
    flushDeletes();
  }

  private void flushDeletes() {
    if (posDeletes.isEmpty()) {
      return;
    }

    // Create a new output file.
    EncryptedOutputFile outputFile;
    if (partition == null) {
      outputFile = fileFactory.newOutputFile();
    } else {
      outputFile = fileFactory.newOutputFile(partition);
    }

    PositionDeleteWriter<T> writer = appenderFactory.newPosDeleteWriter(outputFile, format, partition);
    try (PositionDeleteWriter<T> closeableWriter = writer) {
      // Sort all the paths.
      List<CharSequence> paths = Lists.newArrayListWithCapacity(posDeletes.keySet().size());
      for (CharSequenceWrapper charSequenceWrapper : posDeletes.keySet()) {
        paths.add(charSequenceWrapper.get());
      }
      paths.sort(Comparators.charSequences());

      // Write all the sorted <path, pos, row> triples.
      for (CharSequence path : paths) {
        List<PosRow<T>> positions = posDeletes.get(wrapper.set(path));
        positions.sort(Comparator.comparingLong(PosRow::pos));

        positions.forEach(posRow -> closeableWriter.delete(path, posRow.pos(), posRow.row()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write the sorted path/pos pairs to pos-delete file: " +
          outputFile.encryptingOutputFile().location(), e);
    }

    // Clear the buffered pos-deletions.
    posDeletes.clear();
    records = 0;

    // Add the referenced data files.
    referencedDataFiles.addAll(writer.referencedDataFiles());

    // Add the completed delete files.
    completedFiles.add(writer.toDeleteFile());
  }

  private static class PosRow<R> {
    private final long pos;
    private final R row;

    static <R> PosRow<R> of(long pos, R row) {
      return new PosRow<>(pos, row);
    }

    private PosRow(long pos, R row) {
      this.pos = pos;
      this.row = row;
    }

    long pos() {
      return pos;
    }

    R row() {
      return row;
    }
  }
}
