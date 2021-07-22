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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeMapUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private final List<DataFile> completedDataFiles = Lists.newArrayList();
  private final List<DeleteFile> completedDeleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Map<String, String> properties;

  protected BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                           Map<String, String> properties) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.properties = properties;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(Iterables.concat(completedDataFiles, completedDeleteFiles))
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();

    return WriteResult.builder()
        .addDataFiles(completedDataFiles)
        .addDeleteFiles(completedDeleteFiles)
        .addReferencedDataFiles(referencedDataFiles)
        .build();
  }

  /**
   * Base equality delta writer to write both insert records and equality-deletes.
   */
  protected abstract class BaseEqualityDeltaWriter implements Closeable {
    private final StructProjection structProjection;
    private RollingFileWriter dataWriter;
    private RollingEqDeleteWriter eqDeleteWriter;
    private SortedPosDeleteWriter<T> posDeleteWriter;
    private Map<StructLike, StructLike> insertedRowMap;

    protected BaseEqualityDeltaWriter(StructLike partition, Schema schema, Schema deleteSchema) {
      Preconditions.checkNotNull(schema, "Iceberg table schema cannot be null.");
      Preconditions.checkNotNull(deleteSchema, "Equality-delete schema cannot be null.");
      this.structProjection = StructProjection.create(schema, deleteSchema);

      this.dataWriter = new RollingFileWriter(partition);
      this.eqDeleteWriter = new RollingEqDeleteWriter(partition);
      this.posDeleteWriter = new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, partition);

      this.insertedRowMap = StructLikeMapUtil.load(deleteSchema.asStruct(), PATH_OFFSET_SCHEMA.asStruct(), properties);
    }

    /**
     * Wrap the data as a {@link StructLike}.
     */
    protected abstract StructLike asStructLike(T data);

    public void write(T row) throws IOException {
      PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());

      // Create a copied key from this row.
      StructLike copiedKey = StructCopy.copy(structProjection.wrap(asStructLike(row)));

      // Adding a pos-delete to replace the old path-offset.
      StructLike previous = insertedRowMap.put(copiedKey, pathOffset);
      if (previous != null) {
        writePosDelete(previous, null);
      }

      dataWriter.write(row);
    }

    private void writePosDelete(StructLike pathOffset, T row) {
      Preconditions.checkNotNull(pathOffset, "StructLike pathOffset cannot be null.");
      CharSequence path = pathOffset.get(0, CharSequence.class);
      long offset = pathOffset.get(1, Long.class);
      posDeleteWriter.delete(path, offset, row);
    }

    /**
     * Write the pos-delete if there's an existing row matching the given key.
     *
     * @param key has the same columns with the equality fields.
     */
    private void internalPosDelete(StructLike key) {
      StructLike previous = insertedRowMap.remove(key);

      if (previous != null) {
        writePosDelete(previous, null);
      }
    }

    /**
     * Delete those rows whose equality fields has the same values with the given row. It will write the entire row into
     * the equality-delete file.
     *
     * @param row the given row to delete.
     */
    public void delete(T row) throws IOException {
      internalPosDelete(structProjection.wrap(asStructLike(row)));

      eqDeleteWriter.write(row);
    }

    /**
     * Delete those rows with the given key. It will only write the values of equality fields into the equality-delete
     * file.
     *
     * @param key is the projected data whose columns are the same as the equality fields.
     */
    public void deleteKey(T key) throws IOException {
      internalPosDelete(asStructLike(key));

      eqDeleteWriter.write(key);
    }

    @Override
    public void close() throws IOException {
      // Close data writer and add completed data files.
      if (dataWriter != null) {
        dataWriter.close();
        dataWriter = null;
      }

      // Close eq-delete writer and add completed equality-delete files.
      if (eqDeleteWriter != null) {
        eqDeleteWriter.close();
        eqDeleteWriter = null;
      }

      if (insertedRowMap != null) {
        insertedRowMap.clear();
        insertedRowMap = null;
      }

      // Add the completed pos-delete files.
      if (posDeleteWriter != null) {
        completedDeleteFiles.addAll(posDeleteWriter.complete());
        referencedDataFiles.addAll(posDeleteWriter.referencedDataFiles());
        posDeleteWriter = null;
      }
    }
  }

  private static final Schema PATH_OFFSET_SCHEMA = new Schema(
      Types.NestedField.required(0, "path", Types.StringType.get()),
      Types.NestedField.required(1, "row_offset", Types.LongType.get())
  );

  private static class PathOffset implements StructLike {
    private CharSequence path;
    private long rowOffset;

    private PathOffset(CharSequence path, long rowOffset) {
      this.path = path;
      this.rowOffset = rowOffset;
    }

    private static PathOffset of(CharSequence path, long rowOffset) {
      return new PathOffset(path, rowOffset);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("path", path)
          .add("row_offset", rowOffset)
          .toString();
    }

    @Override
    public int size() {
      return 2;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      switch (pos) {
        case 0:
          return javaClass.cast(path);
        case 1:
          return javaClass.cast(rowOffset);
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    private void put(int pos, Object value) {
      switch (pos) {
        case 0:
          this.path = (CharSequence) value;
          break;
        case 1:
          this.rowOffset = (long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      put(pos, value);
    }
  }

  private abstract class BaseRollingWriter<W extends Closeable> implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final StructLike partitionKey;

    private EncryptedOutputFile currentFile = null;
    private W currentWriter = null;
    private long currentRows = 0;

    private BaseRollingWriter(StructLike partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    abstract W newWriter(EncryptedOutputFile file, StructLike partition);

    abstract long length(W writer);

    abstract void write(W writer, T record);

    abstract void complete(W closedWriter);

    public void write(T record) throws IOException {
      write(currentWriter, record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    public CharSequence currentPath() {
      Preconditions.checkNotNull(currentFile, "The currentFile shouldn't be null");
      return currentFile.encryptingOutputFile().location();
    }

    public long currentRows() {
      return currentRows;
    }

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        this.currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        this.currentFile = fileFactory.newOutputFile(partitionKey);
      }
      this.currentWriter = newWriter(currentFile, partitionKey);
      this.currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      // TODO: ORC file now not support target file size before closed
      return !format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSize;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();

        if (currentRows == 0L) {
          io.deleteFile(currentFile.encryptingOutputFile());
        } else {
          complete(currentWriter);
        }

        this.currentFile = null;
        this.currentWriter = null;
        this.currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }

  protected class RollingFileWriter extends BaseRollingWriter<DataWriter<T>> {
    public RollingFileWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    DataWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      return appenderFactory.newDataWriter(file, format, partitionKey);
    }

    @Override
    long length(DataWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(DataWriter<T> writer, T record) {
      writer.add(record);
    }

    @Override
    void complete(DataWriter<T> closedWriter) {
      completedDataFiles.add(closedWriter.toDataFile());
    }
  }

  protected class RollingEqDeleteWriter extends BaseRollingWriter<EqualityDeleteWriter<T>> {
    RollingEqDeleteWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      return appenderFactory.newEqDeleteWriter(file, format, partitionKey);
    }

    @Override
    long length(EqualityDeleteWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(EqualityDeleteWriter<T> writer, T record) {
      writer.delete(record);
    }

    @Override
    void complete(EqualityDeleteWriter<T> closedWriter) {
      completedDeleteFiles.add(closedWriter.toDeleteFile());
    }
  }
}
