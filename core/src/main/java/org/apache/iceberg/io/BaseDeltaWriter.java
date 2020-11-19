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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseDeltaWriter<T> implements DeltaWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseDeltaWriter.class);

  private final RollingContentFileWriter<DataFile, T> dataWriter;
  private final RollingContentFileWriter<DeleteFile, T> equalityDeleteWriter;
  private final RollingContentFileWriter<DeleteFile, PositionDelete<T>> posDeleteWriter;

  private final PositionDelete<T> positionDelete = new PositionDelete<>();
  private final StructLikeMap<RowOffset> insertedRowMap;

  // Function to convert the generic data to a StructLike.
  private final Function<T, StructLike> structLikeFun;

  public BaseDeltaWriter(RollingContentFileWriter<DataFile, T> dataWriter) {
    this(dataWriter, null);
  }

  public BaseDeltaWriter(RollingContentFileWriter<DataFile, T> dataWriter,
                         RollingContentFileWriter<DeleteFile, PositionDelete<T>> posDeleteWriter) {
    this(dataWriter, posDeleteWriter, null, null, null, null);
  }

  public BaseDeltaWriter(RollingContentFileWriter<DataFile, T> dataWriter,
                         RollingContentFileWriter<DeleteFile, PositionDelete<T>> posDeleteWriter,
                         RollingContentFileWriter<DeleteFile, T> equalityDeleteWriter,
                         Schema tableSchema,
                         List<Integer> equalityFieldIds,
                         Function<T, StructLike> structLikeFun) {

    Preconditions.checkNotNull(dataWriter, "Data writer should always not be null.");

    if (posDeleteWriter == null) {
      // Only accept INSERT records.
      Preconditions.checkArgument(equalityDeleteWriter == null,
          "Could not accept equality deletes when position delete writer is null.");
    }

    if (posDeleteWriter != null && equalityDeleteWriter == null) {
      // Only accept INSERT records and position deletion.
      Preconditions.checkArgument(tableSchema == null, "Table schema is only required for equality delete writer.");
      Preconditions.checkArgument(equalityFieldIds == null,
          "Equality field id list is only required for equality delete writer.");
    }

    if (equalityDeleteWriter != null) {
      // Accept insert records, position deletion, equality deletions.
      Preconditions.checkNotNull(posDeleteWriter,
          "Position delete writer shouldn't be null when writing equality deletions.");
      Preconditions.checkNotNull(tableSchema, "Iceberg table schema shouldn't be null");
      Preconditions.checkNotNull(equalityFieldIds, "Equality field ids shouldn't be null");
      Preconditions.checkNotNull(structLikeFun, "StructLike function shouldn't be null");

      Schema deleteSchema = TypeUtil.select(tableSchema, Sets.newHashSet(equalityFieldIds));
      this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
      this.structLikeFun = structLikeFun;
    } else {
      this.insertedRowMap = null;
      this.structLikeFun = null;
    }

    this.dataWriter = dataWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.posDeleteWriter = posDeleteWriter;
  }

  @Override
  public void writeRow(T row) {
    if (enableEqualityDelete()) {
      RowOffset rowOffset = RowOffset.create(dataWriter.currentPath(), dataWriter.currentRows());

      StructLike key = structLikeFun.apply(row);
      RowOffset previous = insertedRowMap.putIfAbsent(key, rowOffset);
      ValidationException.check(previous == null, "Detected duplicate insert for %s", key);
    }

    dataWriter.write(row);
  }

  @Override
  public void writeEqualityDelete(T equalityDelete) {
    if (!enableEqualityDelete()) {
      throw new UnsupportedOperationException("Could not accept equality deletion.");
    }

    StructLike key = structLikeFun.apply(equalityDelete);
    RowOffset existing = insertedRowMap.get(key);

    if (existing == null) {
      // Delete the row which have been written by other completed delta writer.
      equalityDeleteWriter.write(equalityDelete);
    } else {
      // Delete the rows which was written in current delta writer. If the position delete row schema is null, then the
      // writer won't write the records even if we provide the rows here.
      posDeleteWriter.write(positionDelete.set(existing.path, existing.rowId, equalityDelete));
      // Remove the records from insertedRowMap because we've already deleted it by writing position delete file.
      insertedRowMap.remove(key);
    }
  }

  @Override
  public void writePosDelete(CharSequence path, long offset, T row) {
    if (!enablePosDelete()) {
      throw new UnsupportedOperationException("Could not accept position deletion.");
    }

    posDeleteWriter.write(positionDelete.set(path, offset, row));
  }

  @Override
  public void abort() {
    if (dataWriter != null) {
      try {
        dataWriter.abort();
      } catch (IOException e) {
        LOG.warn("Failed to abort the data writer {} because: ", dataWriter, e);
      }
    }

    if (equalityDeleteWriter != null) {
      try {
        equalityDeleteWriter.abort();
      } catch (IOException e) {
        LOG.warn("Failed to abort the equality-delete writer {} because: ", equalityDeleteWriter, e);
      }
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      try {
        posDeleteWriter.abort();
      } catch (IOException e) {
        LOG.warn("Failed to abort the pos-delete writer {} because: ", posDeleteWriter, e);
      }
    }
  }

  @Override
  public WriterResult complete() throws IOException {
    WriterResult.Builder builder = WriterResult.builder();

    if (dataWriter != null) {
      builder.add(dataWriter.complete());
    }

    if (equalityDeleteWriter != null) {
      builder.add(equalityDeleteWriter.complete());
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      builder.add(posDeleteWriter.complete());
    }

    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (dataWriter != null) {
      dataWriter.close();
    }

    if (equalityDeleteWriter != null) {
      equalityDeleteWriter.close();
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      posDeleteWriter.close();
    }
  }

  private boolean enableEqualityDelete() {
    return equalityDeleteWriter != null && posDeleteWriter != null;
  }

  private boolean enablePosDelete() {
    return posDeleteWriter != null;
  }

  private static class RowOffset {
    private final CharSequence path;
    private final long rowId;

    private RowOffset(CharSequence path, long rowId) {
      this.path = path;
      this.rowId = rowId;
    }

    private static RowOffset create(CharSequence path, long pos) {
      return new RowOffset(path, pos);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("path", path)
          .add("pos", rowId)
          .toString();
    }
  }
}
