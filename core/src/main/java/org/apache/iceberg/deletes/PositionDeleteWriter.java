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

package org.apache.iceberg.deletes;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.CharSequenceSet;

public class PositionDeleteWriter<T> implements Closeable {
  private final FileAppender<StructLike> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final PositionDelete<T> delete;
  private final Set<CharSequence> pathSet;
  private DeleteFile deleteFile = null;

  public PositionDeleteWriter(FileAppender<StructLike> appender, FileFormat format, String location,
                              PartitionSpec spec, StructLike partition, EncryptionKeyMetadata keyMetadata) {
    this.appender = appender;
    this.format = format;
    this.location = location;
    this.spec = spec;
    this.partition = partition;
    this.keyMetadata = keyMetadata != null ? keyMetadata.buffer() : null;
    this.delete = PositionDelete.create();
    this.pathSet = CharSequenceSet.empty();
  }

  public void delete(CharSequence path, long pos) {
    delete(path, pos, null);
  }

  public void delete(CharSequence path, long pos, T row) {
    pathSet.add(path);
    appender.add(delete.set(path, pos, row));
  }

  @Override
  public void close() throws IOException {
    if (deleteFile == null) {
      appender.close();
      this.deleteFile = FileMetadata.deleteFileBuilder(spec)
          .ofPositionDeletes()
          .withFormat(format)
          .withPath(location)
          .withPartition(partition)
          .withEncryptionKeyMetadata(keyMetadata)
          .withFileSizeInBytes(appender.length())
          .withMetrics(appender.metrics())
          .build();
    }
  }

  public Set<CharSequence> referencedDataFiles() {
    return pathSet;
  }

  public DeleteFile toDeleteFile() {
    Preconditions.checkState(deleteFile != null, "Cannot create delete file from unclosed writer");
    return deleteFile;
  }
}
