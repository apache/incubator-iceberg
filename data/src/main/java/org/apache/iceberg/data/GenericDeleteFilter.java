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

package org.apache.iceberg.data;

import java.util.function.Consumer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

public class GenericDeleteFilter extends DeleteFilter<Record> {
  private final FileIO io;
  private final InternalRecordWrapper asStructLike;

  public GenericDeleteFilter(FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema) {
    super(task, tableSchema, requestedSchema);
    this.io = io;
    this.asStructLike = new InternalRecordWrapper(requiredSchema().asStruct());
  }

  @Override
  protected long pos(Record record) {
    return (Long) posAccessor().get(record);
  }

  @Override
  protected Consumer<Record> deleteMarker() {
    return record -> record.set(deleteMarkerIndex(), true);
  }

  @Override
  protected boolean isDeletedRow(Record record) {
    return record.get(deleteMarkerIndex(), Boolean.class);
  }

  @Override
  protected StructLike asStructLike(Record record) {
    return asStructLike.wrap(record);
  }

  @Override
  protected InputFile getInputFile(String location) {
    return io.newInputFile(location);
  }
}
