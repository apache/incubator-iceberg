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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

@Internal
final class IcebergSourceRecordEmitter<T> implements RecordEmitter<RecordAndPosition<T>, T, IcebergSourceSplit> {

  @Override
  public void emitRecord(
      RecordAndPosition<T> element,
      SourceOutput<T> output,
      IcebergSourceSplit split) {

    output.collect(element.getRecord());
    split.updatePosition(element.getOffset(), element.getRecordSkipCount());
  }
}
