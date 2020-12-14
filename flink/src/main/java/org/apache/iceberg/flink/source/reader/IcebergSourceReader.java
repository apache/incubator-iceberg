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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergSourceReader<T> extends
    SingleThreadMultiplexSourceReaderBase<RecordAndPosition<T>, T, IcebergSourceSplit, IcebergSourceSplit> {

  public IcebergSourceReader(
      SourceReaderContext context,
      ReaderFactory<T> readerFactory) {
    super(
        () -> new IcebergSourceSplitReader<>(readerFactory),
        new IcebergSourceRecordEmitter(),
        context.getConfiguration(),
        context);
  }

  @Override
  public void start() {
    requestSplit(Collections.emptyList());
  }

  @Override
  protected void onSplitFinished(Map<String, IcebergSourceSplit> finishedSplitIds) {
    if (!finishedSplitIds.isEmpty()) {
      requestSplit(Lists.newArrayList(finishedSplitIds.keySet()));
    }
  }

  @Override
  protected IcebergSourceSplit initializedState(IcebergSourceSplit split) {
    return split;
  }

  @Override
  protected IcebergSourceSplit toSplitType(
      String splitId,
      IcebergSourceSplit splitState) {
    return splitState;
  }

  private void requestSplit(Collection<String> finishedSplitIds) {
    context.sendSourceEventToCoordinator(new IcebergSourceEvents.SplitRequestEvent(finishedSplitIds));
  }
}
