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

package org.apache.iceberg.flink.sink.rewrite;

import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.flink.FlinkTableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;

public class FileScanTaskGenerateOperator extends
    AbstractRewriteOperator<Long, FileScanTaskGenerateOperatorOut> {
  private int rewriteTaskTriggerSnapCounts;
  private int rewriteTaskTriggerFileCounts;
  // snapshot counts to trigger merge;
  private transient Set<Long> committedSnapshotCache;

  public FileScanTaskGenerateOperator(TableLoader tableLoader) {
    super(tableLoader);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.committedSnapshotCache = Sets.newLinkedHashSet();
    this.rewriteTaskTriggerFileCounts = PropertyUtil.propertyAsInt(table().properties(),
        FlinkTableProperties.NUMS_DATAFILE_MERGE, FlinkTableProperties.NUMS_DATAFILE_MERGE_DEFAULT);
    this.rewriteTaskTriggerSnapCounts = PropertyUtil.propertyAsInt(table().properties(),
        FlinkTableProperties.NUMS_NEW_SNAPSHOT_MERGE, FlinkTableProperties.NUMS_NEW_SNAPSHOT_MERGE_DEFAULT);
  }

  @Override
  public void processElement(StreamRecord<Long> element) throws Exception {
    table().refresh();
    List<DataFile> addedFiles = Lists.newArrayList(table().currentSnapshot().addedFiles());

    committedSnapshotCache.add(element.getValue());
    if (committedSnapshotCache.size() == rewriteTaskTriggerSnapCounts ||
        addedFiles.size() >= rewriteTaskTriggerFileCounts) {
      forwardScanTask();
      committedSnapshotCache.clear();
    }
  }

  private void forwardScanTask() {
    final long currentTimeMillis = System.currentTimeMillis();
    List<CombinedScanTask> combinedScanTasks = generateFileScanTask();
    combinedScanTasks.forEach(combinedScanTask -> {
      output.collect(new StreamRecord<>(new FileScanTaskGenerateOperatorOut(combinedScanTask,
          currentTimeMillis, combinedScanTasks.size())));
    });
  }

  @Override
  public void endInput() throws Exception {
    super.endInput();
    if (committedSnapshotCache.size() > 0) {
      forwardScanTask();
      committedSnapshotCache.clear();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (committedSnapshotCache.size() > 0) {
      forwardScanTask();
      committedSnapshotCache.clear();
    }
  }
}
