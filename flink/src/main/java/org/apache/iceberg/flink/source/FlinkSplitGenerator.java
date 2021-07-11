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
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class FlinkSplitGenerator {
  private FlinkSplitGenerator() {
  }

  static FlinkInputSplit[] createInputSplits(Table table, ScanContext context) {
    List<CombinedScanTask> tasks = tasks(table, context);
    FlinkInputSplit[] splits = new FlinkInputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new FlinkInputSplit(i, tasks.get(i));
    }
    return splits;
  }

  private static List<CombinedScanTask> tasks(Table table, ScanContext context) {
    TableScan scan = table
        .newScan()
        .caseSensitive(context.caseSensitive())
        .project(context.project());

    if (context.snapshotId() != null) {
      scan = scan.useSnapshot(context.snapshotId());
    }

    if (context.asOfTimestamp() != null) {
      scan = scan.asOfTime(context.asOfTimestamp());
    }

    if (context.startSnapshotId() != null) {
      if (context.endSnapshotId() != null) {
        scan = scan.dataBetween(context.startSnapshotId(), context.endSnapshotId());
      } else {
        scan = scan.dataAfter(context.startSnapshotId());
      }
    }

    if (context.splitSize() != null) {
      scan = scan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());
    }

    if (context.splitLookback() != null) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());
    }

    if (context.splitOpenFileCost() != null) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());
    }

    if (context.filters() != null) {
      for (Expression filter : context.filters()) {
        scan = scan.filter(filter);
      }
    }

    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      return Lists.newArrayList(tasksIterable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close table scan: " + scan, e);
    }
  }
}
