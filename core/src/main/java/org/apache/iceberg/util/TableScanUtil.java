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

package org.apache.iceberg.util;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.types.Types;

public class TableScanUtil {

  private TableScanUtil() {
  }

  public static boolean hasDeletes(CombinedScanTask task) {
    return task.files().stream().anyMatch(TableScanUtil::hasDeletes);
  }

  public static boolean hasDeletes(FileScanTask task) {
    return !task.deletes().isEmpty();
  }

  public static CloseableIterable<FileScanTask> splitFiles(CloseableIterable<FileScanTask> tasks, long splitSize) {
    Iterable<FileScanTask> splitTasks = FluentIterable
        .from(tasks)
        .transformAndConcat(input -> input.split(splitSize));
    // Capture manifests which can be closed after scan planning
    return CloseableIterable.combine(splitTasks, tasks);
  }

  public static CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> splitFiles,
      long splitSize, int lookback, long openFileCost) {
    return planTasks(splitFiles, splitSize, lookback, openFileCost, null, null);
  }

  public static CloseableIterable<CombinedScanTask> planTasks(CloseableIterable<FileScanTask> splitFiles,
      long splitSize, int lookback, long openFileCost, PartitionSpec spec,
      Set<Integer> preservedPartitionIds) {
    Function<FileScanTask, Long> weightFunc = file -> Math.max(file.length(), openFileCost);

    if (preservedPartitionIds != null && !preservedPartitionIds.isEmpty()) {
      Preconditions.checkArgument(spec != null, "spec can't be null when " +
          "preservedPartitionIds is not null");
      StructProjection projectedStruct = StructProjection.create(spec.partitionType(),
          preservedPartitionIds);
      Types.StructType projectedPartitionType = projectedStruct.type();
      ListMultimap<StructLikeWrapper, FileScanTask> groupedFiles = Multimaps.newListMultimap(
          Maps.newHashMap(), Lists::newArrayList);

      splitFiles.forEach(f -> {
        StructLikeWrapper wrapper = StructLikeWrapper.forType(projectedPartitionType)
            .set(projectedStruct.copy().wrap(f.file().partition()));
        groupedFiles.put(wrapper, f);
      });

      List<Iterable<BaseCombinedScanTask>> groupedTasks =
              groupedFiles.asMap().values().stream().map(t ->
                  Iterables.transform(
                  new BinPacking.PackingIterable<>(CloseableIterable.withNoopClose(t),
                      splitSize, lookback, weightFunc, true),
                      BaseCombinedScanTask::new
                  )).collect(Collectors.toList());
      return CloseableIterable.combine(Iterables.concat(groupedTasks), splitFiles);
    } else {
      return CloseableIterable.transform(
          CloseableIterable.combine(
              new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
              splitFiles),
          BaseCombinedScanTask::new);
    }
  }
}
