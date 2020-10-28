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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkTableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;

abstract class AbstractRewriteOperator<InT, OutT> extends AbstractStreamOperator<OutT>
    implements OneInputStreamOperator<InT, OutT>, BoundedOneInput {

  private final Expression filter;
  private final TableLoader tableLoader;
  private boolean caseSensitive;
  private transient PartitionSpec spec;
  private transient long targetSizeInBytes;
  private transient long splitOpenFileCost;
  private transient int splitLookback;
  private transient Table table;

  AbstractRewriteOperator(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
    this.filter = Expressions.alwaysTrue();
    super.setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    long splitSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    long targetFileSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetSizeInBytes = Math.min(splitSize, targetFileSize);

    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    this.splitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    this.caseSensitive = PropertyUtil.propertyAsBoolean(
        table.properties(),
        FlinkTableProperties.CASE_SENSITIVE,
        FlinkTableProperties.CASE_SENSITIVE_DEFAULT
    );

    this.spec = table.spec();
  }

  protected List<CombinedScanTask> generateFileScanTask() {
    // a bin contains targetSizeInBytes / splitOpenFileCost items,
    // a bin group contains max splitLookback bins. if bin group size < splitLookback,
    // then return the first bin for each time, else remove the largest and return.
    return generateFilteredGroupedTasks().values().stream()
        .map(scanTasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(scanTasks), targetSizeInBytes);
          return TableScanUtil
              .planTasks(splitTasks, targetSizeInBytes, splitLookback, splitOpenFileCost);
        })
        .flatMap(Streams::stream)
        .collect(Collectors.toList());
  }

  private Map<StructLikeWrapper, Collection<FileScanTask>> generateFilteredGroupedTasks() {
    Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> fileScanTasks =
             table.newScan()
                 .caseSensitive(caseSensitive)
                 .ignoreResiduals()
                 .filter(filter)
                 .planFiles()) {
      Preconditions.checkArgument(fileScanTasks != null, "file scan task should not be null.");
      groupedTasks = groupTasksByPartition(fileScanTasks.iterator());
    } catch (IOException e) {
      LOG.warn("Failed to close task iterable", e);
    }
    return groupedTasks.entrySet().stream()
        .filter(kv -> !kv.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(
      CloseableIterator<FileScanTask> tasksIter) {
    ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition = Multimaps.newListMultimap(
        Maps.newHashMap(), Lists::newArrayList);
    try (CloseableIterator<FileScanTask> iter = tasksIter) {
      iter.forEachRemaining(task -> {
        StructLikeWrapper structLike = StructLikeWrapper.forType(spec.partitionType()).set(task.file().partition());
        tasksGroupedByPartition.put(structLike, task);
      });
    } catch (IOException e) {
      LOG.warn("Failed to close task iterator", e);
    }
    return tasksGroupedByPartition.asMap();
  }

  @Override
  public void endInput() throws Exception {
  }

  public Table table() {
    return table;
  }

  public long getTargetSizeInBytes() {
    return targetSizeInBytes;
  }
}
