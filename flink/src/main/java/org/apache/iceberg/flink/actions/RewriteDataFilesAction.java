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

package org.apache.iceberg.flink.actions;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDataFilesAction;
import org.apache.iceberg.flink.source.RowDataRewriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class RewriteDataFilesAction extends BaseRewriteDataFilesAction<RewriteDataFilesAction> {

  private StreamExecutionEnvironment env;
  private int maxParallelism;

  public RewriteDataFilesAction(StreamExecutionEnvironment env, Table table) {
    super(table);
    this.env = env;
    this.maxParallelism = env.getParallelism();
  }

  @Override
  protected FileIO fileIO() {
    return table().io();
  }

  @Override
  protected RewriteResult rewriteDataForTasks(List<CombinedScanTask> combinedScanTasks) {
    int size = combinedScanTasks.size();
    int parallelism = Math.min(size, maxParallelism);

    DataStream<CombinedScanTask> dataStream = env.fromCollection(combinedScanTasks);
    RowDataRewriter rowDataRewriter = new RowDataRewriter(table(), caseSensitive(), fileIO(), encryptionManager());
    try {
      return rowDataRewriter.rewriteDataForTasks(dataStream, parallelism);
    } catch (Exception e) {
      throw new RuntimeException("Rewrite data file error.", e);
    }
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }

  public RewriteDataFilesAction maxParallelism(int parallelism) {
    Preconditions.checkArgument(parallelism > 0, "Invalid max parallelism %d", parallelism);
    this.maxParallelism = parallelism;
    return this;
  }
}
