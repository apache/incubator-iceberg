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

package org.apache.iceberg.actions;

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.actions.compaction.DataCompactionStrategy;
import org.apache.iceberg.expressions.Expression;

public interface CompactDataFiles extends Action<CompactDataFiles, CompactDataFiles.Result> {

  /**
   * The parallelism level to use when processing jobs generated by the Compaction Strategy. The structure
   * and contents of the jobs is determined by the Compaction Strategy and options passed to it. When running
   * each job will be run independently and asynchronously.
   *
   * @param par number of concurrent jobs
   * @return
   */
  CompactDataFiles parallelism(int par);

  /**
   * Pass an already constructed DataCompactionStrategy
   *
   * @param strategy the algorithm to be used during compaction
   * @return
   */
  CompactDataFiles compactionStrategy(DataCompactionStrategy strategy);

  // Todo Do we want to allow generic class loading here? I think yes this will probably be framework specific
  CompactDataFiles compactionStrategy(String strategyName);

  /**
   * A user provided filter for determining which files will be considered by the
   * Compaction strategy. This will be used in addition to whatever rules the Compaction strategy
   * generates.
   *
   * @param expression
   * @return this for chaining
   */
  CompactDataFiles filter(Expression expression);

  class Result {
    private Map<CompactionJobInfo, CompactionResult> resultMap;


    public Result(
        Map<CompactionJobInfo, CompactionResult> resultMap) {
      this.resultMap = resultMap;
    }

    public Map<CompactionJobInfo, CompactionResult> resultMap() {
      return resultMap;
    }
  }

  class CompactionResult {
    private int numFilesRemoved;
    private int numFilesAdded;

    public CompactionResult(int numFilesRemoved, int numFilesAdded) {
      this.numFilesRemoved = numFilesRemoved;
      this.numFilesAdded = numFilesAdded;
    }

    public int numFilesAdded() {
      return numFilesAdded;
    }

    public int numFilesRemoved() {
      return numFilesRemoved;
    }
  }

  class CompactionJobInfo {
    private final int jobIndex;
    private final int partitionIndex;
    private final StructLike partition;

    public CompactionJobInfo(int jobIndex, int partitionIndex, StructLike partition) {
      this.jobIndex = jobIndex;
      this.partitionIndex = partitionIndex;
      this.partition = partition;
    }

    public int jobIndex() {
      return jobIndex;
    }

    public int partitionIndex() {
      return partitionIndex;
    }

    public StructLike partition() {
      return partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CompactionJobInfo that = (CompactionJobInfo) o;
      return jobIndex == that.jobIndex && partitionIndex == that.partitionIndex &&
          Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobIndex, partitionIndex, partition);
    }
  }
}
