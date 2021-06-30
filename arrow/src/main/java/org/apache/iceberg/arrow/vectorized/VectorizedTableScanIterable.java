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

package org.apache.iceberg.arrow.vectorized;

import java.io.IOException;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * A vectorized implementation of the Iceberg reader that iterates over the table scan. See {@link ArrowReader} for
 * details.
 */
public class VectorizedTableScanIterable extends CloseableGroup implements CloseableIterable<ColumnarBatch> {

  private static final int BATCH_SIZE_IN_NUM_ROWS = 1 << 16;

  private final ArrowReader reader;
  private final CloseableIterable<CombinedScanTask> tasks;

  /**
   * Create a new instance using default values for {@code batchSize} and {@code reuseContainers}. The {@code batchSize}
   * is set to {@link #BATCH_SIZE_IN_NUM_ROWS} and {@code reuseContainers} is set to {@code false}.
   */
  public VectorizedTableScanIterable(TableScan scan) {
    this(scan, BATCH_SIZE_IN_NUM_ROWS, false);
  }

  /**
   * Create a new instance.
   * <p>
   * See {@link ArrowReader#ArrowReader(TableScan, int, boolean)} for details.
   */
  public VectorizedTableScanIterable(TableScan scan, int batchSize, boolean reuseContainers) {
    this.reader = new ArrowReader(scan, batchSize, reuseContainers);
    // start planning tasks in the background
    this.tasks = scan.planTasks();
  }

  @Override
  public CloseableIterator<ColumnarBatch> iterator() {
    CloseableIterator<ColumnarBatch> iter = reader.open(tasks);
    addCloseable(iter);
    return iter;
  }

  @Override
  public void close() throws IOException {
    tasks.close(); // close manifests from scan planning
    super.close(); // close data files
  }
}
