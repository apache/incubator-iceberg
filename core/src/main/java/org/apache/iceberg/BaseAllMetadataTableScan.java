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

package org.apache.iceberg;

import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseAllMetadataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAllMetadataTableScan.class);

  BaseAllMetadataTableScan(TableOperations ops, Table table, Schema fileSchema) {
    super(ops, table, fileSchema);
  }

  BaseAllMetadataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  /**
   * Type of scan being performed, such as {@link MetadataTableType#ALL_DATA_FILES} when scanning
   * a table's {@link org.apache.iceberg.AllDataFilesTable}.
   * <p>
   * Used for logging and error messages.
   */
  protected abstract String tableType();

  @Override
  public TableScan dataBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType()));
  }

  @Override
  public TableScan dataAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType()));
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    LOG.info("Scanning metadata table {} with filter {}.", table(), filter());
    Listeners.notifyAll(new ScanEvent(table().name(), 0L, filter(), schema()));

    return planFiles(tableOps(), snapshot(), filter(), shouldIgnoreResiduals(), isCaseSensitive(), colStats());
  }
}
