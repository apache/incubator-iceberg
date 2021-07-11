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

package org.apache.iceberg.data;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IcebergGenerics {
  private IcebergGenerics() {
  }

  /**
   * Returns a builder to configure a read of the given table that produces generic records.
   *
   * @param table an Iceberg table
   * @return a builder to configure the scan
   */
  public static ScanBuilder read(Table table) {
    return new ScanBuilder(table);
  }

  public static class ScanBuilder {
    private TableScan tableScan;
    private boolean reuseContainers = false;

    public ScanBuilder(Table table) {
      this.tableScan = table.newScan();
    }

    public ScanBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    public ScanBuilder where(Expression rowFilter) {
      this.tableScan = tableScan.filter(rowFilter);
      return this;
    }

    public ScanBuilder caseInsensitive() {
      this.tableScan = tableScan.caseSensitive(false);
      return this;
    }

    public ScanBuilder select(String... selectedColumns) {
      this.tableScan = tableScan.select(ImmutableList.copyOf(selectedColumns));
      return this;
    }

    public ScanBuilder useSnapshot(long scanSnapshotId) {
      this.tableScan = tableScan.useSnapshot(scanSnapshotId);
      return this;
    }

    public ScanBuilder asOfTime(long scanTimestampMillis) {
      this.tableScan = tableScan.asOfTime(scanTimestampMillis);
      return this;
    }

    public ScanBuilder appendsBetween(long fromSnapshotId, long toSnapshotId) {
      this.tableScan = tableScan.dataBetween(fromSnapshotId, toSnapshotId);
      return this;
    }

    public ScanBuilder appendsAfter(long fromSnapshotId) {
      this.tableScan = tableScan.dataAfter(fromSnapshotId);
      return this;
    }

    public CloseableIterable<Record> build() {
      return new TableScanIterable(
          tableScan,
          reuseContainers
      );
    }
  }
}
