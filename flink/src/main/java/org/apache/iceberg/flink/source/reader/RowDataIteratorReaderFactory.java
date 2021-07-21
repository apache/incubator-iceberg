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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataIterator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public class RowDataIteratorReaderFactory extends DataIteratorReaderFactory<RowData> {

  private final Table table;
  private final ScanContext scanContext;

  public RowDataIteratorReaderFactory(
      Configuration config,
      Table table,
      ScanContext scanContext,
      RowType rowType) {
    super(config, new ArrayPoolDataIteratorBatcher<>(config, new RowDataRecordFactory(rowType)));
    this.table = table;
    this.scanContext = scanContext;
  }

  @Override
  protected DataIterator<RowData> createDataIterator(IcebergSourceSplit split) {
    return new RowDataIterator(
        split.task(),
        table.io(),
        table.encryption(),
        table.schema(),
        scanContext.project(),
        scanContext.nameMapping(),
        scanContext.caseSensitive());
  }
}
