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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;

public class TestRowDataIteratorReaderFactory extends ReaderFactoryTestBase<RowData> {

  protected static final RowType rowType = FlinkSchemaUtil
      .convert(scanContext.project());
  private static final DataStructureConverter<Object, Object> rowDataConverter = DataStructureConverters.getConverter(
      TypeConversions.fromLogicalToDataType(rowType));
  private static final org.apache.flink.configuration.Configuration flinkConfig =
      new org.apache.flink.configuration.Configuration();

  public TestRowDataIteratorReaderFactory(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected ReaderFactory<RowData> getReaderFactory() {
    return new RowDataIteratorReaderFactory(new Configuration(), tableResource.table(), scanContext, rowType);
  }

  @Override
  protected void assertRecords(List<Record> expected, List<RowData> actual, Schema schema) {
    final List<Row> rows = toRows(actual);
    TestHelpers.assertRecords(rows, expected, TestFixtures.SCHEMA);
  }

  private List<Row> toRows(List<RowData> actual) {
    return actual.stream()
        .map(rowData -> (Row) rowDataConverter.toExternal(rowData))
        .collect(Collectors.toList());
  }
}
