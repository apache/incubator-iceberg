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
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestMergingMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.io.FileAppender;

public class TestFlinkMergingMetrics extends TestMergingMetrics<RowData> {

  public TestFlinkMergingMetrics(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected FileAppender<RowData> writeAndGetAppender(List<Record> records,
                                                      Table table) throws IOException {
    RowType flinkSchema = FlinkSchemaUtil.convert(SCHEMA);
    FileAppender<RowData> appender =
        new FlinkAppenderFactory(table, flinkSchema, table.properties())
            .newAppender(org.apache.iceberg.Files.localOutput(temp.newFile()), fileFormat);
    try (FileAppender<RowData> fileAppender = appender) {
      records.stream().map(r -> RowDataConverter.convert(SCHEMA, r)).forEach(fileAppender::add);
    }
    return appender;
  }
}
