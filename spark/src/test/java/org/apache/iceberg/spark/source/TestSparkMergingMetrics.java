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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestMergingMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;

public class TestSparkMergingMetrics extends TestMergingMetrics<InternalRow> {

  public TestSparkMergingMetrics(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected FileAppender<InternalRow> writeAndGetAppender(List<Record> records, Table table) throws IOException {
    FileAppender<InternalRow> appender =
        SparkAppenderFactory.builderFor(table, SCHEMA, SparkSchemaUtil.convert(SCHEMA)).build()
            .newAppender(org.apache.iceberg.Files.localOutput(temp.newFile()), fileFormat);
    try (FileAppender<InternalRow> fileAppender = appender) {
      records.stream().map(r -> new StructInternalRow(SCHEMA.asStruct()).setStruct(r)).forEach(fileAppender::add);
    }
    return appender;
  }
}
