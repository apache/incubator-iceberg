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

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestMergingMetrics;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class TestGenericMergingMetrics extends TestMergingMetrics<Record> {

  public TestGenericMergingMetrics(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected FileAppender<Record> writeAndGetAppender(List<Record> records) throws IOException {
    return writeAndGetAppender(records, ImmutableMap.of(), SortOrder.unsorted());
  }

  @Override
  protected FileAppender<Record> writeAndGetAppender(List<Record> records,
                                                     Map<String, String> properties,
                                                     SortOrder sortOrder) throws IOException {
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(
        SCHEMA, PartitionSpec.unpartitioned(), sortOrder);
    properties.forEach(appenderFactory::set);

    FileAppender<Record> appender = appenderFactory.newAppender(
        org.apache.iceberg.Files.localOutput(temp.newFile()), fileFormat);
    try (FileAppender<Record> fileAppender = appender) {
      records.forEach(fileAppender::add);
    }
    return appender;
  }
}
