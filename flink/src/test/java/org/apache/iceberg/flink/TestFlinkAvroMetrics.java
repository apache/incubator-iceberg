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

package org.apache.iceberg.flink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.TestAvroMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

public class TestFlinkAvroMetrics extends TestAvroMetrics  {

  @Override
  protected Metrics getMetrics(Schema schema, OutputFile file, Map<String, String> properties,
                               MetricsConfig metricsConfig, Record... records) throws IOException {
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    FileAppender<RowData> writer = Avro.write(file)
        .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
        .metricsConfig(metricsConfig)
        .setAll(properties)
        .schema(schema)
        .overwrite()
        .build();

    try (FileAppender<RowData> fileAppender = writer) {
      Arrays.stream(records).map(r -> RowDataConverter.convert(schema, r)).forEach(fileAppender::add);
    }

    return writer.metrics();
  }
}
