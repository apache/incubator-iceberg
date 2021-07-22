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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private final RowDataDeltaWriter writer;

  UnpartitionedDeltaWriter(PartitionSpec spec,
                           FileFormat format,
                           FileAppenderFactory<RowData> appenderFactory,
                           OutputFileFactory fileFactory,
                           FileIO io,
                           long targetFileSize,
                           Map<String, String> properties,
                           Schema schema,
                           RowType flinkSchema,
                           List<Integer> equalityFieldIds) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, properties, schema, flinkSchema,
        equalityFieldIds);
    this.writer = new RowDataDeltaWriter(null);
  }

  @Override
  RowDataDeltaWriter route(RowData row) {
    return writer;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
