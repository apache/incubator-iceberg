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

import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ArrayUtil;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final Table table;
  private final RowType flinkSchema;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final List<Integer> equalityFieldIds;
  private final FileAppenderFactory<RowData> appenderFactory;

  private transient OutputFileFactory outputFileFactory;

  public RowDataTaskWriterFactory(Table table,
                                  RowType flinkSchema,
                                  LocationProvider locations,
                                  FileIO io,
                                  EncryptionManager encryptionManager,
                                  long targetFileSizeBytes,
                                  FileFormat format,
                                  Map<String, String> tableProperties,
                                  List<Integer> equalityFieldIds) {
    this.table = table;
    this.flinkSchema = flinkSchema;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.equalityFieldIds = equalityFieldIds;

    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      this.appenderFactory = new FlinkAppenderFactory(table, flinkSchema, tableProperties);
    } else {
      // TODO provide the ability to customize the equality-delete row schema.
      this.appenderFactory = new FlinkAppenderFactory(table, flinkSchema, tableProperties,
          ArrayUtil.toIntArray(equalityFieldIds), table.schema(), null);
    }
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = new OutputFileFactory(table.spec(), format, locations, io, encryptionManager,
        taskId, attemptId);
  }

  @Override
  public TaskWriter<RowData> create() {
    Preconditions.checkNotNull(outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      // Initialize a task writer to write INSERT only.
      if (table.spec().isUnpartitioned()) {
        return new UnpartitionedWriter<>(table.spec(), format, appenderFactory, outputFileFactory, io,
            targetFileSizeBytes);
      } else {
        return new RowDataPartitionedFanoutWriter(table.spec(), format, appenderFactory, outputFileFactory,
            io, targetFileSizeBytes, table.schema(), flinkSchema);
      }
    } else {
      // Initialize a task writer to write both INSERT and equality DELETE.
      if (table.spec().isUnpartitioned()) {
        return new UnpartitionedDeltaWriter(table.spec(), format, appenderFactory, outputFileFactory, io,
            targetFileSizeBytes, table.schema(), flinkSchema, equalityFieldIds);
      } else {
        return new PartitionedDeltaWriter(table.spec(), format, appenderFactory, outputFileFactory, io,
            targetFileSizeBytes, table.schema(), flinkSchema, equalityFieldIds);
      }
    }
  }

  private static class RowDataPartitionedFanoutWriter extends PartitionedFanoutWriter<RowData> {

    private final PartitionKey partitionKey;
    private final RowDataWrapper rowDataWrapper;

    RowDataPartitionedFanoutWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<RowData> appenderFactory,
                                   OutputFileFactory fileFactory, FileIO io, long targetFileSize, Schema schema,
                                   RowType flinkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected PartitionKey partition(RowData row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      return partitionKey;
    }
  }
}
