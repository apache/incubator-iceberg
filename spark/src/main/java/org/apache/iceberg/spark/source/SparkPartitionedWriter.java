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

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class SparkPartitionedWriter extends PartitionedWriter<InternalRow> {
  private final PartitionKey partitionKey;
  private final InternalRowWrapper internalRowWrapper;

  public SparkPartitionedWriter(PartitionSpec spec, FileFormat format,
                                FileAppenderFactory<InternalRow> appenderFactory,
                                OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                                Map<String, String> properties, Schema schema, StructType sparkSchema) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, properties);
    this.partitionKey = new PartitionKey(spec, schema);
    this.internalRowWrapper = new InternalRowWrapper(sparkSchema);
  }

  @Override
  protected PartitionKey partition(InternalRow row) {
    partitionKey.partition(internalRowWrapper.wrap(row));
    return partitionKey;
  }
}
