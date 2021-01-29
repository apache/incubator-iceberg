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

package org.apache.iceberg.beam.writers;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.data.parquet.BaseParquetWriter;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.parquet.schema.MessageType;

public class GenericAvroParquetWriter extends BaseParquetWriter<GenericRecord> {
  private static final GenericAvroParquetWriter INSTANCE = new GenericAvroParquetWriter();

  private GenericAvroParquetWriter() {
  }

  public static ParquetValueWriter<GenericRecord> buildWriter(MessageType type) {
    return INSTANCE.createWriter(type);
  }

  @Override
  protected ParquetValueWriters.StructWriter<GenericRecord> createStructWriter(
      List<ParquetValueWriter<?>> writers
  ) {
    return new RecordWriter(writers);
  }

  private static class RecordWriter extends ParquetValueWriters.StructWriter<GenericRecord> {
    private RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(GenericRecord struct, int index) {
      return struct.get(index);
    }
  }
}
