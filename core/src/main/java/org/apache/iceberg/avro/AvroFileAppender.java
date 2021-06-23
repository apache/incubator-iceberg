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

package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class AvroFileAppender<D> implements FileAppender<D> {
  private PositionOutputStream stream = null;
  private DataFileWriter<D> writer = null;
  private DatumWriter<?> datumWriter = null;
  private org.apache.iceberg.Schema icebergSchema;
  private MetricsConfig metricsConfig;
  private long numRecords = 0L;
  private boolean isClosed = false;

  AvroFileAppender(org.apache.iceberg.Schema icebergSchema, Schema schema, OutputFile file,
                   Function<Schema, DatumWriter<?>> createWriterFunc,
                   CodecFactory codec, Map<String, String> metadata,
                   MetricsConfig metricsConfig, boolean overwrite) throws IOException {
    this.icebergSchema = icebergSchema;
    this.stream = overwrite ? file.createOrOverwrite() : file.create();
    this.datumWriter = createWriterFunc.apply(schema);
    this.writer = newAvroWriter(schema, stream, datumWriter, codec, metadata);
    this.metricsConfig = metricsConfig;
  }

  @Override
  public void add(D datum) {
    try {
      numRecords += 1L;
      writer.append(datum);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(isClosed,
        "Cannot return metrics while appending to an open file.");

    return AvroMetrics.fromWriter(datumWriter, icebergSchema, numRecords, metricsConfig);
  }

  @Override
  public long length() {
    if (stream != null) {
      try {
        return stream.getPos();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to get stream length");
      }
    }
    throw new RuntimeIOException("Failed to get stream length: no open stream");
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      this.writer = null;
      isClosed = true;
    }
  }

  @SuppressWarnings("unchecked")
  private static <D> DataFileWriter<D> newAvroWriter(
      Schema schema, PositionOutputStream stream, DatumWriter<?> datumWriter,
      CodecFactory codec, Map<String, String> metadata) throws IOException {
    DataFileWriter<D> writer = new DataFileWriter<>((DatumWriter<D>) datumWriter);

    writer.setCodec(codec);

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      writer.setMeta(entry.getKey(), entry.getValue());
    }

    return writer.create(schema, stream);
  }
}
