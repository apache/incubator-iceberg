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

package org.apache.iceberg;

import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

abstract class PartitionStatsWriter implements FileAppender<PartitionStatsEntry> {
  private final FileAppender<PartitionStatsEntry> writer;

  protected PartitionStatsWriter(PartitionSpec spec, OutputFile file) {
    this.writer = newAppender(spec, file);
  }

  @Override
  public void add(PartitionStatsEntry datum) {
    writer.add(prepare(datum));
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public long length() {
    return writer.length();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  protected abstract PartitionStatsEntry prepare(PartitionStatsEntry entry);

  protected abstract FileAppender<PartitionStatsEntry> newAppender(PartitionSpec spec, OutputFile outputFile);

  static class V2Writer extends PartitionStatsWriter {
    private final V2Metadata.IndexedPartitionStatsEntry entryWrapper;

    V2Writer(PartitionSpec spec, OutputFile file) {
      super(spec, file);
      this.entryWrapper = new V2Metadata.IndexedPartitionStatsEntry(spec.partitionType());
    }

    @Override
    protected PartitionStatsEntry prepare(PartitionStatsEntry entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<PartitionStatsEntry> newAppender(PartitionSpec spec, OutputFile outputFile) {
      try {
        return Avro.write(outputFile)
            .schema(PartitionStatsEntry.getSchema(spec.partitionType()))
            .named("partitionStats_file")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: %s", outputFile);
      }
    }
  }
}
