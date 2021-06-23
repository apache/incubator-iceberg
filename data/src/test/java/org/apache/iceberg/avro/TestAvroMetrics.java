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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class TestAvroMetrics extends TestMetrics {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  public FileFormat fileFormat() {
    return FileFormat.AVRO;
  }

  @Override
  public Metrics getMetrics(Schema schema, MetricsConfig metricsConfig, Record... records) throws IOException {
    return getMetrics(schema, createOutputFile(), ImmutableMap.of(), metricsConfig, records);
  }

  @Override
  public Metrics getMetrics(Schema schema, Record... records) throws IOException {
    return getMetrics(schema, MetricsConfig.getDefault(), records);
  }

  protected abstract Metrics getMetrics(Schema schema, OutputFile file, Map<String, String> properties,
                                        MetricsConfig metricsConfig, Record... records) throws IOException;

  @Override
  protected Metrics getMetricsForRecordsWithSmallRowGroups(Schema schema, OutputFile outputFile, Record... records) {
    throw new UnsupportedOperationException("supportsSmallRowGroups = " + supportsSmallRowGroups());
  }

  @Override
  public int splitCount(InputFile inputFile) throws IOException {
    return 0;
  }

  @Override
  protected OutputFile createOutputFile() throws IOException {
    File tmpFolder = temp.newFolder("avro");
    String filename = UUID.randomUUID().toString();
    return Files.localOutput(new File(tmpFolder, FileFormat.AVRO.addExtension(filename)));
  }
}
