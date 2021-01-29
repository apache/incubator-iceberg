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

package org.apache.iceberg.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.test.Cat;
import org.joda.time.Duration;
import org.junit.Test;

public class StreamingTest extends BaseTest {
  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);

  @Test
  public void testWriteFiles() {
    for (FileFormat format : FILEFORMATS) {
      runPipeline(format);
    }
  }

  public void runPipeline(FileFormat fileFormat) {
    // We should see four commits in the log
    TestStream<GenericRecord> records =
        TestStream.create(AvroCoder.of(Cat.getClassSchema()))
            .advanceWatermarkTo(START_TIME)
            .addElements(event(genericCats.get(0), 2L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(60L)))
            .addElements(event(genericCats.get(1), 62L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(120L)))
            .addElements(event(genericCats.get(2), 122L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(180L)))
            .addElements(event(genericCats.get(3), 182L))
            .advanceWatermarkToInfinity();

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(Cat.getClassSchema());
    TableIdentifier name = TableIdentifier.of("default", "test_streaming_" + fileFormat.name());

    PCollection<GenericRecord> windowed = pipeline
        .apply(records)
        .apply(Window.into(FixedWindows.of(WINDOW_DURATION)));

    new IcebergIO.Builder()
        .withSchema(icebergSchema)
        .withTableIdentifier(name)
        .withHiveMetastoreUrl(hiveMetastoreUrl)
        .conf(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name())
        .build(windowed);

    pipeline.run(options).waitUntilFinish();
  }
}
