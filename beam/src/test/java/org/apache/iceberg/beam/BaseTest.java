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

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.iceberg.beam.util.TestHiveMetastore;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

public abstract class BaseTest {
  protected static final Instant START_TIME = new Instant(0);
  protected static final List<String> SENTENCES =
      Arrays.asList(
          "Beam window 1 1",
          "Beam window 1 2",
          "Beam window 1 3",
          "Beam window 1 4",
          "Beam window 2 1",
          "Beam window 2 2");
  protected static final PipelineOptions options = TestPipeline.testingPipelineOptions();
  protected static final String stringSchema = "{\n" +
      "\t\"type\": \"record\",\n" +
      "\t\"name\": \"Word\",\n" +
      "\t\"fields\": [{\n" +
      "\t\t\"name\": \"word\",\n" +
      "\t\t\"type\": [\"null\", \"string\"],\n" +
      "\t\t\"default\": null\n" +
      "\t}]\n" +
      "}";
  private static TestHiveMetastore metastore;
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();
  protected final String hiveMetastoreUrl = "thrift://localhost:9083/default";
  protected final Schema avroSchema = new Schema.Parser().parse(stringSchema);

  @BeforeClass
  public static void startMetastore() {
    metastore = new TestHiveMetastore();
    metastore.start();
  }

  @AfterClass
  public static void stopMetastore() {
    metastore.stop();
  }

  protected TimestampedValue<String> event(String word, Long timestamp) {
    return TimestampedValue.of(word, START_TIME.plus(new Duration(timestamp)));
  }
}