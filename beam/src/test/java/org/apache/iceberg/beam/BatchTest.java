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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.test.Cat;
import org.junit.Test;

public class BatchTest extends BaseTest {

  @Test
  public void testWriteFiles() {
    for (FileFormat format : FILEFORMATS) {
      runPipeline(format);
    }
  }

  public void runPipeline(FileFormat fileFormat) {
    final Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(Cat.SCHEMA$));

    PCollection<GenericRecord> records = p.apply(Create.of(genericCats));

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(Cat.getClassSchema());
    TableIdentifier name = TableIdentifier.of("default", "test_batch_" + fileFormat.name());

    new IcebergIO.Builder()
        .withSchema(icebergSchema)
        .withTableIdentifier(name)
        .withHiveMetastoreUrl(hiveMetastoreUrl)
        .conf(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name())
        .build(records);

    p.run();
  }

  @Test
  public void testWriteFilesSpecific() {
    for (FileFormat format : FILEFORMATS) {
      runPipelineSpecific(format);
    }
  }

  public void runPipelineSpecific(FileFormat fileFormat) {
    final Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(Cat.class, AvroCoder.of(Cat.SCHEMA$));

    PCollection<Cat> cats = p.apply(Create.of(specificCats));

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(Cat.getClassSchema());
    TableIdentifier name = TableIdentifier.of("default", "test_specific_batch_" + fileFormat.name());

    new IcebergIO.Builder()
        .withSchema(icebergSchema)
        .withTableIdentifier(name)
        .withHiveMetastoreUrl(hiveMetastoreUrl)
        .conf(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name())
        .build(cats);

    p.run();
  }
}
