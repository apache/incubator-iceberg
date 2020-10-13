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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.RandomAvroData;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestScansAndSchemaEvolution {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      required(2, "data", Types.StringType.get()),
      required(3, "part", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("part")
      .build();

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public final int formatVersion;

  public TestScansAndSchemaEvolution(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private DataFile createDataFile(File dataPath, String partValue) throws IOException {
    List<GenericData.Record> expected = RandomAvroData.generate(SCHEMA, 100, 0L);

    File dataFile = new File(dataPath, FileFormat.AVRO.addExtension(UUID.randomUUID().toString()));
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(dataFile))
        .schema(SCHEMA)
        .named("test")
        .build()) {
      for (GenericData.Record rec : expected) {
        rec.put("part", partValue); // create just one partition
        writer.add(rec);
      }
    }

    PartitionData partition = new PartitionData(SPEC.partitionType());
    partition.set(0, partValue);
    return DataFiles.builder(SPEC)
        .withInputFile(Files.localInput(dataFile))
        .withPartition(partition)
        .withRecordCount(100)
        .build();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testPartitionSourceRename() throws IOException {
    File location = temp.newFolder();
    File dataLocation = new File(location, "data");
    Assert.assertTrue(location.delete()); // should be created by table create

    Table table = TestTables.create(location, "test", SCHEMA, SPEC, formatVersion);

    DataFile fileOne = createDataFile(dataLocation, "one");
    DataFile fileTwo = createDataFile(dataLocation, "two");

    table.newAppend()
        .appendFile(fileOne)
        .appendFile(fileTwo)
        .commit();

    List<FileScanTask> tasks = Lists.newArrayList(
        table.newScan().filter(Expressions.equal("part", "one")).planFiles());

    Assert.assertEquals("Should produce 1 matching file task", 1, tasks.size());

    table.updateSchema()
        .renameColumn("part", "p")
        .commit();

    // plan the scan using the new name in a filter
    tasks = Lists.newArrayList(
        table.newScan().filter(Expressions.equal("p", "one")).planFiles());

    Assert.assertEquals("Should produce 1 matching file task", 1, tasks.size());
  }
}
