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
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSplitScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final long SPLIT_SIZE = 16 * 1024 * 1024;

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
  );

  private Table table;
  private File tableLocation;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private List<Record> expectedRecords;

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[] parameters() {
    return new Object[] { "parquet", "avro" };
  }

  private final FileFormat format;

  public TestSplitScan(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws IOException {
    tableLocation = new File(temp.newFolder(), "table");
    setupTable();
  }

  @Test
  public void test() {
    Assert.assertEquals(
        "There should be 4 tasks created since file size is approximately close to 64MB and split size 16MB",
        4, Lists.newArrayList(table.newScan().planTasks()).size());
    List<Record> records = Lists.newArrayList(IcebergGenerics.read(table).build());
    Assert.assertEquals(expectedRecords.size(), records.size());
    for (int i = 0; i < expectedRecords.size(); i++) {
      Assert.assertEquals(expectedRecords.get(i), records.get(i));
    }
  }

  private void setupTable() throws IOException {
    table = TABLES.create(SCHEMA, tableLocation.toString());
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(SPLIT_SIZE))
        .commit();

    // With these number of records and the given SCHEMA
    // we can effectively write a file of approximate size 64 MB
    int numRecords = 2500000;
    expectedRecords = RandomGenericData.generate(SCHEMA, numRecords, 0L);
    File file = writeToFile(expectedRecords, format);

    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(expectedRecords.size())
        .withFileSizeInBytes(file.length())
        .withPath(file.toString())
        .withFormat(format)
        .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private File writeToFile(List<Record> records, FileFormat fileFormat) throws IOException {
    File file = temp.newFile();
    Assert.assertTrue(file.delete());

    GenericAppenderFactory factory = new GenericAppenderFactory(table).set(
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(SPLIT_SIZE));
    try (FileAppender<Record> appender = factory.newAppender(Files.localOutput(file), fileFormat)) {
      appender.addAll(records);
    }
    return file;
  }
}
