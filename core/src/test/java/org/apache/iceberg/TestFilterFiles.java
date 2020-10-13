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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestFilterFiles {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public final int formatVersion;

  public TestFilterFiles(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final Schema schema = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get()));
  private File tableDir = null;

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testFilterFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFiles(table);
  }

  @Test
  public void testCaseInsensitiveFilterFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testCaseInsensitiveFilterFiles(table);
  }

  @Test
  public void testFilterFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFiles(table);
  }

  @Test
  public void testCaseInsensitiveFilterFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testCaseInsensitiveFilterFiles(table);
  }

  private void testFilterFiles(Table table) {
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    lowerBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));

    Metrics metrics = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds, upperBounds);

    DataFile file = DataFiles.builder(table.spec())
        .withPath("/path/to/file.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics)
        .build();

    table.newAppend().appendFile(file).commit();

    table.refresh();

    TableScan emptyScan = table.newScan().filter(Expressions.equal("id", 5));
    assertEquals(0, Iterables.size(emptyScan.planFiles()));

    TableScan nonEmptyScan = table.newScan().filter(Expressions.equal("id", 1));
    assertEquals(1, Iterables.size(nonEmptyScan.planFiles()));
  }

  private void testCaseInsensitiveFilterFiles(Table table) {
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    lowerBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));

    Metrics metrics = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds, upperBounds);

    DataFile file = DataFiles.builder(table.spec())
        .withPath("/path/to/file.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics)
        .build();

    table.newAppend().appendFile(file).commit();

    table.refresh();

    TableScan emptyScan = table.newScan().caseSensitive(false).filter(Expressions.equal("ID", 5));
    assertEquals(0, Iterables.size(emptyScan.planFiles()));

    TableScan nonEmptyScan = table.newScan().caseSensitive(false).filter(Expressions.equal("ID", 1));
    assertEquals(1, Iterables.size(nonEmptyScan.planFiles()));
  }
}
