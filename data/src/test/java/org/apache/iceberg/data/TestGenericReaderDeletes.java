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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;

public class TestGenericReaderDeletes extends DeleteReadTests {

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    return TestTables.create(tableDir, name, schema, spec, 2);
  }

  @Override
  protected void dropTable(String name) {
    TestTables.clearTables();
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  @Override
  protected boolean expectPruned() {
    return false;
  }
}
