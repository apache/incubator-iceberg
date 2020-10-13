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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPartitionSpecInfo {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final Schema schema = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get()));
  private File tableDir = null;

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  private final int formatVersion;

  public TestPartitionSpecInfo(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testSpecInfoUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());
    Assert.assertEquals(spec.lastAssignedFieldId(), table.spec().lastAssignedFieldId());
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }

  @Test
  public void testSpecInfoPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());
    Assert.assertEquals(spec.lastAssignedFieldId(), table.spec().lastAssignedFieldId());
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }

  @Test
  public void testSpecInfoPartitionSpecEvolutionForV1Table() {
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 4)
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());

    TableMetadata base = TestTables.readMetadata("test");
    PartitionSpec newSpec = PartitionSpec.builderFor(table.schema())
        .bucket("data", 10)
        .withSpecId(1)
        .build();
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    Assert.assertEquals(newSpec, table.spec());
    Assert.assertEquals(newSpec, table.specs().get(newSpec.specId()));
    Assert.assertEquals(spec, table.specs().get(spec.specId()));
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec, newSpec.specId(), newSpec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }
}
