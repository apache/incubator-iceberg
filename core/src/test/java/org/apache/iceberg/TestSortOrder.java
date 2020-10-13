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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSortOrder {

  // column ids will be reassigned during table creation
  private static final Schema SCHEMA = new Schema(
      required(10, "id", Types.IntegerType.get()),
      required(11, "data", Types.StringType.get()),
      optional(12, "s", Types.StructType.of(
          required(17, "id", Types.IntegerType.get()),
          optional(18, "b", Types.ListType.ofOptional(3, Types.StructType.of(
              optional(19, "i", Types.IntegerType.get()),
              optional(20, "s", Types.StringType.get())
          )))
      )),
      required(30, "ext", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  private final int formatVersion;

  public TestSortOrder(int formatVersion) {
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
  public void testSortOrderBuilder() {
    Assert.assertEquals("Should be able to build unsorted order",
        SortOrder.unsorted(),
        SortOrder.builderFor(SCHEMA).withOrderId(0).build());

    AssertHelpers.assertThrows("Should not allow sort orders ID 0",
        IllegalArgumentException.class, "order ID 0 is reserved for unsorted order",
        () -> SortOrder.builderFor(SCHEMA).asc("data").withOrderId(0).build());
    AssertHelpers.assertThrows("Should not allow unsorted orders with arbitrary IDs",
        IllegalArgumentException.class, "order ID must be 0",
        () -> SortOrder.builderFor(SCHEMA).withOrderId(1).build());
  }

  @Test
  public void testDefaultOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, formatVersion);
    Assert.assertEquals("Expected 1 sort order", 1, table.sortOrders().size());

    SortOrder actualOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 0, actualOrder.orderId());
    Assert.assertTrue("Order must unsorted", actualOrder.isUnsorted());
  }

  @Test
  public void testFreshIds() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .withSpecId(5)
        .identity("data")
        .build();
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("s.id", NULLS_LAST)
        .desc(truncate("data", 10), NULLS_FIRST)
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    Assert.assertEquals("Expected 1 sort order", 1, table.sortOrders().size());
    Assert.assertTrue("Order ID must be fresh", table.sortOrders().containsKey(TableMetadata.INITIAL_SORT_ORDER_ID));

    SortOrder actualOrder = table.sortOrder();
    Assert.assertEquals("Order ID must be fresh", TableMetadata.INITIAL_SORT_ORDER_ID, actualOrder.orderId());
    Assert.assertEquals("Order must have 2 fields", 2, actualOrder.fields().size());
    Assert.assertEquals("Field id must be fresh", 5, actualOrder.fields().get(0).sourceId());
    Assert.assertEquals("Field id must be fresh", 2, actualOrder.fields().get(1).sourceId());
  }

  @Test
  public void testCompatibleOrders() {
    SortOrder order1 = SortOrder.builderFor(SCHEMA)
        .withOrderId(9)
        .asc("s.id", NULLS_LAST)
        .build();

    SortOrder order2 = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("s.id", NULLS_LAST)
        .desc(truncate("data", 10), NULLS_FIRST)
        .build();

    SortOrder order3 = SortOrder.builderFor(SCHEMA)
        .withOrderId(11)
        .asc("s.id", NULLS_LAST)
        .desc(truncate("data", 10), NULLS_LAST)
        .build();

    SortOrder order4 = SortOrder.builderFor(SCHEMA)
        .withOrderId(11)
        .asc("s.id", NULLS_LAST)
        .asc(truncate("data", 10), NULLS_FIRST)
        .build();

    SortOrder order5 = SortOrder.builderFor(SCHEMA)
        .withOrderId(11)
        .desc("s.id", NULLS_LAST)
        .build();

    // an unsorted order satisfies only itself
    Assert.assertTrue(SortOrder.unsorted().satisfies(SortOrder.unsorted()));
    Assert.assertFalse(SortOrder.unsorted().satisfies(order1));
    Assert.assertFalse(SortOrder.unsorted().satisfies(order2));
    Assert.assertFalse(SortOrder.unsorted().satisfies(order3));
    Assert.assertFalse(SortOrder.unsorted().satisfies(order4));
    Assert.assertFalse(SortOrder.unsorted().satisfies(order5));

    // any ordering satisfies an unsorted ordering
    Assert.assertTrue(order1.satisfies(SortOrder.unsorted()));
    Assert.assertTrue(order2.satisfies(SortOrder.unsorted()));
    Assert.assertTrue(order3.satisfies(SortOrder.unsorted()));
    Assert.assertTrue(order4.satisfies(SortOrder.unsorted()));
    Assert.assertTrue(order5.satisfies(SortOrder.unsorted()));

    // order1 has the same fields but different sort direction compared to order5
    Assert.assertFalse(order1.satisfies(order5));

    // order2 has more fields than order1 and is compatible
    Assert.assertTrue(order2.satisfies(order1));
    // order2 has more fields than order5 but is incompatible
    Assert.assertFalse(order2.satisfies(order5));
    // order2 has the same fields but different null order compared to order3
    Assert.assertFalse(order2.satisfies(order3));
    // order2 has the same fields but different sort direction compared to order4
    Assert.assertFalse(order2.satisfies(order4));

    // order1 has fewer fields than order2 and is incompatible
    Assert.assertFalse(order1.satisfies(order2));
  }

  @Test
  public void testSameOrder() {
    SortOrder order1 = SortOrder.builderFor(SCHEMA)
        .withOrderId(9)
        .asc("s.id", NULLS_LAST)
        .build();

    SortOrder order2 = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("s.id", NULLS_LAST)
        .build();

    // orders have different ids but are logically the same
    Assert.assertNotEquals("Orders must not be equal", order1, order2);
    Assert.assertTrue("Orders must be equivalent", order1.sameOrder(order2));
    Assert.assertTrue("Orders must be equivalent", order2.sameOrder(order1));
  }

  @Test
  public void testSchemaEvolutionWithSortOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("s.id")
        .desc(truncate("data", 10))
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    table.updateSchema()
        .renameColumn("s.id", "s.id2")
        .commit();

    SortOrder actualOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", TableMetadata.INITIAL_SORT_ORDER_ID, actualOrder.orderId());
    Assert.assertEquals("Order must have 2 fields", 2, actualOrder.fields().size());
    Assert.assertEquals("Field id must match", 5, actualOrder.fields().get(0).sourceId());
    Assert.assertEquals("Field id must match", 2, actualOrder.fields().get(1).sourceId());
  }

  @Test
  public void testIncompatibleSchemaEvolutionWithSortOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("s.id")
        .desc(truncate("data", 10))
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    AssertHelpers.assertThrows("Should reject deletion of sort columns",
        ValidationException.class, "Cannot find source column",
        () -> table.updateSchema().deleteColumn("s.id").commit());
  }
}
