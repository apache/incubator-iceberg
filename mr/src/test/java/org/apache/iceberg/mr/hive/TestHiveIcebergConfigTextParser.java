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

package org.apache.iceberg.mr.hive;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergConfigTextParser {

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "name", Types.StringType.get()),
      optional(3, "employee_info", Types.StructType.of(
          optional(6, "employer", Types.StringType.get()),
          optional(7, "id", Types.LongType.get()),
          optional(8, "address", Types.StringType.get())
      )),
      optional(4, "created", Types.TimestampType.withoutZone()),
      optional(5, "updated", Types.TimestampType.withoutZone())
  );

  @Test
  public void testInvalidInputText() {
    AssertHelpers.assertThrows("should fail when input exceeding max allowed length",
        IllegalArgumentException.class,
        "Partition spec text too long: max allowed length 1000, but got 1024",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, new String(new char[1024]).replace("\0", "s")));

    AssertHelpers.assertThrows("should fail with multiple left brackets",
        IllegalArgumentException.class,
        "found multiple (",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(col(col2))"));

    AssertHelpers.assertThrows("should fail with multiple right brackets",
        IllegalArgumentException.class,
        "end of bracket not followed by ' ' or '|'",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(16,id))"));

    AssertHelpers.assertThrows("should fail with no delimiter between transforms",
        IllegalArgumentException.class,
        "end of bracket not followed by ' ' or '|'",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(16,id)bucket(16,col)"));

    AssertHelpers.assertThrows("should fail with no delimiter between transform and column",
        IllegalArgumentException.class,
        "end of bracket not followed by ' ' or '|'",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(16,id)name"));

    AssertHelpers.assertThrows("should fail with more than 2 transform arguments",
        IllegalArgumentException.class,
        "more than 2 transform arguments used",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(1,2,3)"));

    AssertHelpers.assertThrows("should fail with wrong delimiter",
        IllegalArgumentException.class,
        "detected multiple args for identity transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "name,id"));
  }

  @Test
  public void testParsingColumns() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("id").identity("name").identity("created").build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "id|name|created"));
    Assert.assertEquals("space on the right of pipe should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "id| name|created"));
    Assert.assertEquals("space on the left of pipe should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "id |name|created"));
    Assert.assertEquals("space on both sides of pipe should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "id | name|created"));
  }

  @Test
  public void testParsingColumnsFailure() {
    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot find source column: col",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "col"));
  }

  @Test
  public void testParsing2ArgTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("name", 16).truncate("employee_info.address", 8).build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "bucket(16,name)|truncate(employee_info.address,8)"));
    Assert.assertEquals("space in args should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(
            SCHEMA, "bucket( 16, name)| truncate(employee_info.address , 8 )"));
  }

  @Test
  public void testParsing2ArgTransformsFailure() {
    AssertHelpers.assertThrows("should fail when transform not exist",
        IllegalArgumentException.class,
        "unknown 2-arg transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "unknown(16,col)"));

    AssertHelpers.assertThrows("should fail when there is only 1 arg",
        IllegalArgumentException.class,
        "unknown 1-arg transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(a)"));

    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot find source column",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(8, col)"));

    AssertHelpers.assertThrows("should fail when input to transform is wrong",
        IllegalArgumentException.class,
        "fail to parse integer argument in 2-arg transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "bucket(name, 8)"));
  }

  @Test
  public void testParsing1ArgTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("created").hour("updated").alwaysNull("id").build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "day(created)|hour(updated)|alwaysNull(id)"));
    Assert.assertEquals("space in args should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(
            SCHEMA, "day(created )| hour( updated)| alwaysNull(id)"));
    Assert.assertEquals("case sensitivity should not matter",
        expected, HiveIcebergConfigTextParser.fromPartitioningText(
            SCHEMA, "day(created)|hour(updated)|alwaysnull(id)"));
  }

  @Test
  public void testParsing1ArgTransformsFailure() {
    AssertHelpers.assertThrows("should fail when transform not exist",
        IllegalArgumentException.class,
        "unknown 1-arg transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "unknown(col)"));

    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot find source column: col",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "year(col)"));

    AssertHelpers.assertThrows("should treat additional arguments as plain text column and fail",
        IllegalArgumentException.class,
        "unknown 2-arg transform",
        () -> HiveIcebergConfigTextParser.fromPartitioningText(SCHEMA, "year(8,col)"));
  }

  @Test
  public void textMixedTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("created")
        .bucket("id", 16)
        .identity("employee_info.employer")
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "day(created)|bucket(16,id)|employee_info.employer"));

    expected = PartitionSpec.builderFor(SCHEMA)
        .day("created")
        .identity("employee_info.employer")
        .bucket("id", 16)
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "day(created)|employee_info.employer|bucket(16,id)"));

    expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16)
        .identity("employee_info.employer")
        .day("created")
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "bucket(16,id)|employee_info.employer|day(created)"));

    expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16)
        .day("created")
        .identity("employee_info.employer")
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "bucket(16,id)|day(created)|employee_info.employer"));

    expected = PartitionSpec.builderFor(SCHEMA)
        .identity("employee_info.employer")
        .day("created")
        .bucket("id", 16)
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "employee_info.employer|day(created)|bucket(16,id)"));

    expected = PartitionSpec.builderFor(SCHEMA)
        .identity("employee_info.employer")
        .bucket("id", 16)
        .day("created")
        .build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        SCHEMA, "employee_info.employer|bucket(16,id)|day(created)"));
  }

  @Test
  public void testEscapedColumns() {
    Schema schema = new Schema(
        optional(1, "i|d", Types.LongType.get()),
        optional(2, "na,me", Types.StringType.get()),
        optional(3, "cate,gory", Types.StringType.get()),
        optional(4, "da|ta", Types.StringType.get()));
    PartitionSpec expected = PartitionSpec.builderFor(schema)
        .bucket("i|d", 16).alwaysNull("na,me")
        .truncate("cate,gory", 16).identity("da|ta").build();
    Assert.assertEquals(expected, HiveIcebergConfigTextParser.fromPartitioningText(
        schema, "bucket(16,`i|d`)|alwaysNull(`na,me`)|truncate(`cate,gory`,16)|`da|ta`"));
  }
}
