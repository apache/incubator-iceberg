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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;

@RunWith(Parameterized.class)
public class TestUpdatePartitionSpec extends TableTestBase {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "category", Types.StringType.get()),
      Types.NestedField.optional(4, "data", Types.StringType.get())
  );

  private static final PartitionSpec UNPARTITIONED = PartitionSpec.builderFor(SCHEMA).build();
  private static final PartitionSpec PARTITIONED = PartitionSpec.builderFor(SCHEMA)
      .identity("category")
      .day("ts")
      .bucket("id", 16, "shard")
      .build();

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestUpdatePartitionSpec(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAddIdentityByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField("category")
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddIdentityByTerm() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(ref("category"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddYear() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(year("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .year("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddMonth() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(month("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .month("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddDay() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(day("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddHour() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(hour("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .hour("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddBucket() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(bucket("id", 16))
        .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "id_bucket_16")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddTruncate() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(truncate("data", 4))
        .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .truncate("data", 4, "data_trunc_4")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddNamedPartition() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField("shard", bucket("id", 16))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "shard")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddToExisting() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .addField(truncate("data", 4))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16, "shard")
        .truncate("data", 4, "data_trunc_4")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testMultipleAdds() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField("category")
        .addField(day("ts"))
        .addField("shard", bucket("id", 16))
        .addField("prefix", truncate("data", 4))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16, "shard")
        .truncate("data", 4, "prefix")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddHourToDay() {
    // multiple partitions for the same source with different time granularity is not allowed by the builder, but is
    // allowed when updating a spec so that existing columns in metadata continue to work.
    PartitionSpec byDay = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(day("ts"))
        .apply();

    PartitionSpec byHour = new BaseUpdatePartitionSpec(formatVersion, byDay)
        .addField(hour("ts"))
        .apply();

    Assert.assertEquals("Should have a day and an hour time field",
        ImmutableList.of(
            new PartitionField(2, 1000, "ts_day", Transforms.day(Types.TimestampType.withZone())),
            new PartitionField(2, 1001, "ts_hour", Transforms.hour(Types.TimestampType.withZone()))),
        byHour.fields());
  }

  @Test
  public void testAddMultipleBuckets() {
    PartitionSpec bucket16 = new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
        .addField(bucket("id", 16))
        .apply();

    PartitionSpec bucket8 = new BaseUpdatePartitionSpec(formatVersion, bucket16)
        .addField(bucket("id", 8))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "id_bucket_16")
        .bucket("id", 8, "id_bucket_8")
        .build();

    Assert.assertEquals("Should have a day and an hour time field", expected, bucket8);
  }

  @Test
  public void testRemoveIdentityByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField("category")
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .alwaysNull("category", "category")
        .day("ts")
        .bucket("id", 16, "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRemoveBucketByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField("shard")
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .alwaysNull("id", "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRemoveIdentityByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField(ref("category"))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .alwaysNull("category", "category")
        .day("ts")
        .bucket("id", 16, "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRemoveDayByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField(day("ts"))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .alwaysNull("ts", "ts_day")
        .bucket("id", 16, "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRemoveBucketByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField(bucket("id", 16))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .alwaysNull("id", "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRename() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .renameField("shard", "id_bucket") // rename back to default
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16)
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testMultipleChanges() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .renameField("shard", "id_bucket") // rename back to default
        .removeField(day("ts"))
        .addField("prefix", truncate("data", 4))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .alwaysNull("ts", "ts_day")
        .bucket("id", 16)
        .truncate("data", 4, "prefix")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("id"), 1002, "id_bucket", Transforms.bucket(Types.LongType.get(), 16))
        .add(id("data"), 1003, "prefix", Transforms.truncate(Types.StringType.get(), 4))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testAddDeletedName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField(bucket("id", 16))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .alwaysNull("id", "shard")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testAddDeletedSameSourceMultipleTimes() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
        .removeField(bucket("id", 16))
        .apply();

    updated = new BaseUpdatePartitionSpec(formatVersion, updated, 1002)
        .addField(bucket("id", 8))
        .apply();
    updated = new BaseUpdatePartitionSpec(formatVersion, updated, 1002)
        .removeField(bucket("id", 8))
        .apply();
    updated = new BaseUpdatePartitionSpec(formatVersion, updated, 1003)
        .addField(bucket("id", 6))
        .apply();

    PartitionSpec v1Expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .alwaysNull("id", "shard")
        .alwaysNull("id", "id_bucket_8")
        .bucket("id", 6, "id_bucket_6")
        .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .add(id("id"), 1004, "id_bucket_6", Transforms.bucket(Types.LongType.get(), 6))
        .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @Test
  public void testRemoveNewlyAddedFieldByName() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot delete newly added field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("prefix", truncate("data", 4))
            .removeField("prefix")
    );
  }

  @Test
  public void testRemoveNewlyAddedFieldByTransform() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot delete newly added field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("prefix", truncate("data", 4))
            .removeField(truncate("data", 4)));
  }

  @Test
  public void testAddAlreadyAddedFieldByTransform() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("prefix", truncate("data", 4))
            .addField(truncate("data", 4)));
  }

  @Test
  public void testAddAlreadyAddedFieldByName() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("prefix", truncate("data", 4))
            .addField("prefix", truncate("data", 6)));
  }

  @Test
  public void testAddRedundantTimePartition() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add redundant partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(day("ts"))
            .addField(hour("ts"))); // conflicts with hour

    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField(hour("ts")) // does not conflict with day because day already exists
            .addField(month("ts"))); // conflicts with hour
  }

  @Test
  public void testAddDeletedField() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField("shard")
            .addField(bucket("id", 16))); // duplicates shard
  }

  @Test
  public void testAddDuplicateByName() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("category"));
  }

  @Test
  public void testAddDuplicateByRef() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField(ref("category")));
  }

  @Test
  public void testAddDuplicateTransform() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField(bucket("id", 16)));
  }

  @Test
  public void testAddNamedDuplicate() {
    AssertHelpers.assertThrows("Should fail adding a duplicate field",
        IllegalArgumentException.class, "Cannot add duplicate partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("b16", bucket("id", 16)));
  }

  @Test
  public void testRemoveUnknownFieldByName() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to remove",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField("moon")
    );
  }

  @Test
  public void testRemoveUnknownFieldByEquivalent() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to remove",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField(hour("ts")) // day(ts) exists
    );
  }

  @Test
  public void testRenameUnknownField() {
    AssertHelpers.assertThrows("Should fail trying to rename an unknown field",
        IllegalArgumentException.class, "Cannot find partition field to rename",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).renameField("shake", "seal")
    );
  }

  @Test
  public void testRenameAfterAdd() {
    AssertHelpers.assertThrows("Should fail trying to rename an added field",
        IllegalArgumentException.class, "Cannot rename newly added partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField("data_trunc", truncate("data", 4))
            .renameField("data_trunc", "prefix")
    );
  }

  @Test
  public void testDeleteAndRename() {
    AssertHelpers.assertThrows("Should fail trying to rename a deleted field",
        IllegalArgumentException.class, "Cannot rename and delete partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .renameField("shard", "id_bucket")
            .removeField(bucket("id", 16)));
  }

  @Test
  public void testRenameAndDelete() {
    AssertHelpers.assertThrows("Should fail trying to delete a renamed field",
        IllegalArgumentException.class, "Cannot delete and rename partition field",
        () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField(bucket("id", 16))
            .renameField("shard", "id_bucket"));
  }

  private static int id(String name) {
    return SCHEMA.findField(name).fieldId();
  }
}
