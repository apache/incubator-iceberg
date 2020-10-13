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
import java.nio.ByteOrder;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestOverwriteWithValidation extends TableTestBase {

  private static final String TABLE_NAME = "overwrite_table";

  private static final Schema DATE_SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC = PartitionSpec
      .builderFor(DATE_SCHEMA)
      .identity("date")
      .build();

  private static final DataFile FILE_DAY_1 = DataFiles
      .builder(PARTITION_SPEC)
      .withPath("/path/to/data-1.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("date=2018-06-08")
      .withMetrics(new Metrics(5L,
          null, // no column sizes
          ImmutableMap.of(1, 5L, 2, 3L), // value count
          ImmutableMap.of(1, 0L, 2, 2L), // null count
          ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
      ))
      .build();

  private static final DataFile FILE_DAY_2 = DataFiles
      .builder(PARTITION_SPEC)
      .withPath("/path/to/data-2.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("date=2018-06-09")
      .withMetrics(new Metrics(5L,
          null, // no column sizes
          ImmutableMap.of(1, 5L, 2, 3L), // value count
          ImmutableMap.of(1, 0L, 2, 2L), // null count
          ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
      ))
      .build();

  private static final DataFile FILE_DAY_2_MODIFIED = DataFiles
      .builder(PARTITION_SPEC)
      .withPath("/path/to/data-3.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("date=2018-06-09")
      .withMetrics(new Metrics(5L,
          null, // no column sizes
          ImmutableMap.of(1, 5L, 2, 3L), // value count
          ImmutableMap.of(1, 0L, 2, 2L), // null count
          ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
      ))
      .build();

  private static final DataFile FILE_DAY_2_ANOTHER_RANGE = DataFiles
      .builder(PARTITION_SPEC)
      .withPath("/path/to/data-3.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("date=2018-06-09")
      .withMetrics(new Metrics(5L,
          null, // no column sizes
          ImmutableMap.of(1, 5L, 2, 3L), // value count
          ImmutableMap.of(1, 0L, 2, 2L), // null count
          ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
          ImmutableMap.of(1, longToBuffer(14L)) // upper bounds
      ))
      .build();

  private static final Expression EXPRESSION_DAY_2 = equal("date", "2018-06-09");

  private static final Expression EXPRESSION_DAY_2_ID_RANGE = and(
      greaterThanOrEqual("id", 5L),
      lessThanOrEqual("id", 9L));

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestOverwriteWithValidation(int formatVersion) {
    super(formatVersion);
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  private Table table = null;

  @Before
  public void before() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());
    this.table = TestTables.create(tableDir, TABLE_NAME, DATE_SCHEMA, PARTITION_SPEC, formatVersion);
  }

  @Test
  public void testOverwriteEmptyTableNotValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .commit();

    validateTableFiles(table, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteEmptyTableStrictValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, alwaysTrue())
        .commit();

    validateTableFiles(table, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteEmptyTableValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2)
        .commit();

    validateTableFiles(table, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteTableNotValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteTableStrictValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), alwaysTrue())
        .commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteTableValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2)
        .commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteCompatibleAdditionNotValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();

    validateSnapshot(null, table.currentSnapshot(), FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED);

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteCompatibleAdditionStrictValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), alwaysTrue());

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Found conflicting files",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteCompatibleAdditionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteCompatibleDeletionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(FILE_DAY_1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteIncompatibleAdditionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1);

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Found conflicting files",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteIncompatibleDeletionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(FILE_DAY_2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Missing required files to delete:",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteCompatibleRewriteAllowed() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_DAY_2), ImmutableSet.of(FILE_DAY_2))
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    overwrite.commit();

    Assert.assertNotEquals("Should successfully commit", committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteCompatibleExpirationAdditionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteCompatibleExpirationDeletionValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .appendFile(FILE_DAY_2)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(FILE_DAY_1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteIncompatibleExpirationValidated() {
    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit(); // id 2

    table.newDelete()
        .deleteFile(FILE_DAY_1)
        .commit(); // id 3

    table.expireSnapshots()
        .expireSnapshotId(2L)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Cannot determine history",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteIncompatibleBaseExpirationEmptyTableValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit(); // id 1

    table.newDelete()
        .deleteFile(FILE_DAY_1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Cannot determine history",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteAnotherRangeValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2_ID_RANGE);

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testOverwriteAnotherRangeWithinPartitionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    Expression conflictDetectionFilter = and(EXPRESSION_DAY_2, EXPRESSION_DAY_2_ID_RANGE);
    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, conflictDetectionFilter);

    table.newAppend()
        .appendFile(FILE_DAY_2_ANOTHER_RANGE)
        .commit();

    overwrite.commit();

    validateTableFiles(table, FILE_DAY_2_ANOTHER_RANGE, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testTransactionCompatibleAdditionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    Transaction txn = table.newTransaction();
    OverwriteFiles overwrite = txn.newOverwrite()
        .deleteFile(FILE_DAY_2)
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    overwrite.commit();
    txn.commitTransaction();

    validateTableFiles(table, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @Test
  public void testTransactionIncompatibleAdditionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    Transaction txn = table.newTransaction();

    txn.newAppend()
        .appendFile(FILE_DAY_1)
        .commit();

    OverwriteFiles overwrite = txn.newOverwrite()
        .addFile(FILE_DAY_2_MODIFIED)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(FILE_DAY_2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    overwrite.commit();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Found conflicting files",
        txn::commitTransaction);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }
}
