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

package org.apache.iceberg.actions;


import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSortStrategy extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {2}; // We don't actually use the format version since everything is mock
  }

  @Override
  public void setupTable() throws Exception {
    super.setupTable();
    table.replaceSortOrder().asc("data").commit();
  }

  private static final long MB = 1024 * 1024;

  public TestSortStrategy(int formatVersion) {
    super(formatVersion);
  }

  class TestSortStrategyImpl extends SortStrategy {

    @Override
    public Table table() {
      return table;
    }

    @Override
    public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
      throw new UnsupportedOperationException();
    }
  }

  private SortStrategy defaultSort() {
    return (SortStrategy) new TestSortStrategyImpl().options(Collections.emptyMap());
  }

  private List<FileScanTask> tasksForSortOrder(int sortOrderId, int... fileSizesMB) {
    ImmutableList.Builder<FileScanTask> files = ImmutableList.builder();
    IntStream.of(fileSizesMB).forEach(length -> files.add(MockFileScanTask.mockTask(length * MB, sortOrderId)));
    return files.build();
  }

  @Test
  public void testInvalidSortOrder() {
    AssertHelpers.assertThrows("Should not allow an unsorted Sort order", IllegalArgumentException.class,
        () -> defaultSort().sortOrder(SortOrder.unsorted()).options(Collections.emptyMap()));

    AssertHelpers.assertThrows("Should not allow a Sort order with bad columns", ValidationException.class,
        () -> {
          Schema badSchema = new Schema(
              ImmutableList.of(Types.NestedField.required(0, "nonexistant", Types.IntegerType.get())));

          defaultSort()
              .sortOrder(SortOrder.builderFor(badSchema).asc("nonexistant").build())
              .options(Collections.emptyMap());
        });
  }

  @Test
  public void testSelectAll() {
    List<FileScanTask> invalid = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(-1, 500, 500, 500, 500))
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 10, 10, 2000, 10))
        .build();

    List<FileScanTask> expected = ImmutableList.<FileScanTask>builder()
        .addAll(invalid)
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 500, 490, 520))
        .build();

    RewriteStrategy strategy = defaultSort().options(ImmutableMap.of(SortStrategy.REWRITE_ALL, "true"));
    List<FileScanTask> actual = ImmutableList.copyOf(strategy.selectFilesToRewrite(expected));

    Assert.assertEquals("Should mark all files for rewrite",
        expected, actual);
  }

  @Test
  public void testUseSizeOptions() {
    List<FileScanTask> expected = ImmutableList.<FileScanTask>builder()
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 498, 551))
        .build();

    List<FileScanTask> fileScanTasks = ImmutableList.<FileScanTask>builder()
        .addAll(expected)
        .addAll(tasksForSortOrder(table.sortOrder().orderId(), 500, 500))
        .build();

    RewriteStrategy strategy = defaultSort().options(ImmutableMap.of(
        SortStrategy.MAX_FILE_SIZE_BYTES, Long.toString(550 * MB),
        SortStrategy.MIN_FILE_SIZE_BYTES, Long.toString(499 * MB)));

    List<FileScanTask> actual = ImmutableList.copyOf(strategy.selectFilesToRewrite(fileScanTasks));

    Assert.assertEquals("Should mark files for rewrite with adjusted min and max size",
        expected, actual);
  }
}
