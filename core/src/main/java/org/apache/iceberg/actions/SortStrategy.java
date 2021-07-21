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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rewrite strategy for data files which aims to reorder data with data files to optimally lay them out
 * in relation to a column. For example, if the Sort strategy is used on a set of files which is ordered
 * by column x and original has files File A (x: 0 - 50), File B ( x: 10 - 40) and File C ( x: 30 - 60),
 * this Strategy will attempt to rewrite those files into File A' (x: 0-20), File B' (x: 21 - 40),
 * File C' (x: 41 - 60).
 * <p>
 * Currently the there is no clustering detection and we will rewrite all files if {@link SortStrategy#REWRITE_ALL}
 * is true (default: false). If this property is disabled any files that would be chosen by
 * {@link BinPackStrategy} will be rewrite candidates.
 * <p>
 * In the future other algorithms for determining files to rewrite will be provided.
 */
public abstract class SortStrategy extends BinPackStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(SortStrategy.class);

  /**
   * Rewrites all files, regardless of their size. Defaults to false, rewriting only mis-sized
   * files;
   */
  public static final String REWRITE_ALL = "rewrite-all";
  public static final boolean REWRITE_ALL_DEFAULT = false;


  private static final Set<String> validOptions = ImmutableSet.of(
      REWRITE_ALL
  );

  private boolean rewriteAll;
  private SortOrder sortOrder;

  /**
   * Sets the sort order to be used in this strategy when rewriting files
   * @param order the order to use
   * @return this for method chaining
   */
  public SortStrategy sortOrder(SortOrder order) {
    this.sortOrder = order;
    return this;
  }

  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  public String name() {
    return "SORT";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .addAll(validOptions)
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    super.options(options); // Also checks validity of BinPack options

    rewriteAll = PropertyUtil.propertyAsBoolean(options,
        REWRITE_ALL,
        REWRITE_ALL_DEFAULT);

    if (sortOrder == null) {
      sortOrder = table().sortOrder();
    }

    validateOptions();
    return this;
  }

  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    if (rewriteAll) {
      LOG.info("Sort Strategy for table {} set to rewrite all data files", table().name());
      return dataFiles;
    } else {
      return super.selectFilesToRewrite(dataFiles);
    }
  }

  protected void validateOptions() {
    Preconditions.checkArgument(!sortOrder.isUnsorted(),
        "Can't use %s when there is no sort order, either define table %s's sort order or set sort" +
            "order in the action",
        name(), table().name());

    SortOrder.checkCompatibility(sortOrder, table().schema());
  }
}
