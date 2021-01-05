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

package org.apache.spark.sql.connector.iceberg.read;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A mix-in interface for Scan. Data sources can implement this interface if they support dynamic
 * file filters.
 */
public interface SupportsFileFilter {
  /**
   * Filters this scan to query only selected files.
   *
   * @param files file locations
   */
  void filterFiles(FilterFiles files);

  final class FilterFiles {
    private final List<Consumer<Set<String>>> filterActions = Lists.newArrayList();

    public void setFilteredLocations(Set<String> locations) {
      for (Consumer<Set<String>> action : filterActions) {
        action.accept(locations);
      }
    }

    public void onLocationsFilter(Consumer<Set<String>> filterAction) {
      this.filterActions.add(filterAction);
    }
  }
}
