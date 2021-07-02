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

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SortOrderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;

public class MetricsConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);

  private Map<String, MetricsMode> columnModes = Maps.newHashMap();
  private MetricsMode defaultMode;

  private MetricsConfig() {
  }

  public static MetricsConfig getDefault() {
    MetricsConfig spec = new MetricsConfig();
    spec.defaultMode = MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    return spec;
  }

  static Map<String, String> updateProperties(Map<String, String> props, List<String> deletedColumns,
                                              Map<String, String> renamedColumns) {
    if (props.keySet().stream().noneMatch(key -> key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX))) {
      return props;
    } else {
      Map<String, String> updatedProperties = Maps.newHashMap();
      // Put all of the non metrics columns we aren't modifying
      props.keySet().forEach(key -> {
        if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
          String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
          if (renamedColumns.get(columnAlias) != null) {
            // The name has changed.
            String newKey = METRICS_MODE_COLUMN_CONF_PREFIX + renamedColumns.get(columnAlias);
            updatedProperties.put(newKey, props.get(key));
          } else if (!deletedColumns.contains(columnAlias)) {
            // Copy over the original
            updatedProperties.put(key, props.get(key));
          }
          // Implicit drop if deleted
        } else {
          // Not a metric property
          updatedProperties.put(key, props.get(key));
        }
      });
      return updatedProperties;
    }
  }

  public static MetricsConfig fromProperties(Map<String, String> props) {
    return from(props, null);
  }

  public static MetricsConfig fromTable(Table table) {
    return from(table.properties(), table.sortOrder());
  }

  private static MetricsConfig from(Map<String, String> props, SortOrder order) {
    MetricsConfig spec = new MetricsConfig();
    String defaultModeAsString = props.getOrDefault(DEFAULT_WRITE_METRICS_MODE, DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    try {
      spec.defaultMode = MetricsModes.fromString(defaultModeAsString);
    } catch (IllegalArgumentException err) {
      // Mode was invalid, log the error and use the default
      LOG.warn("Ignoring invalid default metrics mode: {}", defaultModeAsString, err);
      spec.defaultMode = MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    }

    // First set sorted column with sorted column default (can be overridden by user)
    final Set<String> sortedCols = new HashSet<>();
    MetricsMode sortedColDefaultMode = promoteSortedColumnDefault(spec.defaultMode);
    if (order != null) {
      sortedCols.addAll(SortOrderUtil.getSortedColumns(order));
      sortedCols.stream().forEach(sc -> {
        if (!props.containsKey(METRICS_MODE_COLUMN_CONF_PREFIX + sc)) {
          spec.columnModes.put(sc, sortedColDefaultMode);
        }
      });
    }

    props.keySet().stream()
        .filter(key -> key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX))
        .forEach(key -> {
          String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
          MetricsMode mode;
          try {
            mode = MetricsModes.fromString(props.get(key));
          } catch (IllegalArgumentException err) {
            // Mode was invalid, log the error and use the default (or default for sorted columns)
            LOG.warn("Ignoring invalid metrics mode for column {}: {}", columnAlias, props.get(key), err);
            if (sortedCols.contains(columnAlias)) {
              mode = sortedColDefaultMode;
            } else {
              mode = spec.defaultMode;
            }
          }
          spec.columnModes.put(columnAlias, mode);
        });
    return spec;
  }

  /**
   * Auto promote sorted columns to truncate(16) if default is set at Counts or None.
   * @param defaultMode default mode
   * @return mode to use
   */
  private static MetricsMode promoteSortedColumnDefault(MetricsMode defaultMode) {
    if (defaultMode == MetricsModes.None.get() || defaultMode == MetricsModes.Counts.get()) {
      return MetricsModes.Truncate.withLength(16);
    } else {
      return defaultMode;
    }
  }

  public void validateReferencedColumns(Schema schema) {
    for (String column : columnModes.keySet()) {
      ValidationException.check(
          schema.findField(column) != null,
          "Invalid metrics config, could not find column %s from table prop %s in schema %s",
          column, METRICS_MODE_COLUMN_CONF_PREFIX + column, schema);
    }
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }
}
