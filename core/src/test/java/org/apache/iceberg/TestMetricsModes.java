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

import java.util.Map;
import org.apache.iceberg.MetricsModes.Counts;
import org.apache.iceberg.MetricsModes.Full;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.MetricsModes.Truncate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestMetricsModes {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testMetricsModeParsing() {
    Assert.assertEquals(None.get(), MetricsModes.fromString("none"));
    Assert.assertEquals(None.get(), MetricsModes.fromString("nOnE"));
    Assert.assertEquals(Counts.get(), MetricsModes.fromString("counts"));
    Assert.assertEquals(Counts.get(), MetricsModes.fromString("coUntS"));
    Assert.assertEquals(Truncate.withLength(1), MetricsModes.fromString("truncate(1)"));
    Assert.assertEquals(Truncate.withLength(10), MetricsModes.fromString("truNcAte(10)"));
    Assert.assertEquals(Full.get(), MetricsModes.fromString("full"));
    Assert.assertEquals(Full.get(), MetricsModes.fromString("FULL"));
  }

  @Test
  public void testInvalidTruncationLength() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("length should be positive");
    MetricsModes.fromString("truncate(0)");
  }

  @Test
  public void testInvalidColumnModeValue() {
    Map<String, String> properties = ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE, "full",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col", "troncate(5)");

    MetricsConfig config = MetricsConfig.fromProperties(properties);
    Assert.assertEquals("Invalid mode should be defaulted to table default (full)",
        MetricsModes.Full.get(), config.columnMode("col"));
  }

  @Test
  public void testInvalidDefaultColumnModeValue() {
    Map<String, String> properties = ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE, "fuull",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col", "troncate(5)");

    MetricsConfig config = MetricsConfig.fromProperties(properties);
    Assert.assertEquals("Invalid mode should be defaulted to library default (truncate(16))",
        MetricsModes.Truncate.withLength(16), config.columnMode("col"));
  }

  @Test
  public void testMetricsConfigSortedColsDefault() {
    Map<String, String> properties = ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "counts",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "none");

    Schema schema = new Schema(
        required(1, "col1", Types.IntegerType.get()),
        required(2, "col2", Types.IntegerType.get()),
        required(3, "col3", Types.IntegerType.get()),
        required(4, "col4", Types.IntegerType.get())
    );
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();

    Table testTable = TestMetricUtil.createTestTable(properties, sortOrder, schema);

    MetricsConfig config = MetricsConfig.fromTable(testTable);
    Assert.assertEquals("Non-sorted existing column should not be overridden",
        Counts.get(), config.columnMode("col1"));
    Assert.assertEquals("Sorted column defaults should not override user specified config",
        None.get(), config.columnMode("col2"));
    Assert.assertEquals("Unspecified sorted column should use default",
        Truncate.withLength(16), config.columnMode("col3"));
    Assert.assertEquals("Unspecified normal column should use default",
        Counts.get(), config.columnMode("col4"));
  }

  @Test
  public void testMetricsConfigSortedColsDefaultByInvalid() {
    Map<String, String> properties = ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "full",
        TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "invalid");

    Schema schema = new Schema(
        required(1, "col1", Types.IntegerType.get()),
        required(2, "col2", Types.IntegerType.get()),
        required(3, "col3", Types.IntegerType.get())
    );
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();

    Table testTable = TestMetricUtil.createTestTable(properties, sortOrder, schema);

    MetricsConfig config = MetricsConfig.fromTable(testTable);
    Assert.assertEquals("Non-sorted existing column should not be overridden",
        Full.get(), config.columnMode("col1"));
    Assert.assertEquals("Sorted column defaults applies as user entered invalid mode",
        Truncate.withLength(16), config.columnMode("col2"));
  }
}
