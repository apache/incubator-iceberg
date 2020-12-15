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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkBatchQueryScan extends SparkBatchScan {

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;

  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkBatchQueryScan(SparkSession spark, Table table, boolean caseSensitive, Schema expectedSchema,
                      List<Expression> filters, CaseInsensitiveStringMap options) {

    super(spark, table, caseSensitive, expectedSchema, filters, options);

    this.snapshotId = Spark3Util.propertyAsLong(options, SparkReadOptions.SNAPSHOT_ID, null);
    this.asOfTimestamp = Spark3Util.propertyAsLong(options, SparkReadOptions.AS_OF_TIMESTAMP, null);

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = Spark3Util.propertyAsLong(options, "start-snapshot-id", null);
    this.endSnapshotId = Spark3Util.propertyAsLong(options, "end-snapshot-id", null);
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either " +
                SparkReadOptions.SNAPSHOT_ID + " or " + SparkReadOptions.AS_OF_TIMESTAMP + " is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }

    // look for split behavior overrides in options
    this.splitSize = Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, null);
    this.splitLookback = Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, null);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, null);
  }

  @Override
  protected Schema snapshotSchema() {
    if (snapshotId != null && table() instanceof BaseTable) {
      return ((BaseTable) table()).schemaForSnapshot(snapshotId);
    } else if (asOfTimestamp != null && table() instanceof BaseTable) {
      return ((BaseTable) table()).schemaForSnapshotAsOfTime(asOfTimestamp);
    } else {
      return table().schema();
    }
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table()
          .newScan()
          .caseSensitive(caseSensitive())
          .project(expectedSchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
      }

      if (splitSize != null) {
        scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
      }

      if (splitLookback != null) {
        scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
      }

      if (splitOpenFileCost != null) {
        scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
      }

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      }  catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(), snapshotId, startSnapshotId, endSnapshotId,
        asOfTimestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}
