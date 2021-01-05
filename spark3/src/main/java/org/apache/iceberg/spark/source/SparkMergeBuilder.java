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

import java.util.Locale;
import java.util.Map;
import org.apache.arrow.util.Preconditions;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter;
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL_DEFAULT;

class SparkMergeBuilder implements MergeBuilder, SupportsFileFilter {

  private final SparkSession spark;
  private final Table table;
  private final LogicalWriteInfo writeInfo;
  private final IsolationLevel isolationLevel;

  // lazy vars
  private SparkScanBuilder lazyScanBuilder;
  private Scan configuredScan;
  private WriteBuilder lazyWriteBuilder;

  SparkMergeBuilder(SparkSession spark, Table table, String operation, LogicalWriteInfo writeInfo) {
    this.spark = spark;
    this.table = table;
    this.writeInfo = writeInfo;
    this.isolationLevel = getIsolationLevel(table.properties(), operation);
  }

  private IsolationLevel getIsolationLevel(Map<String, String> props, String operation) {
    String isolationLevelAsString;
    if (operation.equalsIgnoreCase("delete")) {
      isolationLevelAsString = props.getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT);
    } else if (operation.equalsIgnoreCase("update")) {
      isolationLevelAsString = props.getOrDefault(UPDATE_ISOLATION_LEVEL, UPDATE_ISOLATION_LEVEL_DEFAULT);
    } else if (operation.equalsIgnoreCase("merge")) {
      isolationLevelAsString = props.getOrDefault(MERGE_ISOLATION_LEVEL, MERGE_ISOLATION_LEVEL_DEFAULT);
    } else {
      throw new IllegalArgumentException("Unsupported operation: " + operation);
    }
    return IsolationLevel.valueOf(isolationLevelAsString.toUpperCase(Locale.ROOT));
  }

  @Override
  public void filterFiles(FilterFiles files) {
    scanBuilder().filterFiles(files);
  }

  @Override
  public ScanBuilder asScanBuilder() {
    return scanBuilder();
  }

  private SparkScanBuilder scanBuilder() {
    if (lazyScanBuilder == null) {
      SparkScanBuilder scanBuilder = new SparkScanBuilder(spark, table, writeInfo.options()) {
        @Override
        public Scan build() {
          Scan scan = super.buildMergeScan();
          SparkMergeBuilder.this.configuredScan = scan;
          return scan;
        }
      };
      // ignore residuals to ensure we read full files
      lazyScanBuilder = scanBuilder.ignoreResiduals();
    }

    return lazyScanBuilder;
  }

  @Override
  public WriteBuilder asWriteBuilder() {
    return writeBuilder();
  }

  private WriteBuilder writeBuilder() {
    if (lazyWriteBuilder == null) {
      Preconditions.checkState(configuredScan != null, "Write must be configured after scan");
      SparkWriteBuilder writeBuilder = new SparkWriteBuilder(spark, table, writeInfo);
      lazyWriteBuilder = writeBuilder.overwriteFiles(configuredScan, isolationLevel);
    }

    return lazyWriteBuilder;
  }
}
