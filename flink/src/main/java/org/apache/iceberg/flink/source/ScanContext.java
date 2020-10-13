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

package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

/**
 * Context object with optional arguments for a Flink Scan.
 */
class ScanContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("case-sensitive").booleanType().defaultValue(false);

  private static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

  private static final ConfigOption<Long> START_SNAPSHOT_ID =
      ConfigOptions.key("start-snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Long> END_SNAPSHOT_ID =
      ConfigOptions.key("end-snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Long> SPLIT_SIZE =
      ConfigOptions.key("split-size").longType().defaultValue(null);

  private static final ConfigOption<Integer> SPLIT_LOOKBACK =
      ConfigOptions.key("split-lookback").intType().defaultValue(null);

  private static final ConfigOption<Long> SPLIT_FILE_OPEN_COST =
      ConfigOptions.key("split-file-open-cost").longType().defaultValue(null);

  private final boolean caseSensitive;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final String nameMapping;
  private final Schema projectedSchema;
  private final List<Expression> filterExpressions;

  ScanContext() {
    this.caseSensitive = CASE_SENSITIVE.defaultValue();
    this.snapshotId = SNAPSHOT_ID.defaultValue();
    this.startSnapshotId = START_SNAPSHOT_ID.defaultValue();
    this.endSnapshotId = END_SNAPSHOT_ID.defaultValue();
    this.asOfTimestamp = AS_OF_TIMESTAMP.defaultValue();
    this.splitSize = SPLIT_SIZE.defaultValue();
    this.splitLookback = SPLIT_LOOKBACK.defaultValue();
    this.splitOpenFileCost = SPLIT_FILE_OPEN_COST.defaultValue();
    this.nameMapping = null;
    this.projectedSchema = null;
    this.filterExpressions = null;
  }

  private ScanContext(boolean caseSensitive, Long snapshotId, Long startSnapshotId, Long endSnapshotId,
      Long asOfTimestamp, Long splitSize, Integer splitLookback, Long splitOpenFileCost,
      String nameMapping, Schema projectedSchema, List<Expression> filterExpressions) {
    this.caseSensitive = caseSensitive;
    this.snapshotId = snapshotId;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.splitSize = splitSize;
    this.splitLookback = splitLookback;
    this.splitOpenFileCost = splitOpenFileCost;
    this.nameMapping = nameMapping;
    this.projectedSchema = projectedSchema;
    this.filterExpressions = filterExpressions;
  }

  ScanContext fromProperties(Map<String, String> properties) {
    Configuration config = new Configuration();
    properties.forEach(config::setString);
    return new ScanContext(config.get(CASE_SENSITIVE), config.get(SNAPSHOT_ID), config.get(START_SNAPSHOT_ID),
        config.get(END_SNAPSHOT_ID), config.get(AS_OF_TIMESTAMP), config.get(SPLIT_SIZE), config.get(SPLIT_LOOKBACK),
        config.get(SPLIT_FILE_OPEN_COST), properties.get(DEFAULT_NAME_MAPPING), projectedSchema, filterExpressions);
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  ScanContext setCaseSensitive(boolean isCaseSensitive) {
    return new ScanContext(isCaseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long snapshotId() {
    return snapshotId;
  }

  ScanContext useSnapshotId(Long scanSnapshotId) {
    return new ScanContext(caseSensitive, scanSnapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long startSnapshotId() {
    return startSnapshotId;
  }

  ScanContext startSnapshotId(Long id) {
    return new ScanContext(caseSensitive, snapshotId, id, endSnapshotId, asOfTimestamp, splitSize, splitLookback,
        splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long endSnapshotId() {
    return endSnapshotId;
  }

  ScanContext endSnapshotId(Long id) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, id, asOfTimestamp, splitSize, splitLookback,
        splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long asOfTimestamp() {
    return asOfTimestamp;
  }

  ScanContext asOfTimestamp(Long timestamp) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, timestamp, splitSize,
        splitLookback, splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long splitSize() {
    return splitSize;
  }

  ScanContext splitSize(Long size) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, size,
        splitLookback, splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Integer splitLookback() {
    return splitLookback;
  }

  ScanContext splitLookback(Integer lookback) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        lookback, splitOpenFileCost, nameMapping, projectedSchema, filterExpressions);
  }

  Long splitOpenFileCost() {
    return splitOpenFileCost;
  }

  ScanContext splitOpenFileCost(Long fileCost) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, fileCost, nameMapping, projectedSchema, filterExpressions);
  }

  String nameMapping() {
    return nameMapping;
  }

  ScanContext nameMapping(String mapping) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, splitOpenFileCost, mapping, projectedSchema, filterExpressions);
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  ScanContext project(Schema schema) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, splitOpenFileCost, nameMapping, schema, filterExpressions);
  }

  List<Expression> filterExpressions() {
    return filterExpressions;
  }

  ScanContext filterRows(List<Expression> filters) {
    return new ScanContext(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
        splitLookback, splitOpenFileCost, nameMapping, projectedSchema, filters);
  }
}
