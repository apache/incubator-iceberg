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

public class TableProperties {

  private TableProperties() {
  }

  public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
  public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
  public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

  public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
  public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000; // 1 minute

  public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
  public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 1800000; // 30 minutes

  public static final String COMMIT_NUM_STATUS_CHECKS = "commit.num-status-checks";
  public static final int COMMIT_NUM_STATUS_CHECKS_DEFAULT = 3;

  public static final String MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";
  public static final long MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 8388608; // 8 MB

  public static final String MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge";
  public static final int MANIFEST_MIN_MERGE_COUNT_DEFAULT = 100;

  public static final String MANIFEST_MERGE_ENABLED = "commit.manifest-merge.enabled";
  public static final boolean MANIFEST_MERGE_ENABLED_DEFAULT = true;

  public static final String DEFAULT_FILE_FORMAT = "write.format.default";
  public static final String DEFAULT_FILE_FORMAT_DEFAULT = "parquet";

  public static final String PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes";
  public static final String PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = "134217728"; // 128 MB

  public static final String PARQUET_PAGE_SIZE_BYTES = "write.parquet.page-size-bytes";
  public static final String PARQUET_PAGE_SIZE_BYTES_DEFAULT = "1048576"; // 1 MB

  public static final String PARQUET_DICT_SIZE_BYTES = "write.parquet.dict-size-bytes";
  public static final String PARQUET_DICT_SIZE_BYTES_DEFAULT = "2097152"; // 2 MB

  public static final String PARQUET_COMPRESSION = "write.parquet.compression-codec";
  public static final String PARQUET_COMPRESSION_DEFAULT = "gzip";

  public static final String PARQUET_COMPRESSION_LEVEL = "write.parquet.compression-level";
  public static final String PARQUET_COMPRESSION_LEVEL_DEFAULT = null;

  public static final String AVRO_COMPRESSION = "write.avro.compression-codec";
  public static final String AVRO_COMPRESSION_DEFAULT = "gzip";

  public static final String SPLIT_SIZE = "read.split.target-size";
  public static final long SPLIT_SIZE_DEFAULT = 134217728; // 128 MB

  public static final String METADATA_SPLIT_SIZE = "read.split.metadata-target-size";
  public static final long METADATA_SPLIT_SIZE_DEFAULT = 32 * 1024 * 1024; // 32 MB

  public static final String SPLIT_LOOKBACK = "read.split.planning-lookback";
  public static final int SPLIT_LOOKBACK_DEFAULT = 10;

  public static final String SPLIT_OPEN_FILE_COST = "read.split.open-file-cost";
  public static final long SPLIT_OPEN_FILE_COST_DEFAULT = 4 * 1024 * 1024; // 4MB

  public static final String PARQUET_VECTORIZATION_ENABLED = "read.parquet.vectorization.enabled";
  public static final boolean PARQUET_VECTORIZATION_ENABLED_DEFAULT = false;

  public static final String PARQUET_BATCH_SIZE = "read.parquet.vectorization.batch-size";
  public static final int PARQUET_BATCH_SIZE_DEFAULT = 5000;

  public static final String ORC_VECTORIZATION_ENABLED = "read.orc.vectorization.enabled";
  public static final boolean ORC_VECTORIZATION_ENABLED_DEFAULT = false;

  public static final String OBJECT_STORE_ENABLED = "write.object-storage.enabled";
  public static final boolean OBJECT_STORE_ENABLED_DEFAULT = false;

  public static final String OBJECT_STORE_PATH = "write.object-storage.path";

  public static final String WRITE_LOCATION_PROVIDER_IMPL = "write.location-provider.impl";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "data" folder underneath the root path of the table.
  public static final String WRITE_NEW_DATA_LOCATION = "write.folder-storage.path";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "metadata" folder underneath the root path of the table.
  public static final String WRITE_METADATA_LOCATION = "write.metadata.path";

  public static final String WRITE_PARTITION_SUMMARY_LIMIT = "write.summary.partition-limit";
  public static final int WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT = 0;

  public static final String MANIFEST_LISTS_ENABLED = "write.manifest-lists.enabled";
  public static final boolean MANIFEST_LISTS_ENABLED_DEFAULT = true;

  public static final String METADATA_COMPRESSION = "write.metadata.compression-codec";
  public static final String METADATA_COMPRESSION_DEFAULT = "none";

  public static final String METADATA_PREVIOUS_VERSIONS_MAX = "write.metadata.previous-versions-max";
  public static final int METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT = 100;

  // This enables to delete the oldest metadata file after commit.
  public static final String METADATA_DELETE_AFTER_COMMIT_ENABLED = "write.metadata.delete-after-commit.enabled";
  public static final boolean METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT = false;

  public static final String METRICS_MODE_COLUMN_CONF_PREFIX = "write.metadata.metrics.column.";
  public static final String DEFAULT_WRITE_METRICS_MODE = "write.metadata.metrics.default";
  public static final String DEFAULT_WRITE_METRICS_MODE_DEFAULT = "truncate(16)";

  public static final String DEFAULT_NAME_MAPPING = "schema.name-mapping.default";

  public static final String WRITE_AUDIT_PUBLISH_ENABLED = "write.wap.enabled";
  public static final String WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT = "false";

  public static final String WRITE_TARGET_FILE_SIZE_BYTES = "write.target-file-size-bytes";
  public static final long WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 536870912; // 512 MB

  public static final String SPARK_WRITE_PARTITIONED_FANOUT_ENABLED = "write.spark.fanout.enabled";
  public static final boolean SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT = false;

  public static final String SNAPSHOT_ID_INHERITANCE_ENABLED = "compatibility.snapshot-id-inheritance.enabled";
  public static final boolean SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT = false;

  public static final String ENGINE_HIVE_ENABLED = "engine.hive.enabled";
  public static final boolean ENGINE_HIVE_ENABLED_DEFAULT = false;

  public static final String WRITE_DISTRIBUTION_MODE = "write.distribution-mode";
  public static final String WRITE_DISTRIBUTION_MODE_NONE = "none";
  public static final String WRITE_DISTRIBUTION_MODE_HASH = "hash";
  public static final String WRITE_DISTRIBUTION_MODE_RANGE = "range";
  public static final String WRITE_DISTRIBUTION_MODE_DEFAULT = WRITE_DISTRIBUTION_MODE_NONE;

  public static final String GC_ENABLED = "gc.enabled";
  public static final boolean GC_ENABLED_DEFAULT = true;

  public static final String MAX_SNAPSHOT_AGE_MS = "history.expire.max-snapshot-age-ms";
  public static final long MAX_SNAPSHOT_AGE_MS_DEFAULT = 5 * 24 * 60 * 60 * 1000; // 5 days

  public static final String MIN_SNAPSHOTS_TO_KEEP = "history.expire.min-snapshots-to-keep";
  public static final int MIN_SNAPSHOTS_TO_KEEP_DEFAULT = 1;

  public static final String DELETE_ISOLATION_LEVEL = "write.delete.isolation-level";
  public static final String DELETE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String DELETE_MODE = "write.delete.mode";
  public static final String DELETE_MODE_DEFAULT = "copy-on-write";

  public static final String UPDATE_ISOLATION_LEVEL = "write.update.isolation-level";
  public static final String UPDATE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String UPDATE_MODE = "write.update.mode";
  public static final String UPDATE_MODE_DEFAULT = "copy-on-write";

  public static final String MERGE_ISOLATION_LEVEL = "write.merge.isolation-level";
  public static final String MERGE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String MERGE_MODE = "write.merge.mode";
  public static final String MERGE_MODE_DEFAULT = "copy-on-write";

  public static final String MERGE_CARDINALITY_CHECK_ENABLED = "write.merge.cardinality-check.enabled";
  public static final boolean MERGE_CARDINALITY_CHECK_ENABLED_DEFAULT = true;

  /**
   * Encryption manager type
   */
  public static final String ENCRYPTION_MANAGER_TYPE = "encryption.manager.type";
  public static final String ENCRYPTION_MANAGER_TYPE_PLAINTEXT = "plaintext";
  public static final String ENCRYPTION_MANAGER_TYPE_ENVELOPE = "envelope";

  /**
   * Allow file format native encryption instead of
   * encrypting the entire file through Iceberg encryption stream.
   */
  public static final String ENCRYPTION_PUSHDOWN_ENABLED = "encryption.pushdown";
  public static final boolean ENCRYPTION_PUSHDOWN_ENABLED_DEFAULT = false;

  /**
   * Implementation of the key provider for envelope encryption.
   */
  public static final String ENCRYPTION_KEY_PROVIDER_IMPL = "encryption.key.provider-impl";

}
