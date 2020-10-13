# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


class TableProperties(object):
    COMMIT_NUM_RETRIES = "commit.retry.num-retries"
    COMMIT_NUM_RETRIES_DEFAULT = 4

    COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms"
    COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100

    COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms"
    COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000

    COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms"
    COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 1800000

    MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes"
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 8388608

    MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge"
    MANIFEST_MIN_MERGE_COUNT_DEFAULT = 100

    DEFAULT_FILE_FORMAT = "write.format.default"
    DEFAULT_FILE_FORMAT_DEFAULT = "parquet"

    PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes"
    PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = "134217728"

    PARQUET_PAGE_SIZE_BYTES = "write.parquet.page-size-bytes"
    PARQUET_PAGE_SIZE_BYTES_DEFAULT = "1048576"

    PARQUET_DICT_SIZE_BYTES = "write.parquet.dict-size-bytes"
    PARQUET_DICT_SIZE_BYTES_DEFAULT = "2097152"

    PARQUET_COMPRESSION = "write.parquet.compression-codec"
    PARQUET_COMPRESSION_DEFAULT = "gzip"

    AVRO_COMPRESSION = "write.avro.compression-codec"
    AVRO_COMPRESSION_DEFAULT = "gzip"

    SPLIT_SIZE = "read.split.target-size"
    SPLIT_SIZE_DEFAULT = 134217728

    SPLIT_LOOKBACK = "read.split.planning-lookback"
    SPLIT_LOOKBACK_DEFAULT = 10

    SPLIT_OPEN_FILE_COST = "read.split.open-file-cost"
    SPLIT_OPEN_FILE_COST_DEFAULT = 4 * 1024 * 1024

    OBJECT_STORE_ENABLED = "write.object-storage.enabled"
    OBJECT_STORE_ENABLED_DEFAULT = False

    OBJECT_STORE_PATH = "write.object-storage.path"

    WRITE_LOCATION_PROVIDER_IMPL = "write.location-provider.impl"

    WRITE_NEW_DATA_LOCATION = "write.folder-storage.path"

    WRITE_METADATA_LOCATION = "write.metadata.path"

    MANIFEST_LISTS_ENABLED = "write.manifest-lists.enabled"

    MANIFEST_LISTS_ENABLED_DEFAULT = True
