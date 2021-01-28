<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Configuration

## Catalog properties

Iceberg catalogs support using catalog properties to configure catalog behaviors. Here is a list of commonly used catalog properties:

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| catalog-impl                      | null               | a custom `Catalog` implementation to use by an engine  |
| io-impl                           | null               | a custom `FileIO` implementation to use in a catalog   |
| warehouse                         | null               | the root path of the data warehouse                    |
| uri                               | null               | a URI string, such as Hive metastore URI               |
| clients                           | 2                  | client pool size                                       |

`HadoopCatalog` and `HiveCatalog` can access the properties in their constructors.
Any other custom catalog can access the properties by implementing `Catalog.initialize(catalogName, catalogProperties)`.
The properties can be manually constructed or passed in from a compute engine like Spark or Flink.
Spark uses its session properties as catalog properties, see more details in the [Spark configuration](#spark-configuration) section.
Flink passes in catalog properties through `CREATE CATALOG` statement, see more details in the [Flink](../flink/#creating-catalogs-and-using-catalogs) section.

### Lock catalog properties

Here are the catalog properties related to locking. They are used by some catalog implementations to control the locking behavior during commits.

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| lock-impl                         | null               | a custom implementation of the lock manager, the actual interface depends on the catalog used  |
| lock.table                        | null               | an auxiliary table for locking, such as in [AWS DynamoDB lock manager](../aws/#dynamodb-for-commit-locking)  |
| lock.acquire-interval-ms          | 5 seconds          | the interval to wait between each attempt to acquire a lock  |
| lock.acquire-timeout-ms           | 3 minutes          | the maximum time to try acquiring a lock               |
| lock.heartbeat-interval-ms        | 3 seconds          | the interval to wait between each heartbeat after acquiring a lock  |
| lock.heartbeat-timeout-ms         | 15 seconds         | the maximum time without a heartbeat to consider a lock expired  |


## Table properties

Iceberg tables support table properties to configure table behavior, like the default split size for readers.

### Read properties

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| read.split.target-size            | 134217728 (128 MB) | Target size when combining data input splits           |
| read.split.metadata-target-size   | 33554432 (32 MB)   | Target size when combining metadata input splits       |
| read.split.planning-lookback      | 10                 | Number of bins to consider when combining input splits |
| read.split.open-file-cost         | 4194304 (4 MB)     | The estimated cost to open a file, used as a minimum weight when combining splits. |

### Write properties

| Property                           | Default            | Description                                        |
| ---------------------------------- | ------------------ | -------------------------------------------------- |
| write.format.default               | parquet            | Default file format for the table; parquet, avro, or orc |
| write.parquet.row-group-size-bytes | 134217728 (128 MB) | Parquet row group size                             |
| write.parquet.page-size-bytes      | 1048576 (1 MB)     | Parquet page size                                  |
| write.parquet.dict-size-bytes      | 2097152 (2 MB)     | Parquet dictionary page size                       |
| write.parquet.compression-codec    | gzip               | Parquet compression codec                          |
| write.parquet.compression-level    | null               | Parquet compression level                          |
| write.avro.compression-codec       | gzip               | Avro compression codec                             |
| write.location-provider.impl       | null               | Optional custom implemention for LocationProvider  |
| write.metadata.compression-codec   | none               | Metadata compression codec; none or gzip           |
| write.metadata.metrics.default     | truncate(16)       | Default metrics mode for all columns in the table; none, counts, truncate(length), or full |
| write.metadata.metrics.column.col1 | (not set)          | Metrics mode for column 'col1' to allow per-column tuning; none, counts, truncate(length), or full |
| write.target-file-size-bytes       | Long.MAX_VALUE     | Controls the size of files generated to target about this many bytes |
| write.distribution-mode            | none               | Defines distribution of write data: __none__: don't shuffle rows; __hash__: hash distribute by partition key ; __range__: range distribute by partition key or sort key if table has an SortOrder |
| write.wap.enabled                  | false              | Enables write-audit-publish writes |
| write.summary.partition-limit      | 0                  | Includes partition-level summary stats in snapshot summaries if the changed partition count is less than this limit |
| write.metadata.delete-after-commit.enabled | false      | Controls whether to delete the oldest version metadata files after commit |
| write.metadata.previous-versions-max       | 100        | The max number of previous version metadata files to keep before deleting after commit |
| write.spark.fanout.enabled       | false        | Enables Partitioned-Fanout-Writer writes in Spark |

### Table behavior properties

| Property                           | Default          | Description                                                   |
| ---------------------------------- | ---------------- | ------------------------------------------------------------- |
| commit.retry.num-retries           | 4                | Number of times to retry a commit before failing              |
| commit.retry.min-wait-ms           | 100              | Minimum time in milliseconds to wait before retrying a commit |
| commit.retry.max-wait-ms           | 60000 (1 min)    | Maximum time in milliseconds to wait before retrying a commit |
| commit.retry.total-timeout-ms      | 1800000 (30 min) | Maximum time in milliseconds to wait before retrying a commit |
| commit.manifest.target-size-bytes  | 8388608 (8 MB)   | Target size when merging manifest files                       |
| commit.manifest.min-count-to-merge | 100              | Minimum number of manifests to accumulate before merging      |
| commit.manifest-merge.enabled      | true             | Controls whether to automatically merge manifests on writes   |
| history.expire.max-snapshot-age-ms | 432000000 (5 days) | Default max age of snapshots to keep while expiring snapshots    |
| history.expire.min-snapshots-to-keep | 1                | Default min number of snapshots to keep while expiring snapshots |

### Compatibility flags

| Property                                      | Default  | Description                                                   |
| --------------------------------------------- | -------- | ------------------------------------------------------------- |
| compatibility.snapshot-id-inheritance.enabled | false    | Enables committing snapshots without explicit snapshot IDs    |

## Hadoop configuration

The following properties from the Hadoop configuration are used by the Hive Metastore connector.

| Property                              | Default          | Description                                                                        |
| ------------------------------------- | ---------------- | ---------------------------------------------------------------------------------- |
| iceberg.hive.client-pool-size         | 5                | The size of the Hive client pool when tracking tables in HMS                       |
| iceberg.hive.lock-timeout-ms          | 180000 (3 min)   | Maximum time in milliseconds to acquire a lock                                     |
| iceberg.hive.lock-check-min-wait-ms   | 50               | Minimum time in milliseconds to check back on the status of lock acquisition       |
| iceberg.hive.lock-check-max-wait-ms   | 5000             | Maximum time in milliseconds to check back on the status of lock acquisition       |

Note: `iceberg.hive.lock-check-max-wait-ms` should be less than the [transaction timeout](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.txn.timeout) 
of the Hive Metastore (`hive.txn.timeout` or `metastore.txn.timeout` in the newer versions). Otherwise, the heartbeats on the lock (which happens during the lock checks) would end up expiring in the 
Hive Metastore before the lock is retried from Iceberg.

The following properties from the Hadoop configuration are used by Hadoop Tables

| Property                                      | Default  | Description                                                            |
| --------------------------------------------- | -------- | ---------------------------------------------------------------------- |
| iceberg.engine.hadoop.`SCHEME`.atomic.write   | false    | Controls whether atomic write will be used instead of atomic rename. `SCHEME` is the scheme used, e.g. `{hdfs, cos}`    |


## Spark configuration

### Catalogs

[Spark catalogs](./spark.md#configuring-catalogs) are configured using Spark session properties.

A catalog is created and named by adding a property `spark.sql.catalog.(catalog-name)` with an implementation class for its value.

Iceberg supplies two implementations:

* `org.apache.iceberg.spark.SparkCatalog` supports a Hive Metastore or a Hadoop warehouse as a catalog
* `org.apache.iceberg.spark.SparkSessionCatalog` adds support for Iceberg tables to Spark's built-in catalog, and delegates to the built-in catalog for non-Iceberg tables

Both catalogs are configured using properties nested under the catalog name:

| Property                                           | Values                        | Description                                                          |
| -------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------- |
| spark.sql.catalog._catalog-name_.type              | `hive` or `hadoop`            | The underlying Iceberg catalog implementation, `HiveCatalog` or `HadoopCatalog` |
| spark.sql.catalog._catalog-name_.catalog-impl      |                               | The underlying Iceberg catalog implementation. When set, the value of `type` property is ignored |
| spark.sql.catalog._catalog-name_.default-namespace | default                       | The default current namespace for the catalog                        |
| spark.sql.catalog._catalog-name_.uri               | thrift://host:port            | URI for the Hive Metastore; default from `hive-site.xml` (Hive only) |
| spark.sql.catalog._catalog-name_.warehouse         | hdfs://nn:8020/warehouse/path | Base path for the warehouse directory (Hadoop only)                  |

### Read options

Spark read options are passed when configuring the DataFrameReader, like this:

```scala
// time travel
spark.read
    .option("snapshot-id", 10963874102873L)
    .table("catalog.db.table")
```

| Spark option    | Default               | Description                                                                               |
| --------------- | --------------------- | ----------------------------------------------------------------------------------------- |
| snapshot-id     | (latest)              | Snapshot ID of the table snapshot to read                                                 |
| as-of-timestamp | (latest)              | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |
| split-size      | As per table property | Overrides this table's read.split.target-size and read.split.metadata-target-size         |
| lookback        | As per table property | Overrides this table's read.split.planning-lookback                                       |
| file-open-cost  | As per table property | Overrides this table's read.split.open-file-cost                                          |
| vectorization-enabled  | As per table property | Overrides this table's read.parquet.vectorization.enabled                                          |
| batch-size  | As per table property | Overrides this table's read.parquet.vectorization.batch-size                                          |

### Write options

Spark write options are passed when configuring the DataFrameWriter, like this:

```scala
// write with Avro instead of Parquet
df.write
    .option("write-format", "avro")
    .option("snapshot-property.key", "value")
    .insertInto("catalog.db.table")
```

| Spark option           | Default                    | Description                                                  |
| ---------------------- | -------------------------- | ------------------------------------------------------------ |
| write-format           | Table write.format.default | File format to use for this write operation; parquet, avro, or orc |
| target-file-size-bytes | As per table property      | Overrides this table's write.target-file-size-bytes          |
| check-nullability      | true                       | Sets the nullable check on fields                            |
| snapshot-property._custom-key_    | null            | Adds an entry with custom-key and corresponding value in the snapshot summary  |
| fanout-enabled       | false        | Overrides this table's write.spark.fanout.enabled  |
| check-ordering       | true        | Checks if input schema and table schema are same  |

