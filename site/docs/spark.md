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

# Spark

To use Iceberg in Spark, first configure [Spark catalogs](./spark-configuration.md).

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions:

| SQL feature support                              | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [`CREATE TABLE`](#create-table)                | ✔️        |            |                                                |
| [`CREATE TABLE AS`](#create-table-as-select)   | ✔️        |            |                                                |
| [`REPLACE TABLE AS`](#replace-table-as-select) | ✔️        |            |                                                |
| [`ALTER TABLE`](#alter-table)                  | ✔️        |            | ⚠ Requires [SQL extensions](./spark-configuration.md#sql-extensions) enabled to update partition field and sort order |
| [`DROP TABLE`](#drop-table)                    | ✔️        |            |                                                |
| [`SELECT`](#querying-with-sql)                 | ✔️        |            |                                                |
| [`INSERT INTO`](#insert-into)                  | ✔️        |            |                                                |
| [`INSERT OVERWRITE`](#insert-overwrite)        | ✔️        |            |                                                |

| DataFrame feature support                        | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [DataFrame reads](#querying-with-dataframes)     | ✔️        | ✔️          |                                                |
| [DataFrame append](#appending-data)              | ✔️        | ✔️          |                                                |
| [DataFrame overwrite](#overwriting-data)         | ✔️        | ✔️          | ⚠ Behavior changed in Spark 3.0                |
| [DataFrame CTAS and RTAS](#creating-tables)      | ✔️        |            |                                                |
| [Metadata tables](#inspecting-tables)            | ✔️        | ✔️          |                                                |

## Configuring catalogs

Spark 3.0 adds an API to plug in table catalogs that are used to load, create, and manage Iceberg tables. Spark catalogs are configured by setting [Spark properties](./configuration.md#catalogs) under `spark.sql.catalog`.

This creates an Iceberg catalog named `hive_prod` that loads tables from a Hive metastore:

```plain
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
# omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml
```

Iceberg also supports a directory-based catalog in HDFS that can be configured using `type=hadoop`:

```plain
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
```

!!! Note
    The Hive-based catalog only loads Iceberg tables. To load non-Iceberg tables in the same Hive metastore, use a [session catalog](#replacing-the-session-catalog).

### Using catalogs

Catalog names are used in SQL queries to identify a table. In the examples above, `hive_prod` and `hadoop_prod` can be used to prefix database and table names that will be loaded from those catalogs.

```sql
SELECT * FROM hive_prod.db.table -- load db.table from catalog hive_prod
```

Spark 3 keeps track of the current catalog and namespace, which can be omitted from table names.

```sql
USE hive_prod.db;
SELECT * FROM table -- load db.table from catalog hive_prod
```

To see the current catalog and namespace, run `SHOW CURRENT NAMESPACE`.

### Replacing the session catalog

To add Iceberg table support to Spark's built-in catalog, configure `spark_catalog` to use Iceberg's `SparkSessionCatalog`.

```plain
spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type = hive
```

Spark's built-in catalog supports existing v1 and v2 tables tracked in a Hive Metastore. This configures Spark to use Iceberg's `SparkSessionCatalog` as a wrapper around that session catalog. When a table is not an Iceberg table, the built-in catalog will be used to load it instead.

This configuration can use same Hive Metastore for both Iceberg and non-Iceberg tables.

### Loading a custom catalog

Spark supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property.
When `catalog-impl` is set, the value of `type` is ignored. Here is an example:

```plain
spark.sql.catalog.custom_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.custom_prod.catalog-impl = com.my.custom.CatalogImpl
spark.sql.catalog.custom_prod.my-additional-catalog-config = my-value
```

## DDL commands

!!! Note
    Spark 2.4 can't create Iceberg tables with DDL, instead use the [Iceberg API](./java-api-quickstart.md).

### `CREATE TABLE`

Spark 3.0 can create tables in any Iceberg catalog with the clause `USING iceberg`:

```sql
CREATE TABLE prod.db.sample (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg
```

Iceberg will convert the column type in Spark to corresponding Iceberg type. Please check the section of [type compatibility on creating table](#spark-type-to-iceberg-type) for details.

Table create commands, including CTAS and RTAS, support the full range of Spark create clauses, including:

* `PARTITION BY (partition-expressions)` to configure partitioning
* `LOCATION '(fully-qualified-uri)'` to set the table location
* `COMMENT 'table documentation'` to set a table description
* `TBLPROPERTIES ('key'='value', ...)` to set [table configuration](./configuration.md)

Create commands may also set the default format with the `USING` clause. This is only supported for `SparkCatalog` because Spark handles the `USING` clause differently for the built-in catalog.

#### `PARTITIONED BY`

To create a partitioned table, use `PARTITIONED BY`:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string)
USING iceberg
PARTITIONED BY (category)
```

The `PARTITIONED BY` clause supports transform expressions to create [hidden partitions](./partitioning.md).

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category)
```

Supported partition transforms are:

* `years` for yearly partitions
* `months` for monthly partitions
* `days` for daily partitions
* `hours` for hourly partitions
* `bucket` for bucketing (with width)
* `truncate` to truncate integers or strings (with length)

### `CREATE TABLE ... AS SELECT`

Iceberg supports CTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). CTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

```sql
CREATE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

### `REPLACE TABLE ... AS SELECT`

Iceberg supports RTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). RTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

Atomic table replacement creates a new snapshot with the results of the `SELECT` query, but keeps table history.

```sql
REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```
```sql
CREATE OR REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

The schema and partition spec will be replaced if changed. To avoid modifying the table's schema and partitioning, use `INSERT OVERWRITE` instead of `REPLACE TABLE`.
The new table properties in the `REPLACE TABLE` command will be merged with any existing table properties. The existing table properties will be updated if changed else they are preserved.
### `ALTER TABLE`

Iceberg has full `ALTER TABLE` support in Spark 3, including:

* Renaming a table
* Setting or removing table properties
* Adding, deleting, and renaming columns
* Adding, deleting, and renaming nested fields
* Reordering top-level columns and nested struct fields
* Widening the type of `int`, `float`, and `decimal` fields
* Making required columns optional

### `ALTER TABLE ... RENAME TO`

```sql
ALTER TABLE prod.db.sample RENAME TO prod.db.new_name
```

### `ALTER TABLE ... SET TBLPROPERTIES`

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
    'read.split.target-size'='268435456'
)
```

Iceberg uses table properties to control table behavior. For a list of available properties, see [Table configuration](./configuration.md).

`UNSET` is used to remove properties:

```sql
ALTER TABLE prod.db.sample UNSET TBLPROPERTIES ('read.split.target-size')
```

### `ALTER TABLE ... ADD COLUMN`

```sql
ALTER TABLE prod.db.sample ADD COLUMN point struct<x: double NOT NULL, y: double NOT NULL> AFTER data
ALTER TABLE prod.db.sample ADD COLUMN point.z double FIRST
```

### `ALTER TABLE ... RENAME COLUMN`

```sql
ALTER TABLE prod.db.sample RENAME COLUMN data TO payload
ALTER TABLE prod.db.sample RENAME COLUMN location.lat TO latitude
```

Note that nested rename commands only rename the leaf field. The above command renames `location.lat` to `location.latitude`

### `ALTER TABLE ... ALTER COLUMN`

Alter column is used to widen types, make a field optional, set comments, and reorder fields.

```sql
ALTER TABLE prod.db.sample ALTER COLUMN id DROP NOT NULL
ALTER TABLE prod.db.sample ALTER COLUMN location.lat TYPE double
ALTER TABLE prod.db.sample ALTER COLUMN point.z AFTER y
ALTER TABLE prod.db.sample ALTER COLUMN id COMMENT 'unique id'
```

### `ALTER TABLE ... DROP COLUMN`

```sql
ALTER TABLE prod.db.sample DROP COLUMN id
ALTER TABLE prod.db.sample DROP COLUMN point.z
```

### `ALTER TABLE ... ADD PARTITION FIELD`

```sql
ALTER TABLE prod.db.sample ADD PARTITION FIELD catalog -- identity transform
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id)
ALTER TABLE prod.db.sample ADD PARTITION FIELD truncate(data, 4)
ALTER TABLE prod.db.sample ADD PARTITION FIELD years(ts)
-- use optional AS keyword to specify a custom name for the partition field
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id) AS shard
```

!!! Warning
    Changing partitioning will change the behavior of dynamic writes, which overwrite any partition that is written to.
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.


### `ALTER TABLE ... DROP PARTITION FIELD`

```sql
ALTER TABLE prod.db.sample DROP PARTITION FIELD catalog
ALTER TABLE prod.db.sample DROP PARTITION FIELD bucket(16, id)
ALTER TABLE prod.db.sample DROP PARTITION FIELD truncate(data, 4)
ALTER TABLE prod.db.sample DROP PARTITION FIELD years(ts)
ALTER TABLE prod.db.sample DROP PARTITION FIELD shard
```

!!! Warning
    Changing partitioning will change the behavior of dynamic writes, which overwrite any partition that is written to.
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.


### `ALTER TABLE ... WRITE ORDERED BY`

```sql
ALTER TABLE prod.db.sample WRITE ORDERED BY category, id
-- use optional ASC/DEC keyword to specify sort order of each field (default ASC)
ALTER TABLE prod.db.sample WRITE ORDERED BY category ASC, id DESC
-- use optional NULLS FIRST/NULLS LAST keyword to specify null order of each field (default FIRST)
ALTER TABLE prod.db.sample WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST
```

### `DROP TABLE`

To delete a table, run:

```sql
DROP TABLE prod.db.sample
```

## Querying with SQL

In Spark 3, tables use identifiers that include a [catalog name](#using-catalogs).

```sql
SELECT * FROM prod.db.table -- catalog: prod, namespace: db, table: table
```

Metadata tables, like `history` and `snapshots`, can use the Iceberg table name as a namespace.

For example, to read from the `files` metadata table for `prod.db.table`, run:

```
SELECT * FROM prod.db.table.files
```

## Querying with DataFrames

To load a table as a DataFrame, use `table`:

```scala
val df = spark.table("prod.db.table")
```

!!! Warning
    When reading with DataFrames in Spark 3, use `table` to load a table by name from a catalog unless `option` is also required.
    Using `format("iceberg")` loads an isolated table reference that is not refreshed when other queries update the table.


### Time travel

To select a specific table snapshot or the snapshot at some time, Iceberg supports two Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds

```scala
// time travel to October 26, 1986 at 01:21:00
spark.read
    .option("as-of-timestamp", "499162860000")
    .format("iceberg")
    .load("path/to/table")
```

```scala
// time travel to snapshot with ID 10963874102873L
spark.read
    .option("snapshot-id", 10963874102873L)
    .format("iceberg")
    .load("path/to/table")
```

!!! Note
    Spark does not currently support using `option` with `table` in DataFrameReader commands. All options will be silently
    ignored. Do not use `table` when attempting to time-travel or use other options. Options will be supported with `table`
    in [Spark 3.1 - SPARK-32592](https://issues.apache.org/jira/browse/SPARK-32592).

Time travel is not yet supported by Spark's SQL syntax.

### Incremental read
To incrementally read a specific table, Iceberg supports read over snapshot with two Spark read options:
* `start-snapshot-id` the snapshot id of the start of the incremental read
* `end-snapshot-id` the snapshot id of the end of the incremental read (need to be greater than the start)

```scala
// Read from snapshot ID 10963874102873L
spark.read
    .option("start-snapshot-id", 10963874102873L)
    .format("iceberg")
    .load("path/to/table")
```

```scala
// Read from snapshot ID 296410040247533544L to ID 6536733823181975045L
spark.read
    .option("start-snapshot-id", 296410040247533544L)
    .option("end-snapshot-id", 6536733823181975045L)
    .format("iceberg")
    .load("path/to/table")
```
!!! Note
    Spark does not currently support using `option` with `table` in DataFrameReader commands. All options will be silently
    ignored. Do not use `table` when attempting to time-travel or use other options. Options will be supported with `table`
    in [Spark 3.1 - SPARK-32592](https://issues.apache.org/jira/browse/SPARK-32592).

Incremental read not yet support overwrite operations.

Incremental read is not yet supported by Spark's SQL syntax.


### Table names and paths

Paths and table names can be loaded from the Spark3 dataframe interface. How paths/tables are loaded depends on how
the identifier is specified. When using `spark.read().format("iceberg").path(table)` or `spark.table(table)` the `table`
variable can take a number of forms as listed below:

*  `file:/path/to/table` -> loads a HadoopTable at given path
*  `tablename` -> loads `currentCatalog.currentNamespace.tablename`
*  `catalog.tablename` -> load `tablename` from the specified catalog.
*  `namespace.tablename` -> load `namespace.tablename` from current catalog
*  `catalog.namespace.tablename` -> load `namespace.tablename` from the specified catalog.
*  `namespace1.namespace2.tablename` -> load `namespace1.namespace2.tablename` from current catalog

The above list is in order of priority. For example: a matching catalog will take priority over any namespace resolution.

### Spark 2.4

Spark 2.4 requires using the DataFrame reader with `iceberg` as a format, because 2.4 does not support catalogs:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table")
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table")
```

#### Spark 2.4 with SQL

To run SQL `SELECT` statements on Iceberg tables in 2.4, register the DataFrame as a temporary table:

```scala
val df = spark.read.format("iceberg").load("db.table")
df.createOrReplaceTempView("table")

spark.sql("""select count(1) from table""").show()
```


## Writing with SQL

Spark 3 supports SQL `INSERT INTO` and `INSERT OVERWRITE`, as well as the new `DataFrameWriterV2` API.

### `INSERT INTO`

To append new data to a table, use `INSERT INTO`.

```sql
INSERT INTO prod.db.table VALUES (1, 'a'), (2, 'b')
```
```sql
INSERT INTO prod.db.table SELECT ...
```

### `INSERT OVERWRITE`

To replace data in the table with the result of a query, use `INSERT OVERWRITE`. Overwrites are atomic operations for Iceberg tables.

The partitions that will be replaced by `INSERT OVERWRITE` depends on Spark's partition overwrite mode and the partitioning of a table.

!!! Warning
    Spark 3.0.0 has a correctness bug that affects dynamic `INSERT OVERWRITE` with hidden partitioning, [SPARK-32168][spark-32168].
    For tables with [hidden partitions](./partitioning.md), wait for Spark 3.0.1.

[spark-32168]: https://issues.apache.org/jira/browse/SPARK-32168


#### Overwrite behavior

Spark's default overwrite mode is **static**, but **dynamic overwrite mode is recommended when writing to Iceberg tables.** Static overwrite mode determines which partitions to overwrite in a table by converting the `PARTITION` clause to a filter, but the `PARTITION` clause can only reference table columns.

Dynamic overwrite mode is configured by setting `spark.sql.sources.partitionOverwriteMode=dynamic`.

To demonstrate the behavior of dynamic and static overwrites, consider a `logs` table defined by the following DDL:

```sql
CREATE TABLE prod.my_app.logs (
    uuid string NOT NULL,
    level string NOT NULL,
    ts timestamp NOT NULL,
    message string)
USING iceberg
PARTITIONED BY (level, hours(ts))
```

#### Dynamic overwrite

When Spark's overwrite mode is dynamic, partitions that have rows produced by the `SELECT` query will be replaced.

For example, this query removes duplicate log events from the example `logs` table.

```sql
INSERT OVERWRITE prod.my_app.logs
SELECT uuid, first(level), first(ts), first(message)
FROM prod.my_app.logs
WHERE cast(ts as date) = '2020-07-01'
GROUP BY uuid
```

In dynamic mode, this will replace any partition with rows in the `SELECT` result. Because the date of all rows is restricted to 1 July, only hours of that day will be replaced.

#### Static overwrite

When Spark's overwrite mode is static, the `PARTITION` clause is converted to a filter that is used to delete from the table. If the `PARTITION` clause is omitted, all partitions will be replaced.

Because there is no `PARTITION` clause in the query above, it will drop all existing rows in the table when run in static mode, but will only write the logs from 1 July.

To overwrite just the partitions that were loaded, add a `PARTITION` clause that aligns with the `SELECT` query filter:

```sql
INSERT OVERWRITE prod.my_app.logs
PARTITION (level = 'INFO')
SELECT uuid, first(level), first(ts), first(message)
FROM prod.my_app.logs
WHERE level = 'INFO'
GROUP BY uuid
```

Note that this mode cannot replace hourly partitions like the dynamic example query because the `PARTITION` clause can only reference table columns, not hidden partitions.

### `DELETE FROM`

Spark 3 added support for `DELETE FROM` queries to remove data from tables.

Delete queries accept a filter to match rows to delete. Iceberg can delete data as long as the filter matches entire partitions of the table, or it can determine that all rows of a file match. If a file contains some rows that should be deleted and some that should not, Iceberg will throw an exception.

```sql
DELETE FROM prod.db.table
WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'
```


## Writing with DataFrames

Spark 3 introduced the new `DataFrameWriterV2` API for writing to tables using data frames. The v2 API is recommended for several reasons:

* CTAS, RTAS, and overwrite by filter are supported
* All operations consistently write columns to a table by name
* Hidden partition expressions are supported in `partitionedBy`
* Overwrite behavior is explicit, either dynamic or by a user-supplied filter
* The behavior of each operation corresponds to SQL statements
    - `df.writeTo(t).create()` is equivalent to `CREATE TABLE AS SELECT`
    - `df.writeTo(t).replace()` is equivalent to `REPLACE TABLE AS SELECT`
    - `df.writeTo(t).append()` is equivalent to `INSERT INTO`
    - `df.writeTo(t).overwritePartitions()` is equivalent to dynamic `INSERT OVERWRITE`

The v1 DataFrame `write` API is still supported, but is not recommended.

!!! Warning
    When writing with the v1 DataFrame API in Spark 3, use `saveAsTable` or `insertInto` to load tables with a catalog.
    Using `format("iceberg")` loads an isolated table reference that will not automatically refresh tables used by queries.


### Appending data

To append a dataframe to an Iceberg table, use `append`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").append()
```

#### Spark 2.4

In Spark 2.4, use the v1 API with `append` mode and `iceberg` format:

```scala
data.write
    .format("iceberg")
    .mode("append")
    .save("db.table")
```

### Overwriting data

To overwrite partitions dynamically, use `overwritePartitions()`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").overwritePartitions()
```

To explicitly overwrite partitions, use `overwrite` to supply a filter:

```scala
data.writeTo("prod.db.table").overwrite($"level" === "INFO")
```

#### Spark 2.4

In Spark 2.4, overwrite values in an Iceberg table with `overwrite` mode and `iceberg` format:

```scala
data.write
    .format("iceberg")
    .mode("overwrite")
    .save("db.table")
```

!!! Warning
    **The behavior of overwrite mode changed between Spark 2.4 and Spark 3**.

The behavior of DataFrameWriter overwrite mode was undefined in Spark 2.4, but is required to overwrite the entire table in Spark 3. Because of this new requirement, the Iceberg source's behavior changed in Spark 3. In Spark 2.4, the behavior was to dynamically overwrite partitions. To use the Spark 2.4 behavior, add option `overwrite-mode=dynamic`.

### Creating tables

To run a CTAS or RTAS, use `create`, `replace`, or `createOrReplace` operations:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").create()
```

Create and replace operations support table configuration methods, like `partitionedBy` and `tableProperty`:

```scala
data.writeTo("prod.db.table")
    .tableProperty("write.format.default", "orc")
    .partitionBy($"level", days($"ts"))
    .createOrReplace()
```

## Writing against partitioned table

Iceberg requires the data to be sorted according to the partition spec per task (Spark partition) in prior to write
against partitioned table. This applies both Writing with SQL and Writing with DataFrames.

!!! Note
    Explicit sort is necessary because Spark doesn't allow Iceberg to request a sort before writing as of Spark 3.0.
    [SPARK-23889](https://issues.apache.org/jira/browse/SPARK-23889) is filed to enable Iceberg to require specific
    distribution & sort order to Spark.

!!! Note
    Both global sort (`orderBy`/`sort`) and local sort (`sortWithinPartitions`) work for the requirement.

Let's go through writing the data against below sample table:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (days(ts), category)
```

To write data to the sample table, your data needs to be sorted by `days(ts), category`.

If you're inserting data with SQL statement, you can use `ORDER BY` to achieve it, like below:

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
ORDER BY ts, category
```

If you're inserting data with DataFrame, you can use either `orderBy`/`sort` to trigger global sort, or `sortWithinPartitions`
to trigger local sort. Local sort for example:

```scala
data.sortWithinPartitions("ts", "category")
    .writeTo("prod.db.sample")
    .append()
```

You can simply add the original column to the sort condition for the most partition transformations, except `bucket`.

For `bucket` partition transformation, you need to register the Iceberg transform function in Spark to specify it during sort.

Let's go through another sample table having bucket partition:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id))
```

You need to register the function to deal with bucket, like below:

```scala
import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.types.DataTypes

IcebergSpark.registerBucketUDF(spark, "iceberg_bucket16", DataTypes.LongType, 16)
```

!!! Note
    Explicit registration of the function is necessary because Spark doesn't allow Iceberg to provide functions.
    [SPARK-27658](https://issues.apache.org/jira/browse/SPARK-27658) is filed to enable Iceberg to provide functions
    which can be used in query.

Here we just registered the bucket function as `iceberg_bucket16`, which can be used in sort clause.

If you're inserting data with SQL statement, you can use the function like below:

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
ORDER BY iceberg_bucket16(id)
```

If you're inserting data with DataFrame, you can use the function like below:

```scala
data.sortWithinPartitions(expr("iceberg_bucket16(id)"))
    .writeTo("prod.db.sample")
    .append()
```

## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table.history`.

!!! Note
    As of Spark 3.0, the format of the table name for inspection (`catalog.database.table.metadata`) doesn't work with Spark's default catalog (`spark_catalog`). If you've replaced the default catalog, you may want to use DataFrameReader API to inspect the table.

### History

To show table history, run:

```sql
SELECT * FROM prod.db.table.history
```
```text
+-------------------------+---------------------+---------------------+---------------------+
| made_current_at         | snapshot_id         | parent_id           | is_current_ancestor |
+-------------------------+---------------------+---------------------+---------------------+
| 2019-02-08 03:29:51.215 | 5781947118336215154 | NULL                | true                |
| 2019-02-08 03:47:55.948 | 5179299526185056830 | 5781947118336215154 | true                |
| 2019-02-09 16:24:30.13  | 296410040247533544  | 5179299526185056830 | false               |
| 2019-02-09 16:32:47.336 | 2999875608062437330 | 5179299526185056830 | true                |
| 2019-02-09 19:42:03.919 | 8924558786060583479 | 2999875608062437330 | true                |
| 2019-02-09 19:49:16.343 | 6536733823181975045 | 8924558786060583479 | true                |
+-------------------------+---------------------+---------------------+---------------------+
```

!!! Note
    **This shows a commit that was rolled back.** The example has two snapshots with the same parent, and one is *not* an ancestor of the current table state.

### Snapshots

To show the valid snapshots for a table, run:

```sql
SELECT * FROM prod.db.table.snapshots
```
```text
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
| committed_at            | snapshot_id    | parent_id | operation | manifest_list                                      | summary                                               |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | { added-records -> 2478404, total-records -> 2478404, |
|                         |                |           |           |                                                    |   added-data-files -> 438, total-data-files -> 438,   |
|                         |                |           |           |                                                    |   spark.app.id -> application_1520379288616_155055 }  |
| ...                     | ...            | ...       | ...       | ...                                                | ...                                                   |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
```

You can also join snapshots to table history. For example, this query will show table history, with the application ID that wrote each snapshot:

```sql
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['spark.app.id']
from prod.db.table.history h
join prod.db.table.snapshots s
  on h.snapshot_id = s.snapshot_id
order by made_current_at
```
```text
+-------------------------+-----------+----------------+---------------------+----------------------------------+
| made_current_at         | operation | snapshot_id    | is_current_ancestor | summary[spark.app.id]            |
+-------------------------+-----------+----------------+---------------------+----------------------------------+
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-09 16:24:30.13  | delete    | 29641004024753 | false               | application_1520379288616_151109 |
| 2019-02-09 16:32:47.336 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-08 03:47:55.948 | overwrite | 51792995261850 | true                | application_1520379288616_152431 |
+-------------------------+-----------+----------------+---------------------+----------------------------------+
```

### Manifests

To show a table's file manifests and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.manifests
```
```text
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| path                                                                 | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions                      |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,2019-05-13,2019-05-15]] |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
```

### Files

To show a table's data files and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.files
```
```text
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
| file_path                                                               | file_format | record_count | file_size_in_bytes | column_sizes       | value_counts     | null_value_counts | lower_bounds    | upper_bounds    | key_metadata | split_offsets |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
| s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           |
| s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           |
| s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
```

### Inspecting with DataFrames

Metadata tables can be loaded in Spark 2.4 or Spark 3 using the DataFrameReader API:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table.files").show(truncate = false)
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table#files").show(truncate = false)
```
