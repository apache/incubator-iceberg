<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# ![Iceberg](img/Iceberg-logo.png)


**Apache Iceberg is an open table format for huge analytic datasets.** Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink and Hive using a high-performance format that works just like a SQL table.

### User experience

Iceberg avoids unpleasant surprises. Schema evolution works and won't inadvertently un-delete data. Users don't need to know about partitioning to get fast queries.

* [Schema evolution](evolution#schema-evolution) supports add, drop, update, or rename, and has [no side-effects](evolution#correctness)
* [Hidden partitioning](partitioning) prevents user mistakes that cause silently incorrect results or extremely slow queries
* [Partition layout evolution](evolution#partition-evolution) can update the layout of a table as data volume or query patterns change
* [Time travel](spark#time-travel) enables reproducible queries that use exactly the same table snapshot, or lets users easily examine changes
* Version rollback allows users to quickly correct problems by resetting tables to a good state

### Reliability and performance

Iceberg was built for huge tables. Iceberg is used in production where a single table can contain tens of petabytes of data and even these huge tables can be read without a distributed SQL engine.

* [Scan planning is fast](performance#scan-planning) -- a distributed SQL engine isn't needed to read a table or find files
* [Advanced filtering](performance#data-filtering) -- data files are pruned with partition and column-level stats, using table metadata

Iceberg was designed to solve correctness problems in eventually-consistent cloud object stores.

* [Works with any cloud store](reliability) and reduces NN congestion when in HDFS, by avoiding listing and renames
* [Serializable isolation](reliability) -- table changes are atomic and readers never see partial or uncommitted changes
* [Multiple concurrent writers](reliability#concurrent-write-operations) use optimistic concurrency and will retry to ensure that compatible updates succeed, even when writes conflict

### Open standard

Iceberg has been designed and developed to be an open community standard with a [specification](spec) to ensure compatibility across languages and implementations.

[Apache Iceberg is open source](community), and is developed at the [Apache Software Foundation](https://www.apache.org/).

