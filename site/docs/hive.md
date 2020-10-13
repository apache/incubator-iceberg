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

# Hive

## Hive read support
Iceberg supports the reading of Iceberg tables from [Hive](https://hive.apache.org) by using a [StorageHandler](https://cwiki.apache.org/confluence/display/Hive/StorageHandlers). Please note that only Hive 2.x versions are currently supported.

### Table creation
This section explains the various steps needed in order to overlay a Hive table "on top of" an existing Iceberg table. Iceberg tables are created using either a [`Catalog`](/javadoc/master/index.html?org/apache/iceberg/catalog/Catalog.html) or an implementation of the [`Tables`](/javadoc/master/index.html?org/apache/iceberg/Tables.html) interface and Hive needs to be configured accordingly to read data from these different types of table.

#### Add the Iceberg Hive Runtime jar file to the Hive classpath
Regardless of the table type, the `HiveIcebergStorageHandler` and supporting classes need to be made available on Hive's classpath. These are provided by the `iceberg-hive-runtime` jar file. For example, if using the Hive shell, this can be achieved by issuing a statement like so:
```sql
add jar /path/to/iceberg-hive-runtime.jar;
```
There are many others ways to achieve this including adding the jar file to Hive's auxillary classpath (so it is available by default) - please refer to Hive's documentation for more information.

#### Using Hadoop Tables
Iceberg tables created using `HadoopTables` are stored entirely in a directory in a filesytem like HDFS. 

##### Create an Iceberg table
The first step is to create an Iceberg table using the Spark/Java/Python API and `HadoopTables`. For the purposes of this documentation we will assume that the table is called `table_a` and that the table location is `hdfs://some_path/table_a`.

##### Create a Hive table
Now overlay a Hive table on top of this Iceberg table by issuing Hive DDL like so:
```sql
CREATE EXTERNAL TABLE table_a 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 'hdfs://some_bucket/some_path/table_a';
```

#### Query the Iceberg table via Hive
You should now be able to issue Hive SQL `SELECT` queries using the above table and see the results returned from the underlying Iceberg table. Both the Map Reduce and Tez query execution engines are supported.
```sql
SELECT * from table_a;
```

### Features

#### Predicate pushdown
Pushdown of the Hive SQL `WHERE` clause has been implemented so that these filters are used at the Iceberg TableScan level as well as by the Parquet and ORC Readers.
