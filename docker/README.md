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

# Version

| Framework | Version |
| :--------- | :------- |
| Hadoop    | 2.7.4   |
| Spark     | 2.4.5   |
| Iceberg   | 0.8.0   |

# Setting up Docker Demo

```$xslt
# start the docker demo
cd docker
./start_demo.sh

# You can see the output below if the docker demo starts successfully
Creating network "compose_default" with the default driver
Creating namenode ... done
Creating datanode ... done
Creating spark    ... done
```

# Demo

At this point, you can enter the container and try something with Spark and Iceberg.

```$xslt
docker exec -it spark /bin/bash

# After getting into the container, we start with spark-shell
spark-shell --master local[2]
```

## Test Data

You can check the data located on /opt/data/logs.json, which shows a few records of logging data.

```$xslt
{"level": "INFO", "event_time": 1591430621, "message": "Containers are ready.", "call_stack": []}
{"level": "INFO", "event_time": 1591430621, "message": "Start working with Iceberg!", "call_stack": []}
{"level": "WARN", "event_time": 1591430621, "message": "This is a warn meesage", "call_stack": ["functionA", "functionB"]}
{"level": "ERROR", "event_time": 1591430621, "message": "NullPointerException", "call_stack": ["String.substring(int, int)"]}
{"level": "ERROR", "event_time": 1591430621, "message": "IllegalArgumentException", "call_stack": ["unknow stack"]}
{"level": "INFO", "event_time": 1591430621, "message": "The cluster is shutting donw.", "call_stack": []}
``` 

## Create an Iceberg Table

Let's start with creating an Iceberg table on HDFS.

```$xslt
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types._

val schema = new Schema(
    NestedField.required(1, "level", StringType.get()),
    NestedField.required(2, "event_time", TimestampType.withZone()),
    NestedField.required(3, "message", StringType.get()),
    NestedField.optional(4, "call_stack", ListType.ofRequired(5, StringType.get()))
)

import org.apache.iceberg.PartitionSpec
val spec = PartitionSpec.builderFor(schema).hour("event_time").build()

import org.apache.iceberg.hadoop.HadoopTables
val tables = new HadoopTables(spark.sessionState.newHadoopConf())
val table = tables.create(schema, spec, "hdfs:/tables/logging/logs")
```

## Load Data

The data can loaded as a dataframe with Spark.(We use sparkSchema here because the schema of the dataframe should be 
compatitable with the Iceberg schema we used above.)

```$xslt
import org.apache.spark.sql.types._
val sparkSchema = StructType(Seq(
        StructField("level", StringType, nullable = false),
        StructField("event_time", TimestampType, nullable = false),
        StructField("message", StringType, nullable = false),
        StructField("call_stack", ArrayType(StringType, containsNull = false), nullable = false)
))
val logsDF = spark.read.schema(sparkSchema).json("file:///opt/data/logs.json")
val df = spark.createDataFrame(logsDF.rdd, sparkSchema)
``` 

## Write Data

```$xslt
df.write.format("iceberg").mode("append").save("hdfs:/tables/logging/logs")
```

After this you can exit the spark-shell and check the directory structure on the HDFS.

```$xslt
hadoop fs -ls /tables/logging/logs
```


