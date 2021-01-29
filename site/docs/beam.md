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

# Apache Beam

!!! Warning
    The Beam API is experimental and in early alpha stage

## Write records

When sinking Avro files to a distributed file system using `FileIO`, we can catch rely the filenames downstream. We append these filenames to an Iceberg table incrementally.

```java
final String hiveMetastoreUrl = "thrift://localhost:9083/default";
final FileIO.Write<Void, GenericRecord> avroFileIO = FileIO.<GenericRecord>write()
        .via(AvroIO.sink(avroSchema))
        .to("gs://.../../")
        .withSuffix(".avro");

final WriteFilesResult<Void> filesWritten = records.apply(avroFileIO);
final org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
final TableIdentifier name = TableIdentifier.of("default", "test");

IcebergIO.write(name, icebergSchema, hiveMetastoreUrl, filesWritten);
```