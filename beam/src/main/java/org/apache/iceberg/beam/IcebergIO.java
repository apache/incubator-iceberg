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

package org.apache.iceberg.beam;


import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergIO {
  private IcebergIO() {
  }

  public static final class Builder {
    private TableIdentifier tableIdentifier;
    private Schema schema;
    private String hiveMetaStoreUrl;

    private final Map<String, String> properties = Maps.newHashMap();

    public Builder withTableIdentifier(TableIdentifier newTableIdentifier) {
      this.tableIdentifier = newTableIdentifier;
      return this;
    }

    public Builder withSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public Builder withHiveMetastoreUrl(String newHiveMetaStoreUrl) {
      assert newHiveMetaStoreUrl.startsWith("thrift://");
      this.hiveMetaStoreUrl = newHiveMetaStoreUrl;
      return this;
    }

    public Builder conf(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public <T extends GenericRecord> PCollection<Snapshot> build(PCollection<T> avroRecords) {
      return IcebergIO.write(
          avroRecords,
          this.tableIdentifier,
          this.schema,
          this.hiveMetaStoreUrl,
          this.properties
      );
    }
  }

  private static <T extends GenericRecord> PCollection<Snapshot> write(
      PCollection<T> avroRecords,
      TableIdentifier table,
      Schema schema,
      String hiveMetastoreUrl,
      Map<String, String> properties
  ) {
    // We take the filenames that are emitted by the FileIO
    final PCollection<DataFile> dataFiles = avroRecords
        .apply(
            "Write DataFiles",
            ParDo.of(new FileWriter<>(table, schema, PartitionSpec.unpartitioned(), hiveMetastoreUrl, properties))
        )
        .setCoder(SerializableCoder.of(DataFile.class));

    // We use a combiner, to combine all the files to a single commit in
    // the Iceberg log
    final IcebergDataFileCommitter combiner = new IcebergDataFileCommitter(table, hiveMetastoreUrl, properties);
    final Combine.Globally<DataFile, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

    // We return the latest snapshot, which can be used to notify downstream consumers.
    return dataFiles.apply("Commit DataFiles", combined);
  }
}

