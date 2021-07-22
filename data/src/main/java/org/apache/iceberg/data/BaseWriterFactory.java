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

package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriterFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public abstract class BaseWriterFactory<T> implements WriterFactory<T> {
  private final Map<String, String> properties;
  private final Schema dataSchema;
  private final int[] equalityFieldIds;
  private final Schema equalityDeleteRowSchema;
  private final Schema positionDeleteRowSchema;

  protected BaseWriterFactory(Map<String, String> properties, Schema dataSchema,
                              int[] equalityFieldIds, Schema equalityDeleteRowSchema,
                              Schema positionDeleteRowSchema) {
    this.properties = properties;
    this.dataSchema = dataSchema;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.positionDeleteRowSchema = positionDeleteRowSchema;
  }

  protected abstract void configureDataWrite(Avro.DataWriteBuilder builder);
  protected abstract void configureEqualityDelete(Avro.DeleteWriteBuilder builder);
  protected abstract void configurePositionDelete(Avro.DeleteWriteBuilder builder);

  protected abstract void configureDataWrite(Parquet.DataWriteBuilder builder);
  protected abstract void configureEqualityDelete(Parquet.DeleteWriteBuilder builder);
  protected abstract void configurePositionDelete(Parquet.DeleteWriteBuilder builder);

  protected Schema equalityDeleteRowSchema() {
    return equalityDeleteRowSchema;
  }

  protected Schema positionDeleteRowSchema() {
    return positionDeleteRowSchema;
  }

  @Override
  public DataWriter<T> newDataWriter(EncryptedOutputFile file, FileFormat format,
                                     PartitionSpec spec, StructLike partition) {
    Preconditions.checkArgument(spec != null,
        "Spec must not be null when creating a data writer");
    Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    OutputFile outputFile = file.encryptingOutputFile();
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(properties);

    try {
      switch (format) {
        case AVRO:
          Avro.DataWriteBuilder avroBuilder = Avro.writeData(outputFile)
              .setAll(properties)
              .metricsConfig(metricsConfig)
              .schema(dataSchema)
              .spec(spec)
              .partition(partition)
              .keyMetadata(keyMetadata)
              .overwrite();

          configureDataWrite(avroBuilder);

          return avroBuilder.buildDataWriter();

        case PARQUET:
          Parquet.DataWriteBuilder parquetBuilder = Parquet.writeData(outputFile)
              .setAll(properties)
              .metricsConfig(metricsConfig)
              .schema(dataSchema)
              .spec(spec)
              .partition(partition)
              .keyMetadata(keyMetadata)
              .overwrite();

          configureDataWrite(parquetBuilder);

          return parquetBuilder.buildDataWriter();

        default:
          throw new UnsupportedOperationException("Cannot write data for unsupported file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<T> newEqualityDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                         PartitionSpec spec, StructLike partition) {
    Preconditions.checkArgument(spec != null,
        "Spec must not be null when creating an equality delete writer");
    Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");
    Preconditions.checkArgument(equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field IDs must not be null or empty when creating an equality delete writer");
    Preconditions.checkArgument(equalityDeleteRowSchema != null,
        "Equality delete row schema must not be null when creating an equality delete writer");

    OutputFile outputFile = file.encryptingOutputFile();
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();

    try {
      switch (format) {
        case AVRO:
          Avro.DeleteWriteBuilder avroBuilder = Avro.writeDeletes(outputFile)
              .overwrite()
              .rowSchema(equalityDeleteRowSchema)
              .spec(spec)
              .partition(partition)
              .equalityFieldIds(equalityFieldIds)
              .keyMetadata(keyMetadata);

          configureEqualityDelete(avroBuilder);

          return avroBuilder.buildEqualityWriter();

        case PARQUET:
          Parquet.DeleteWriteBuilder parquetBuilder = Parquet.writeDeletes(outputFile)
              .overwrite()
              .rowSchema(equalityDeleteRowSchema)
              .spec(spec)
              .partition(partition)
              .equalityFieldIds(equalityFieldIds)
              .keyMetadata(keyMetadata);

          configureEqualityDelete(parquetBuilder);

          return parquetBuilder.buildEqualityWriter();

        default:
          throw new UnsupportedOperationException("Cannot write equality deletes for unsupported format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }

  @Override
  public PositionDeleteWriter<T> newPositionDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                         PartitionSpec spec, StructLike partition) {
    Preconditions.checkArgument(spec != null,
        "Spec must not be null when creating a position delete writer");
    Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    OutputFile outputFile = file.encryptingOutputFile();
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();

    try {
      switch (format) {
        case AVRO:
          Avro.DeleteWriteBuilder avroBuilder = Avro.writeDeletes(outputFile)
              .overwrite()
              .rowSchema(positionDeleteRowSchema)
              .spec(spec)
              .partition(partition)
              .keyMetadata(keyMetadata);

          configurePositionDelete(avroBuilder);

          return avroBuilder.buildPositionWriter();

        case PARQUET:
          Parquet.DeleteWriteBuilder parquetBuilder = Parquet.writeDeletes(outputFile)
              .overwrite()
              .rowSchema(positionDeleteRowSchema)
              .spec(spec)
              .partition(partition)
              .keyMetadata(keyMetadata);

          configurePositionDelete(parquetBuilder);

          return parquetBuilder.buildPositionWriter();

        default:
          throw new UnsupportedOperationException("Cannot write position deletes for unsupported format: " + format);
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new position delete writer", e);
    }
  }
}
