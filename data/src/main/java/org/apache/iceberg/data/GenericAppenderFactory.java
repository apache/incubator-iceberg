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
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Factory to create a new {@link FileAppender} to write {@link Record}s.
 */
public class GenericAppenderFactory implements FileAppenderFactory<Record> {

  private final Table table;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Map<String, String> config = Maps.newHashMap();

  public GenericAppenderFactory(Table table) {
    this(table, null, null, null);
  }

  public GenericAppenderFactory(Table table,
                                int[] equalityFieldIds,
                                Schema eqDeleteRowSchema,
                                Schema posDeleteRowSchema) {
    Preconditions.checkNotNull(table, "Table must not be null");
    if (equalityFieldIds != null) {
      Preconditions.checkNotNull(eqDeleteRowSchema, "Equality Field Ids and Equality Delete Row Schema" +
          " must be set together");
    }
    if (eqDeleteRowSchema != null) {
      Preconditions.checkNotNull(equalityFieldIds, "Equality Field Ids and Equality Delete Row Schema" +
          " must be set together");
    }

    this.table = table;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  public GenericAppenderFactory set(String property, String value) {
    config.put(property, value);
    return this;
  }

  public GenericAppenderFactory setAll(Map<String, String> properties) {
    config.putAll(properties);
    return this;
  }

  @Override
  public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromTable(table);
    try {
      switch (fileFormat) {
        case AVRO:
          return Avro.write(outputFile)
              .schema(table.schema())
              .createWriterFunc(DataWriter::create)
              .metricsConfig(metricsConfig)
              .setAll(config)
              .overwrite()
              .build();

        case PARQUET:
          return Parquet.write(outputFile)
              .schema(table.schema())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .setAll(config)
              .metricsConfig(metricsConfig)
              .overwrite()
              .build();

        case ORC:
          return ORC.write(outputFile)
              .schema(table.schema())
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .setAll(config)
              .metricsConfig(metricsConfig)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write unknown file format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public org.apache.iceberg.io.DataWriter<Record> newDataWriter(EncryptedOutputFile file, FileFormat format,
                                                                StructLike partition) {
    return new org.apache.iceberg.io.DataWriter<>(
        newAppender(file.encryptingOutputFile(), format), format,
        file.encryptingOutputFile().location(), table.spec(), partition, file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<Record> newEqDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                        StructLike partition) {
    Preconditions.checkState(equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field ids shouldn't be null or empty when creating equality-delete writer");
    Preconditions.checkNotNull(eqDeleteRowSchema,
        "Equality delete row schema shouldn't be null when creating equality-delete writer");

    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (format) {
        case AVRO:
          return Avro.writeDeletes(file.encryptingOutputFile())
              .createWriterFunc(DataWriter::create)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .rowSchema(eqDeleteRowSchema)
              .withSpec(table.spec())
              .withKeyMetadata(file.keyMetadata())
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        case PARQUET:
          return Parquet.writeDeletes(file.encryptingOutputFile())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .metricsConfig(metricsConfig)
              .rowSchema(eqDeleteRowSchema)
              .withSpec(table.spec())
              .withKeyMetadata(file.keyMetadata())
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        default:
          throw new UnsupportedOperationException(
              "Cannot write equality-deletes for unsupported file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<Record> newPosDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                         StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (format) {
        case AVRO:
          return Avro.writeDeletes(file.encryptingOutputFile())
              .createWriterFunc(DataWriter::create)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .rowSchema(posDeleteRowSchema)
              .withSpec(table.spec())
              .withKeyMetadata(file.keyMetadata())
              .buildPositionWriter();

        case PARQUET:
          return Parquet.writeDeletes(file.encryptingOutputFile())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .metricsConfig(metricsConfig)
              .rowSchema(posDeleteRowSchema)
              .withSpec(table.spec())
              .withKeyMetadata(file.keyMetadata())
              .buildPositionWriter();

        default:
          throw new UnsupportedOperationException("Cannot write pos-deletes for unsupported file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
