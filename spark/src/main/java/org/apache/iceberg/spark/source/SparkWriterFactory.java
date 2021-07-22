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

package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseWriterFactory;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

class SparkWriterFactory extends BaseWriterFactory<InternalRow> {
  private final StructType dataSparkType;
  private StructType lazyEqualityDeleteSparkType = null;
  private StructType lazyPositionDeleteSparkType = null;

  SparkWriterFactory(Map<String, String> properties, Schema dataSchema, StructType dataSparkType,
                     int[] equalityFieldIds, Schema equalityDeleteRowSchema, Schema positionDeleteRowSchema) {
    super(properties, dataSchema, equalityFieldIds, equalityDeleteRowSchema, positionDeleteRowSchema);
    this.dataSparkType = dataSparkType;
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new SparkAvroWriter(dataSparkType));
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new SparkAvroWriter(equalityDeleteSparkType()));
  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new SparkAvroWriter(positionDeleteSparkType()));
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    builder.createWriterFunc(msgType -> SparkParquetWriters.buildWriter(dataSparkType, msgType));
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(msgType -> SparkParquetWriters.buildWriter(equalityDeleteSparkType(), msgType));
  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(msgType -> SparkParquetWriters.buildWriter(positionDeleteSparkType(), msgType));
  }

  private StructType equalityDeleteSparkType() {
    if (lazyEqualityDeleteSparkType == null) {
      Preconditions.checkNotNull(equalityDeleteRowSchema(), "Equality delete row schema shouldn't be null");
      this.lazyEqualityDeleteSparkType = SparkSchemaUtil.convert(equalityDeleteRowSchema());
    }
    return lazyEqualityDeleteSparkType;
  }

  private StructType positionDeleteSparkType() {
    if (lazyPositionDeleteSparkType == null) {
      // wrap the row schema into a position delete schema
      Schema positionDeleteSchema = DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema());
      this.lazyPositionDeleteSparkType = SparkSchemaUtil.convert(positionDeleteSchema);
    }
    return lazyPositionDeleteSparkType;
  }

  static class Builder {
    private final Table table;
    private Schema dataSchema;
    private StructType dataSparkType;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private Schema positionDeleteRowSchema;

    Builder(Table table) {
      this.table = table;
    }

    Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    Builder dataSparkType(StructType newDataSparkType) {
      this.dataSparkType = newDataSparkType;
      return this;
    }

    Builder equalityFieldIds(int[] newEqualityFieldIds) {
      this.equalityFieldIds = newEqualityFieldIds;
      return this;
    }

    Builder equalityDeleteRowSchema(Schema newEqualityDeleteRowSchema) {
      this.equalityDeleteRowSchema = newEqualityDeleteRowSchema;
      return this;
    }

    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    SparkWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new SparkWriterFactory(
          table.properties(), dataSchema, dataSparkType,
          equalityFieldIds, equalityDeleteRowSchema,
          positionDeleteRowSchema);
    }
  }
}
