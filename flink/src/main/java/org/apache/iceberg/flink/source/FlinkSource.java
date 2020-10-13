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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkSource {
  private FlinkSource() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}.
   * See more options in {@link ScanContext}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and
   * read records incrementally.
   * <ul>
   *   <li>Without startSnapshotId: Bounded</li>
   *   <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  /**
   * Source builder to build {@link DataStream}.
   */
  public static class Builder {
    private StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private Configuration hadoopConf;
    private TableSchema projectedSchema;
    private ScanContext context = new ScanContext();

    private RowDataTypeInfo rowTypeInfo;

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder hadoopConf(Configuration newConf) {
      this.hadoopConf = newConf;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public Builder filters(List<Expression> filters) {
      this.context = context.filterRows(filters);
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      this.context = context.fromProperties(properties);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      this.context = context.setCaseSensitive(caseSensitive);
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      this.context = context.useSnapshotId(snapshotId);
      return this;
    }

    public Builder startSnapshotId(Long startSnapshotId) {
      this.context = context.startSnapshotId(startSnapshotId);
      return this;
    }

    public Builder endSnapshotId(Long endSnapshotId) {
      this.context = context.endSnapshotId(endSnapshotId);
      return this;
    }

    public Builder asOfTimestamp(Long asOfTimestamp) {
      this.context = context.asOfTimestamp(asOfTimestamp);
      return this;
    }

    public Builder splitSize(Long splitSize) {
      this.context = context.splitSize(splitSize);
      return this;
    }

    public Builder splitLookback(Integer splitLookback) {
      this.context = context.splitLookback(splitLookback);
      return this;
    }

    public Builder splitOpenFileCost(Long splitOpenFileCost) {
      this.context = context.splitOpenFileCost(splitOpenFileCost);
      return this;
    }

    public Builder nameMapping(String nameMapping) {
      this.context = context.nameMapping(nameMapping);
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      hadoopConf = hadoopConf == null ? FlinkCatalogFactory.clusterHadoopConf() : hadoopConf;

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open(hadoopConf);
        try (TableLoader loader = tableLoader) {
          table = loader.loadTable();
          icebergSchema = table.schema();
          io = table.io();
          encryption = table.encryption();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        icebergSchema = table.schema();
        io = table.io();
        encryption = table.encryption();
      }

      rowTypeInfo = RowDataTypeInfo.of((RowType) (
          projectedSchema == null ?
              FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema)) :
              projectedSchema).toRowDataType().getLogicalType());

      context = context.project(projectedSchema == null ? icebergSchema :
          FlinkSchemaUtil.convert(icebergSchema, projectedSchema));

      return new FlinkInputFormat(tableLoader, new SerializableConfiguration(hadoopConf), io, encryption, context);
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();
      if (isBounded(context)) {
        return env.createInput(format, rowTypeInfo);
      } else {
        throw new UnsupportedOperationException("The Unbounded mode is not supported yet");
      }
    }
  }

  private static boolean isBounded(ScanContext context) {
    return context.startSnapshotId() == null || context.endSnapshotId() != null;
  }

  public static boolean isBounded(Map<String, String> properties) {
    return isBounded(new ScanContext().fromProperties(properties));
  }
}
