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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkTableOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
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
    private TableSchema projectedSchema;
    private ReadableConfig readableConfig = new Configuration();
    private final ScanContext.Builder contextBuilder = ScanContext.builder();

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public Builder filters(List<Expression> filters) {
      contextBuilder.filters(filters);
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder limit(long newLimit) {
      contextBuilder.limit(newLimit);
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      contextBuilder.fromProperties(properties);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      contextBuilder.caseSensitive(caseSensitive);
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      contextBuilder.useSnapshotId(snapshotId);
      return this;
    }

    public Builder startSnapshotId(Long startSnapshotId) {
      contextBuilder.startSnapshotId(startSnapshotId);
      return this;
    }

    public Builder endSnapshotId(Long endSnapshotId) {
      contextBuilder.endSnapshotId(endSnapshotId);
      return this;
    }

    public Builder asOfTimestamp(Long asOfTimestamp) {
      contextBuilder.asOfTimestamp(asOfTimestamp);
      return this;
    }

    public Builder splitSize(Long splitSize) {
      contextBuilder.splitSize(splitSize);
      return this;
    }

    public Builder splitLookback(Integer splitLookback) {
      contextBuilder.splitLookback(splitLookback);
      return this;
    }

    public Builder splitOpenFileCost(Long splitOpenFileCost) {
      contextBuilder.splitOpenFileCost(splitOpenFileCost);
      return this;
    }

    public Builder streaming(boolean streaming) {
      contextBuilder.streaming(streaming);
      return this;
    }

    public Builder nameMapping(String nameMapping) {
      contextBuilder.nameMapping(nameMapping);
      return this;
    }

    public Builder flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open();
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

      if (projectedSchema == null) {
        contextBuilder.project(icebergSchema);
      } else {
        contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
      }

      DataType[] dataTypes = projectedSchema != null ? projectedSchema.getFieldDataTypes() :
          FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(table.schema())).getFieldDataTypes();
      return new FlinkInputFormat(tableLoader, icebergSchema, io, encryption, contextBuilder.build(), dataTypes,
          readableConfig);
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();

      ScanContext context = contextBuilder.build();
      TypeInformation<RowData> typeInfo = FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(context.project()));

      if (!context.isStreaming()) {
        int parallelism = inferParallelism(format, context);
        return env.createInput(format, typeInfo).setParallelism(parallelism);
      } else {
        StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader, context);

        String monitorFunctionName = String.format("Iceberg table (%s) monitor", table);
        String readerOperatorName = String.format("Iceberg table (%s) reader", table);

        return env.addSource(function, monitorFunctionName)
            .transform(readerOperatorName, typeInfo, StreamingReaderOperator.factory(format));
      }
    }

    int inferParallelism(FlinkInputFormat format, ScanContext context) {
      int parallelism = readableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
      if (readableConfig.get(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM)) {
        int maxInferParallelism = readableConfig.get(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX);
        Preconditions.checkState(maxInferParallelism >= 1,
            FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX.key() + " cannot be less than 1");
        int splitNum;
        try {
          FlinkInputSplit[] splits = format.createInputSplits(0);
          splitNum = splits.length;
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to create iceberg input splits for table: " + table, e);
        }

        parallelism = Math.min(splitNum, maxInferParallelism);
      }

      if (context.limit() > 0) {
        int limit = context.limit() >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) context.limit();
        parallelism = Math.min(parallelism, limit);
      }

      // parallelism must be positive.
      parallelism = Math.max(1, parallelism);
      return parallelism;
    }
  }

  public static boolean isBounded(Map<String, String> properties) {
    return !ScanContext.builder().fromProperties(properties).build().isStreaming();
  }
}
