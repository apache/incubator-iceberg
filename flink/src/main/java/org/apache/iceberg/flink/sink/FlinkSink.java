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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class FlinkSink {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSink.class);

  private static final String ICEBERG_STREAM_WRITER_NAME = IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME = IcebergFilesCommitter.class.getSimpleName();

  private FlinkSink() {
  }

  /**
   * Initialize a {@link Builder} to export the data from generic input data stream into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a mapper function and a
   * {@link TypeInformation} to convert those generic records to a RowData DataStream.
   *
   * @param input      the generic source input data stream.
   * @param mapper     function to convert the generic data to {@link RowData}
   * @param outputType to define the {@link TypeInformation} for the input data.
   * @param <T>        the data type of records.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static <T> Builder builderFor(DataStream<T> input,
                                       MapFunction<T, RowData> mapper,
                                       TypeInformation<RowData> outputType) {
    DataStream<RowData> dataStream = input.map(mapper, outputType);
    return forRowData(dataStream);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link Row}s into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a {@link TableSchema} for builder to convert
   * those {@link Row}s to a {@link RowData} DataStream.
   *
   * @param input       the source input data stream with {@link Row}s.
   * @param tableSchema defines the {@link TypeInformation} for input data.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRow(DataStream<Row> input, TableSchema tableSchema) {
    RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
    DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();

    DataFormatConverters.RowConverter rowConverter = new DataFormatConverters.RowConverter(fieldDataTypes);
    return builderFor(input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
        .tableSchema(tableSchema);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  public static class Builder {
    private DataStream<RowData> rowDataInput = null;
    private TableLoader tableLoader;
    private Table table;
    private TableSchema tableSchema;
    private boolean overwrite = false;
    private DistributionMode distributionMode = null;
    private Integer writeParallelism = null;
    private List<String> equalityFieldColumns = null;
    private String uidPrefix = null;

    private Builder() {
    }

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.rowDataInput = newRowDataInput;
      return this;
    }

    /**
     * This iceberg {@link Table} instance is used for initializing {@link IcebergStreamWriter} which will write all
     * the records into {@link DataFile}s and emit them to downstream operator. Providing a table would avoid so many
     * table loading from each separate task.
     *
     * @param newTable the loaded iceberg table instance.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    /**
     * The table loader is used for loading tables in {@link IcebergFilesCommitter} lazily, we need this loader because
     * {@link Table} is not serializable and could not just use the loaded table from Builder#table in the remote task
     * manager.
     *
     * @param newTableLoader to load iceberg table inside tasks.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder tableLoader(TableLoader newTableLoader) {
      this.tableLoader = newTableLoader;
      return this;
    }

    public Builder tableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    public Builder overwrite(boolean newOverwrite) {
      this.overwrite = newOverwrite;
      return this;
    }

    /**
     * Configure the write {@link DistributionMode} that the flink sink will use. Currently, flink support
     * {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
     *
     * @param mode to specify the write distribution mode.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder distributionMode(DistributionMode mode) {
      Preconditions.checkArgument(!DistributionMode.RANGE.equals(mode),
          "Flink does not support 'range' write distribution mode now.");
      this.distributionMode = mode;
      return this;
    }

    /**
     * Configuring the write parallel number for iceberg stream writer.
     *
     * @param newWriteParallelism the number of parallel iceberg stream writer.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder writeParallelism(int newWriteParallelism) {
      this.writeParallelism = newWriteParallelism;
      return this;
    }

    /**
     * Configuring the equality field columns for iceberg table that accept CDC or UPSERT events.
     *
     * @param columns defines the iceberg table's key.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder equalityFieldColumns(List<String> columns) {
      this.equalityFieldColumns = columns;
      return this;
    }

    /**
     * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of multiple operators (like
     * writer, committer, dummy sink etc.) Actually operator uid will be appended with a suffix like "uid-writer".
     * <p>
     * Flink auto generates operator uids if not set explicitly. It is a recommended
     * <a href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
     * best-practice to set uid for all operators</a> before deploying to production. Flink has an option to {@code
     * pipeline.auto-generate-uids=false} to disable auto-generation and force explicit setting of all operator uids.
     * <p>
     * Be careful with setting this for an existing job, because now we are changing the opeartor uid from an
     * auto-generated one to this new value. When deploying the change with a checkpoint, Flink won't be able to restore
     * the previous Flink sink operator state (more specifically the committer operator state). You need to use {@code
     * --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink state is used to check if
     * checkpointed files were actually committed or not. {@code --allowNonRestoredState} can lead to data loss if the
     * Iceberg commit failed in the last completed checkpoints.
     *
     * @param newPrefix UID prefix for Flink sink operators
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder uidPrefix(String newPrefix) {
      this.uidPrefix = newPrefix;
      return this;
    }

    @SuppressWarnings("unchecked")
    public DataStreamSink<RowData> build() {
      Preconditions.checkArgument(rowDataInput != null,
          "Please use forRowData() to initialize the input DataStream.");
      Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

      if (table == null) {
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          this.table = loader.loadTable();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to load iceberg table from table loader: " + tableLoader, e);
        }
      }

      // Find out the equality field id list based on the user-provided equality field column names.
      List<Integer> equalityFieldIds = Lists.newArrayList();
      if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
        for (String column : equalityFieldColumns) {
          org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
          Preconditions.checkNotNull(field, "Missing required equality field column '%s' in table schema %s",
              column, table.schema());
          equalityFieldIds.add(field.fieldId());
        }
      }

      // Convert the requested flink table schema to flink row type.
      RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);

      // Distribute the records from input data stream based on the write.distribution-mode.
      rowDataInput = distributeDataStream(rowDataInput, table.properties(), table.spec(), table.schema(), flinkRowType);

      // Chain the iceberg stream writer and committer operator.
      IcebergStreamWriter<RowData> streamWriter = createStreamWriter(table, flinkRowType, equalityFieldIds);
      IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, overwrite);

      this.writeParallelism = writeParallelism == null ? rowDataInput.getParallelism() : writeParallelism;

      SingleOutputStreamOperator<WriteResult> writerStream = rowDataInput
          .transform(ICEBERG_STREAM_WRITER_NAME, TypeInformation.of(WriteResult.class), streamWriter)
          .setParallelism(writeParallelism);
      if (uidPrefix != null) {
        writerStream = writerStream.uid(uidPrefix + "-writer");
      }

      SingleOutputStreamOperator<Void> committerStream = writerStream
          .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
          .setParallelism(1)
          .setMaxParallelism(1);
      if (uidPrefix != null) {
        committerStream = committerStream.uid(uidPrefix + "-committer");
      }

      DataStreamSink<RowData> resultStream = committerStream
          .addSink(new DiscardingSink())
          .name(String.format("IcebergSink %s", table.name()))
          .setParallelism(1);
      if (uidPrefix != null) {
        resultStream = resultStream.uid(uidPrefix + "-dummysink");
      }
      return resultStream;
    }

    private DataStream<RowData> distributeDataStream(DataStream<RowData> input,
                                                     Map<String, String> properties,
                                                     PartitionSpec partitionSpec,
                                                     Schema iSchema,
                                                     RowType flinkRowType) {
      DistributionMode writeMode;
      if (distributionMode == null) {
        // Fallback to use distribution mode parsed from table properties if don't specify in job level.
        String modeName = PropertyUtil.propertyAsString(properties,
            WRITE_DISTRIBUTION_MODE,
            WRITE_DISTRIBUTION_MODE_DEFAULT);

        writeMode = DistributionMode.fromName(modeName);
      } else {
        writeMode = distributionMode;
      }

      switch (writeMode) {
        case NONE:
          return input;

        case HASH:
          if (partitionSpec.isUnpartitioned()) {
            return input;
          } else {
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }

        case RANGE:
          LOG.warn("Fallback to use 'none' distribution mode, because {}={} is not supported in flink now",
              WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName());
          return input;

        default:
          throw new RuntimeException("Unrecognized write.distribution-mode: " + writeMode);
      }
    }
  }

  static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
      TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
      // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
      // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
      // schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  static IcebergStreamWriter<RowData> createStreamWriter(Table table,
                                                         RowType flinkRowType,
                                                         List<Integer> equalityFieldIds) {
    Map<String, String> props = table.properties();
    long targetFileSize = getTargetFileSizeBytes(props);
    FileFormat fileFormat = getFileFormat(props);

    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(table, flinkRowType,
        table.locationProvider(), table.io(), table.encryption(),
        targetFileSize, fileFormat, props, equalityFieldIds);

    return new IcebergStreamWriter<>(table.name(), taskWriterFactory);
  }

  private static FileFormat getFileFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private static long getTargetFileSizeBytes(Map<String, String> properties) {
    return PropertyUtil.propertyAsLong(properties,
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }
}
