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
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class FlinkSink {

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
    return builderFor(input, rowConverter::toInternal, RowDataTypeInfo.of(rowType))
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
    private Integer writeParallelism = null;
    private boolean upsert = false;
    private List<String> equalityFieldColumns = null;

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
     * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which means it will
     * DELETE the old records and then INSERT the new records. In partitioned table, the partition fields should be
     * a subset of equality fields, otherwise the old row that located in partition-A could not be deleted by the
     * new row that located in partition-B.
     *
     * @param enable indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder upsert(boolean enable) {
      this.upsert = enable;
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

      // Convert the iceberg schema to flink's RowType.
      RowType flinkSchema = convertToRowType(table, tableSchema);

      // Convert the INSERT stream to be an UPSERT stream if needed.
      if (upsert) {
        Preconditions.checkState(!equalityFieldIds.isEmpty(),
            "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
        if (!table.spec().isUnpartitioned()) {
          for (PartitionField partitionField : table.spec().fields()) {
            Preconditions.checkState(equalityFieldIds.contains(partitionField.sourceId()),
                "Partition field '%s' is not included in equality fields: '%s'", partitionField, equalityFieldColumns);
          }
        }
      }

      IcebergStreamWriter<RowData> streamWriter = createStreamWriter(table, flinkSchema, equalityFieldIds, upsert);
      IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, overwrite);

      this.writeParallelism = writeParallelism == null ? rowDataInput.getParallelism() : writeParallelism;

      DataStream<Void> returnStream = rowDataInput
          .transform(ICEBERG_STREAM_WRITER_NAME, TypeInformation.of(WriteResult.class), streamWriter)
          .setParallelism(writeParallelism)
          .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
          .setParallelism(1)
          .setMaxParallelism(1);

      return returnStream.addSink(new DiscardingSink())
          .name(String.format("IcebergSink %s", table.name()))
          .setParallelism(1);
    }
  }

  private static RowType convertToRowType(Table table, TableSchema requestedSchema) {
    Preconditions.checkArgument(table != null, "Iceberg table should't be null");

    RowType flinkSchema;
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), table.schema());
      TypeUtil.validateWriteSchema(table.schema(), writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
      // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
      // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
      // schema.
      flinkSchema = (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      flinkSchema = FlinkSchemaUtil.convert(table.schema());
    }

    return flinkSchema;
  }

  static IcebergStreamWriter<RowData> createStreamWriter(Table table,
                                                         RowType flinkSchema,
                                                         List<Integer> equalityFieldIds,
                                                         boolean upsert) {
    Preconditions.checkArgument(table != null, "Iceberg table should't be null");

    Map<String, String> props = table.properties();
    long targetFileSize = getTargetFileSizeBytes(props);
    FileFormat fileFormat = getFileFormat(props);

    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(table.schema(), flinkSchema,
        table.spec(), table.locationProvider(), table.io(), table.encryption(), targetFileSize, fileFormat, props,
        equalityFieldIds, upsert);

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
