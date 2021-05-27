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

package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.IcebergSerializationUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SerializationUtil;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 *
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job.getConfiguration());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    Table table = Optional
        .ofNullable(IcebergSerializationUtil.table(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER)))
        .orElseGet(() -> Catalogs.loadTable(conf));

    TableScan scan = table.newScan()
            .caseSensitive(conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT));
    long snapshotId = conf.getLong(InputFormatConfig.SNAPSHOT_ID, -1);
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }
    long asOfTime = conf.getLong(InputFormatConfig.AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }
    long splitSize = conf.getLong(InputFormatConfig.SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }
    String schemaStr = conf.get(InputFormatConfig.READ_SCHEMA);
    if (schemaStr != null) {
      scan.project(SchemaParser.fromJson(schemaStr));
    }
    String[] selectedColumns = conf.getStrings(InputFormatConfig.SELECTED_COLUMNS);
    if (selectedColumns != null) {
      scan.select(selectedColumns);
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter = SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (filter != null) {
      scan = scan.filter(filter);
    }

    List<InputSplit> splits = Lists.newArrayList();
    boolean applyResidual = !conf.getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);
    InputFormatConfig.InMemoryDataModel model = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
        InputFormatConfig.InMemoryDataModel.GENERIC);
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> {
        if (applyResidual && (model == InputFormatConfig.InMemoryDataModel.HIVE ||
            model == InputFormatConfig.InMemoryDataModel.PIG)) {
          // TODO: We do not support residual evaluation for HIVE and PIG in memory data model yet
          checkResiduals(task);
        }
        splits.add(new IcebergSplit(conf, task, table.io(), table.encryption()));
      });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }

    return splits;
  }

  private static void checkResiduals(CombinedScanTask task) {
    task.files().forEach(fileScanTask -> {
      Expression residual = fileScanTask.residual();
      if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter expression %s is not completely satisfied. Additional rows " +
                    "can be returned not satisfied by the filter expression", residual));
      }
    });
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {
    private TaskAttemptContext context;
    private Schema tableSchema;
    private Schema expectedSchema;
    private boolean reuseContainers;
    private boolean caseSensitive;
    private InputFormatConfig.InMemoryDataModel inMemoryDataModel;
    private Iterator<FileScanTask> tasks;
    private T currentRow;
    private CloseableIterator<T> currentIterator;
    private FileIO io;
    private EncryptionManager encryptionManager;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      Configuration conf = newContext.getConfiguration();
      // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
      CombinedScanTask task = ((IcebergSplit) split).task();
      this.context = newContext;
      this.io = ((IcebergSplit) split).io();
      this.encryptionManager = ((IcebergSplit) split).encryptionManager();
      this.tasks = task.files().iterator();
      this.tableSchema = InputFormatConfig.tableSchema(conf);
      this.caseSensitive = conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT);
      this.expectedSchema = readSchema(conf, tableSchema, caseSensitive);
      this.reuseContainers = conf.getBoolean(InputFormatConfig.REUSE_CONTAINERS, false);
      this.inMemoryDataModel = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
              InputFormatConfig.InMemoryDataModel.GENERIC);
      this.currentIterator = open(tasks.next(), expectedSchema).iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          currentRow = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          currentIterator.close();
          currentIterator = open(tasks.next(), expectedSchema).iterator();
        } else {
          currentIterator.close();
          return false;
        }
      }
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return currentRow;
    }

    @Override
    public float getProgress() {
      // TODO: We could give a more accurate progress based on records read from the file. Context.getProgress does not
      // have enough information to give an accurate progress value. This isn't that easy, since we don't know how much
      // of the input split has been processed and we are pushing filters into Parquet and ORC. But we do know when a
      // file is opened and could count the number of rows returned, so we can estimate. And we could also add a row
      // count to the readers so that we can get an accurate count of rows that have been either returned or filtered
      // out.
      return context.getProgress();
    }

    @Override
    public void close() throws IOException {
      currentIterator.close();
    }

    private CloseableIterable<T> openTask(FileScanTask currentTask, Schema readSchema) {
      DataFile file = currentTask.file();
      InputFile inputFile = encryptionManager.decrypt(EncryptedFiles.encryptedInput(
          io.newInputFile(file.path().toString()),
          file.keyMetadata()));

      CloseableIterable<T> iterable;
      switch (file.format()) {
        case AVRO:
          iterable = newAvroIterable(inputFile, currentTask, readSchema);
          break;
        case ORC:
          iterable = newOrcIterable(inputFile, currentTask, readSchema);
          break;
        case PARQUET:
          iterable = newParquetIterable(inputFile, currentTask, readSchema);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot read %s file: %s", file.format().name(), file.path()));
      }

      return iterable;
    }

    @SuppressWarnings("unchecked")
    private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          // TODO: Support Pig and Hive object models for IcebergInputFormat
          throw new UnsupportedOperationException("Pig and Hive object models are not supported.");
        case GENERIC:
          DeleteFilter deletes = new GenericDeleteFilter(io, currentTask, tableSchema, readSchema);
          Schema requiredSchema = deletes.requiredSchema();
          return deletes.filter(openTask(currentTask, requiredSchema));
        default:
          throw new UnsupportedOperationException("Unsupported memory model");
      }
    }

    private CloseableIterable<T> applyResidualFiltering(CloseableIterable<T> iter, Expression residual,
                                                        Schema readSchema) {
      boolean applyResidual = !context.getConfiguration().getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);

      if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
        // Date and timestamp values are not the correct type for Evaluator.
        // Wrapping to return the expected type.
        InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
        Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
        return CloseableIterable.filter(iter, record -> filter.eval(wrapper.wrap((StructLike) record)));
      } else {
        return iter;
      }
    }

    private CloseableIterable<T> newAvroIterable(
        InputFile inputFile, FileScanTask task, Schema readSchema) {
      Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
          .project(readSchema)
          .split(task.start(), task.length());
      if (reuseContainers) {
        avroReadBuilder.reuseContainers();
      }

      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          // TODO implement value readers for Pig and Hive
          throw new UnsupportedOperationException("Avro support not yet supported for Pig and Hive");
        case GENERIC:
          avroReadBuilder.createReaderFunc(
              (expIcebergSchema, expAvroSchema) ->
                  DataReader.create(expIcebergSchema, expAvroSchema,
                      constantsMap(task, IdentityPartitionConverters::convertConstant)));
      }
      return applyResidualFiltering(avroReadBuilder.build(), task.residual(), readSchema);
    }

    private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
          .project(readSchema)
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .split(task.start(), task.length());
      if (reuseContainers) {
        parquetReadBuilder.reuseContainers();
      }

      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          // TODO implement value readers for Pig and Hive
          throw new UnsupportedOperationException("Parquet support not yet supported for Pig and Hive");
        case GENERIC:
          parquetReadBuilder.createReaderFunc(
              fileSchema -> GenericParquetReaders.buildReader(
                  readSchema, fileSchema, constantsMap(task, IdentityPartitionConverters::convertConstant)));
      }
      return applyResidualFiltering(parquetReadBuilder.build(), task.residual(), readSchema);
    }

    private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Map<Integer, ?> idToConstant = constantsMap(task, IdentityPartitionConverters::convertConstant);
      Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(readSchema,
          Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));
      ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
          .project(readSchemaWithoutConstantAndMetadataFields)
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .split(task.start(), task.length());
      // ORC does not support reuse containers yet
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          // TODO: implement value readers for Pig and Hive
          throw new UnsupportedOperationException("ORC support not yet supported for Pig and Hive");
        case GENERIC:
          orcReadBuilder.createReaderFunc(
              fileSchema -> GenericOrcReader.buildReader(
                  readSchema, fileSchema, idToConstant));
      }

      return applyResidualFiltering(orcReadBuilder.build(), task.residual(), readSchema);
    }

    private Map<Integer, ?> constantsMap(FileScanTask task, BiFunction<Type, Object, Object> converter) {
      PartitionSpec spec = task.spec();
      Set<Integer> idColumns = spec.identitySourceIds();
      Schema partitionSchema = TypeUtil.select(expectedSchema, idColumns);
      boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();
      if (projectsIdentityPartitionColumns) {
        return PartitionUtil.constantsMap(task, converter);
      } else {
        return Collections.emptyMap();
      }
    }

    private static Schema readSchema(Configuration conf, Schema tableSchema, boolean caseSensitive) {
      Schema readSchema = InputFormatConfig.readSchema(conf);

      if (readSchema != null) {
        return readSchema;
      }

      String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
      if (selectedColumns == null) {
        return tableSchema;
      }

      return caseSensitive ? tableSchema.select(selectedColumns) : tableSchema.caseInsensitiveSelect(selectedColumns);
    }
  }

}
