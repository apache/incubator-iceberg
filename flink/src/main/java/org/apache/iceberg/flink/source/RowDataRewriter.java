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

import java.util.Locale;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

public class RowDataRewriter {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataRewriter.class);

  private final Schema schema;
  private final String nameMapping;
  private final FileIO io;
  private final boolean caseSensitive;
  private final EncryptionManager encryptionManager;
  private final TaskWriterFactory<RowData> taskWriterFactory;
  private final String tableName;

  public RowDataRewriter(Table table, boolean caseSensitive, FileIO io, EncryptionManager encryptionManager) {
    this.schema = table.schema();
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.nameMapping = PropertyUtil.propertyAsString(table.properties(), DEFAULT_NAME_MAPPING, null);
    this.tableName = table.name();

    String formatString = PropertyUtil.propertyAsString(table.properties(), TableProperties.DEFAULT_FILE_FORMAT,
        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    this.taskWriterFactory = new RowDataTaskWriterFactory(
        table.schema(),
        flinkSchema,
        table.spec(),
        table.locationProvider(),
        io,
        encryptionManager,
        Long.MAX_VALUE,
        format,
        table.properties(),
        // TODO Need to fill the latest equality field ids if plan to produce delete files after rewrite.
        null);
  }

  public RewriteResult rewriteDataForTasks(DataStream<CombinedScanTask> dataStream, int parallelism) throws Exception {
    RewriteMap map = new RewriteMap(schema, nameMapping, io, caseSensitive, encryptionManager, taskWriterFactory);

    RewriteResult.Builder builder = RewriteResult.builder();

    try (CloseableIterator<RewriteResult> results = dataStream.map(map)
        .setParallelism(parallelism)
        .executeAndCollect("Rewrite table :" + tableName)) {

      builder.merge(() -> results);
    }

    return builder.build();
  }

  public static class RewriteMap extends RichMapFunction<CombinedScanTask, RewriteResult> {

    private int subTaskId;
    private int attemptId;

    private final Schema schema;
    private final String nameMapping;
    private final FileIO io;
    private final boolean caseSensitive;
    private final EncryptionManager encryptionManager;
    private final TaskWriterFactory<RowData> taskWriterFactory;

    public RewriteMap(Schema schema, String nameMapping, FileIO io, boolean caseSensitive,
                      EncryptionManager encryptionManager, TaskWriterFactory<RowData> taskWriterFactory) {
      this.schema = schema;
      this.nameMapping = nameMapping;
      this.io = io;
      this.caseSensitive = caseSensitive;
      this.encryptionManager = encryptionManager;
      this.taskWriterFactory = taskWriterFactory;
    }

    @Override
    public void open(Configuration parameters) {
      this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
      this.attemptId = getRuntimeContext().getAttemptNumber();
      // Initialize the task writer factory.
      this.taskWriterFactory.initialize(subTaskId, attemptId);
    }

    @Override
    public RewriteResult map(CombinedScanTask task) throws Exception {
      // Initialize the builder of ResultWriter.
      RewriteResult.Builder resultBuilder = RewriteResult.builder();
      for (FileScanTask scanTask : task.files()) {
        resultBuilder.addDataFilesToDelete(scanTask.file());
        resultBuilder.addDeleteFilesToDelete(scanTask.deletes());
      }

      // Initialize the task writer.
      TaskWriter<RowData> writer = taskWriterFactory.create();
      try (RowDataIterator iterator = new RowDataIterator(task, io, encryptionManager, schema,
          schema, nameMapping, caseSensitive)) {

        while (iterator.hasNext()) {
          RowData rowData = iterator.next();
          writer.write(rowData);
        }

        // Add the data files only because deletions from delete files has been eliminated.
        resultBuilder.addDataFilesToAdd(writer.dataFiles());

        return resultBuilder.build();
      } catch (Throwable originalThrowable) {
        try {
          LOG.error("Aborting commit for  (subTaskId {}, attemptId {})", subTaskId, attemptId);
          writer.abort();
          LOG.error("Aborted commit for  (subTaskId {}, attemptId {})", subTaskId, attemptId);
        } catch (Throwable inner) {
          if (originalThrowable != inner) {
            originalThrowable.addSuppressed(inner);
            LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
          }
        }

        if (originalThrowable instanceof Exception) {
          throw originalThrowable;
        } else {
          throw new RuntimeException(originalThrowable);
        }
      }
    }
  }
}
