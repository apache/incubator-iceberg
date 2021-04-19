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

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

public class RowDataRewriter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataRewriter.class);

  private final Schema schema;
  private final PartitionSpec spec;
  private final Map<String, String> properties;
  private final FileFormat format;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final LocationProvider locations;
  private final String nameMapping;
  private final boolean caseSensitive;

  public RowDataRewriter(Table table, PartitionSpec spec, boolean caseSensitive,
                         Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager) {
    this.schema = table.schema();
    this.spec = spec;
    this.locations = table.locationProvider();
    this.properties = table.properties();
    this.io = io;
    this.encryptionManager = encryptionManager;

    this.caseSensitive = caseSensitive;
    this.nameMapping = table.properties().get(DEFAULT_NAME_MAPPING);

    String formatString = table.properties().getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  public RewriteResult rewriteDataForTasks(JavaRDD<CombinedScanTask> taskRDD) {
    List<RewriteResult> rewriteResults = taskRDD.map(this::rewriteDataForTask)
        .collect();

    return RewriteResult.builder()
        .merge(rewriteResults)
        .build();
  }

  private RewriteResult rewriteDataForTask(CombinedScanTask task) throws Exception {
    TaskContext context = TaskContext.get();
    int partitionId = context.partitionId();
    long taskId = context.taskAttemptId();

    RowDataReader dataReader = new RowDataReader(
        task, schema, schema, nameMapping, io.value(), encryptionManager.value(), caseSensitive);

    StructType structType = SparkSchemaUtil.convert(schema);
    SparkAppenderFactory appenderFactory = new SparkAppenderFactory(properties, schema, structType, spec);
    OutputFileFactory fileFactory = new OutputFileFactory(
        spec, format, locations, io.value(), encryptionManager.value(), partitionId, taskId);

    TaskWriter<InternalRow> writer;
    if (spec.isUnpartitioned()) {
      writer = new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, io.value(),
          Long.MAX_VALUE);
    } else if (PropertyUtil.propertyAsBoolean(properties,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT)) {
      writer = new SparkPartitionedFanoutWriter(
          spec, format, appenderFactory, fileFactory, io.value(), Long.MAX_VALUE, schema,
          structType);
    } else {
      writer = new SparkPartitionedWriter(
          spec, format, appenderFactory, fileFactory, io.value(), Long.MAX_VALUE, schema,
          structType);
    }

    RewriteResult.Builder resultBuilder = RewriteResult.builder();
    for (FileScanTask scanTask : task.files()) {
      resultBuilder.addDataFilesToDelete(scanTask.file());
      resultBuilder.addDeleteFilesToDelete(scanTask.deletes());
    }

    try {
      while (dataReader.next()) {
        InternalRow row = dataReader.get();
        writer.write(row);
      }

      dataReader.close();
      dataReader = null;

      writer.close();

      // Add the data files only because deletions from delete files has been eliminated.
      return resultBuilder
          .addDataFilesToAdd(writer.dataFiles())
          .build();

    } catch (Throwable originalThrowable) {
      try {
        LOG.error("Aborting task", originalThrowable);
        context.markTaskFailed(originalThrowable);

        LOG.error("Aborting commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.attemptNumber(), context.stageId(), context.stageAttemptNumber());
        if (dataReader != null) {
          dataReader.close();
        }
        writer.abort();
        LOG.error("Aborted commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.taskAttemptId(), context.stageId(), context.stageAttemptNumber());

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
