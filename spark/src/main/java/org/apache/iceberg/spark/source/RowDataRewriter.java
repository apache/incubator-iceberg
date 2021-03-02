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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDataRewriter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataRewriter.class);

  private final Broadcast<Table> tableBroadcast;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final boolean caseSensitive;

  public RowDataRewriter(Broadcast<Table> tableBroadcast, PartitionSpec spec, boolean caseSensitive) {
    this.tableBroadcast = tableBroadcast;
    this.spec = spec;
    this.caseSensitive = caseSensitive;

    Table table = tableBroadcast.value();
    String formatString = table.properties().getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  public List<DataFile> rewriteDataForTasks(JavaRDD<CombinedScanTask> taskRDD) {
    JavaRDD<List<DataFile>> dataFilesRDD = taskRDD.map(this::rewriteDataForTask);

    return dataFilesRDD.collect().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private List<DataFile> rewriteDataForTask(CombinedScanTask task) throws Exception {
    TaskContext context = TaskContext.get();
    int partitionId = context.partitionId();
    long taskId = context.taskAttemptId();

    Table table = tableBroadcast.value();
    Schema schema = table.schema();
    Map<String, String> properties = table.properties();

    RowDataReader dataReader = new RowDataReader(task, table, schema, caseSensitive);

    StructType structType = SparkSchemaUtil.convert(schema);
    SparkAppenderFactory appenderFactory = SparkAppenderFactory
        .builderFor(properties, schema, structType)
        .partitionSpec(spec).build();
    OutputFileFactory fileFactory = new OutputFileFactory(
        spec, format, locations, io.value(), encryptionManager.value(), partitionId, taskId);

    TaskWriter<InternalRow> writer;
    if (spec.isUnpartitioned()) {
      writer = new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, table.io(),
          Long.MAX_VALUE);
    } else if (PropertyUtil.propertyAsBoolean(properties,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT)) {
      writer = new SparkPartitionedFanoutWriter(
          spec, format, appenderFactory, fileFactory, table.io(), Long.MAX_VALUE, schema,
          structType);
    } else {
      writer = new SparkPartitionedWriter(
          spec, format, appenderFactory, fileFactory, table.io(), Long.MAX_VALUE, schema,
          structType);
    }

    try {
      while (dataReader.next()) {
        InternalRow row = dataReader.get();
        writer.write(row);
      }

      dataReader.close();
      dataReader = null;

      writer.close();
      return Lists.newArrayList(writer.dataFiles());

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
