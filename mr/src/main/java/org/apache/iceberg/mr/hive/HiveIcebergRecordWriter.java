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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveIcebergRecordWriter extends PartitionedFanoutWriter<Record>
    implements FileSinkOperator.RecordWriter, org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergRecordWriter.class);

  // The current key is reused at every write to avoid unnecessary object creation
  private final PartitionKey currentKey;
  private final FileIO io;

  // <TaskAttemptId, <TABLE_NAME, HiveIcebergRecordWriter>> map to store the active writers
  // Stored in concurrent map, since some executor engines can share containers
  private static final Map<TaskAttemptID, Map<String, HiveIcebergRecordWriter>> writers = Maps.newConcurrentMap();

  static Map<String, HiveIcebergRecordWriter> removeWriters(TaskAttemptID taskAttemptID) {
    return writers.remove(taskAttemptID);
  }

  static Map<String, HiveIcebergRecordWriter> getWriters(TaskAttemptID taskAttemptID) {
    return writers.get(taskAttemptID);
  }

  HiveIcebergRecordWriter(Schema schema, PartitionSpec spec, FileFormat format,
      FileAppenderFactory<Record> appenderFactory, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
      Map<String, String> properties, TaskAttemptID taskAttemptID, String tableName) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, properties);
    this.io = io;
    this.currentKey = new PartitionKey(spec, schema);
    writers.putIfAbsent(taskAttemptID, Maps.newConcurrentMap());
    writers.get(taskAttemptID).put(tableName, this);
  }

  @Override
  protected PartitionKey partition(Record row) {
    currentKey.partition(row);
    return currentKey;
  }

  @Override
  public void write(Writable row) throws IOException {
    super.write(((Container<Record>) row).get());
  }

  @Override
  public void write(NullWritable key, Container value) throws IOException {
    write(value);
  }

  @Override
  public void close(boolean abort) throws IOException {
    DataFile[] dataFiles = super.dataFiles();

    // If abort then remove the unnecessary files
    if (abort) {
      Tasks.foreach(dataFiles)
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove file {} on abort", file, exception))
          .run(dataFile -> io.deleteFile(dataFile.path().toString()));
    }

    LOG.info("IcebergRecordWriter is closed with abort={}. Created {} files", abort, dataFiles.length);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
