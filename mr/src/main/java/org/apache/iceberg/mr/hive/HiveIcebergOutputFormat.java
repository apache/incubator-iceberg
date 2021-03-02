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

import java.util.Locale;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
<<<<<<< HEAD
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
=======
import org.apache.iceberg.SortOrder;
>>>>>>> Address review comments
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.util.PropertyUtil;

public class HiveIcebergOutputFormat<T> implements OutputFormat<NullWritable, Container<Record>>,
    HiveOutputFormat<NullWritable, Container<Record>> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass,
      boolean isCompressed, Properties tableAndSerDeProperties, Progressable progress) {
    return writer(jc);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) {
    return writer(job);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  private static HiveIcebergRecordWriter writer(JobConf jc) {
    TaskAttemptID taskAttemptID = TezUtil.taskAttemptWrapper(jc);
    // It gets the config from the FileSinkOperator which has its own config for every target table
    Table table = HiveIcebergStorageHandler.table(jc, jc.get(hive_metastoreConstants.META_TABLE_NAME));
    Schema schema = HiveIcebergStorageHandler.schema(jc);
    PartitionSpec spec = table.spec();
    FileFormat fileFormat = FileFormat.valueOf(PropertyUtil.propertyAsString(table.properties(),
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH));
    long targetFileSize = PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    FileIO io = table.io();
    LocationProvider location = table.locationProvider();
    EncryptionManager encryption = table.encryption();
    OutputFileFactory outputFileFactory =
        new OutputFileFactory(spec, fileFormat, location, io, encryption, taskAttemptID.getTaskID().getId(),
            taskAttemptID.getId(), jc.get(HiveConf.ConfVars.HIVEQUERYID.varname) + "-" + taskAttemptID.getJobID());
    String tableName = jc.get(Catalogs.NAME);

    return new HiveIcebergRecordWriter(schema, spec, fileFormat,
        new GenericAppenderFactory(schema, spec), outputFileFactory, io, targetFileSize, taskAttemptID, tableName);
  }
}
