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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.iceberg.mr.hive.HiveIcebergRecordWriter.getWriters;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestHiveIcebergOutputCommitter {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private static final int RECORD_NUM = 5;
  private static final String QUERY_ID = "query_id";
  private static final JobID JOB_ID = new JobID("test", 0);
  private static final TaskAttemptID MAP_TASK_ID =
      new TaskAttemptID(JOB_ID.getJtIdentifier(), JOB_ID.getId(), TaskType.MAP, 0, 0);
  private static final TaskAttemptID REDUCE_TASK_ID =
      new TaskAttemptID(JOB_ID.getJtIdentifier(), JOB_ID.getId(), TaskType.REDUCE, 0, 0);

  private static final Schema CUSTOMER_SCHEMA = new Schema(
      required(1, "customer_id", Types.LongType.get()),
      required(2, "first_name", Types.StringType.get())
  );

  private static final PartitionSpec PARTITIONED_SPEC =
      PartitionSpec.builderFor(CUSTOMER_SCHEMA).bucket("customer_id", 3).build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testNeedsTaskCommit() {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();

    JobConf mapOnlyJobConf = new JobConf();
    mapOnlyJobConf.setNumMapTasks(10);
    mapOnlyJobConf.setNumReduceTasks(0);

    // Map only job should commit map tasks
    Assert.assertTrue(committer.needsTaskCommit(new TaskAttemptContextImpl(mapOnlyJobConf, MAP_TASK_ID)));

    JobConf mapReduceJobConf = new JobConf();
    mapReduceJobConf.setNumMapTasks(10);
    mapReduceJobConf.setNumReduceTasks(10);

    // MapReduce job should not commit map tasks, but should commit reduce tasks
    Assert.assertFalse(committer.needsTaskCommit(new TaskAttemptContextImpl(mapReduceJobConf, MAP_TASK_ID)));
    Assert.assertTrue(committer.needsTaskCommit(new TaskAttemptContextImpl(mapReduceJobConf, REDUCE_TASK_ID)));
  }

  @Test
  public void testSuccessfulUnpartitionedWrite() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), false);

    JobConf conf = jobConf(table, 1);
    List<Record> expected = writeRecords(table.name(), 1, 0, true, false, conf);
    committer.commitJob(new JobContextImpl(conf, JOB_ID));

    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 1);
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testSuccessfulPartitionedWrite() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), true);
    JobConf conf = jobConf(table, 1);
    List<Record> expected = writeRecords(table.name(), 1, 0, true, false, conf);
    committer.commitJob(new JobContextImpl(conf, JOB_ID));

    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 3);
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testSuccessfulMultipleTasksUnpartitionedWrite() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), false);
    JobConf conf = jobConf(table, 2);
    List<Record> expected = writeRecords(table.name(), 2, 0, true, false, conf);
    committer.commitJob(new JobContextImpl(conf, JOB_ID));

    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 2);
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testSuccessfulMultipleTasksPartitionedWrite() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), true);
    JobConf conf = jobConf(table, 2);
    List<Record> expected = writeRecords(table.name(), 2, 0, true, false, conf);
    committer.commitJob(new JobContextImpl(conf, JOB_ID));

    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 6);
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testRetryTask() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), false);
    JobConf conf = jobConf(table, 2);

    // Write records and abort the tasks
    writeRecords(table.name(), 2, 0, false, true, conf);
    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 0);
    HiveIcebergTestUtils.validateData(table, Collections.emptyList(), 0);

    // Write records but do not abort the tasks
    // The data files remain since we can not identify them but should not be read
    writeRecords(table.name(), 2, 1, false, false, conf);
    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 2);
    HiveIcebergTestUtils.validateData(table, Collections.emptyList(), 0);

    // Write and commit the records
    List<Record> expected = writeRecords(table.name(), 2, 2, true, false, conf);
    committer.commitJob(new JobContextImpl(conf, JOB_ID));
    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 4);
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @Test
  public void testAbortJob() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    Table table = table(temp.getRoot().getPath(), false);
    JobConf conf = jobConf(table, 1);
    writeRecords(table.name(), 1, 0, true, false, conf);
    committer.abortJob(new JobContextImpl(conf, JOB_ID), JobStatus.State.FAILED);

    HiveIcebergTestUtils.validateFiles(table, conf, JOB_ID, 0);
    HiveIcebergTestUtils.validateData(table, Collections.emptyList(), 0);
  }

  @Test
  public void writerIsClosedAfterTaskCommitFailure() throws IOException {
    HiveIcebergOutputCommitter committer = new HiveIcebergOutputCommitter();
    HiveIcebergOutputCommitter failingCommitter = Mockito.spy(committer);
    ArgumentCaptor<TaskAttemptContextImpl> argumentCaptor = ArgumentCaptor.forClass(TaskAttemptContextImpl.class);
    String exceptionMessage = "Commit task failed!";
    Mockito.doThrow(new RuntimeException(exceptionMessage))
            .when(failingCommitter).commitTask(argumentCaptor.capture());

    Table table = table(temp.getRoot().getPath(), false);
    JobConf conf = jobConf(table, 1);
    try {
      writeRecords(table.name(), 1, 0, true, false, conf, failingCommitter);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains(exceptionMessage));
    }

    Assert.assertEquals(1, argumentCaptor.getAllValues().size());
    TaskAttemptID capturedId = TezUtil.taskAttemptWrapper(argumentCaptor.getValue().getTaskAttemptID());
    // writer is still in the map after commitTask failure
    Assert.assertNotNull(getWriters(capturedId));
    failingCommitter.abortTask(new TaskAttemptContextImpl(conf, capturedId));
    // abortTask succeeds and removes writer
    Assert.assertNull(getWriters(capturedId));
  }

  private Table table(String location, boolean partitioned) {
    HadoopTables tables = new HadoopTables();

    return tables.create(CUSTOMER_SCHEMA, partitioned ? PARTITIONED_SPEC : PartitionSpec.unpartitioned(),
            ImmutableMap.of(InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_HADOOP_TABLE_NAME), location);
  }

  private JobConf jobConf(Table table, int taskNum) {
    JobConf conf = new JobConf();
    conf.setNumMapTasks(taskNum);
    conf.setNumReduceTasks(0);
    conf.set(HiveConf.ConfVars.HIVEQUERYID.varname, QUERY_ID);
    conf.set(InputFormatConfig.OUTPUT_TABLES, table.name());
    conf.set(InputFormatConfig.TABLE_CATALOG_PREFIX + table.name(),
            table.properties().get(InputFormatConfig.CATALOG_NAME));
    conf.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + table.name(), SerializationUtil.serializeToBase64(table));

    Map<String, String> propMap = Maps.newHashMap();
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(new Properties());
    tableDesc.getProperties().setProperty(Catalogs.NAME, table.name());
    tableDesc.getProperties().setProperty(Catalogs.LOCATION, table.location());
    tableDesc.getProperties().setProperty(InputFormatConfig.CATALOG_NAME, table.properties()
            .get(InputFormatConfig.CATALOG_NAME));
    HiveIcebergStorageHandler.overlayTableProperties(conf, tableDesc, propMap);
    propMap.forEach((key, value) -> conf.set(key, value));
    return conf;
  }

  /**
   * Write random records to the given table using separate {@link HiveIcebergOutputCommitter} and
   * a separate {@link HiveIcebergRecordWriter} for every task.
   * @param name The name of the table to get the table object from the conf
   * @param taskNum The number of tasks in the job handled by the committer
   * @param attemptNum The id used for attempt number generation
   * @param commitTasks If <code>true</code> the tasks will be committed
   * @param abortTasks If <code>true</code> the tasks will be aborted - needed so we can simulate no commit/no abort
   *                   situation
   * @param conf The job configuration
   * @param committer The output committer that should be used for committing/aborting the tasks
   * @return The random generated records which were appended to the table
   * @throws IOException Propagating {@link HiveIcebergRecordWriter} exceptions
   */
  private List<Record> writeRecords(String name, int taskNum, int attemptNum, boolean commitTasks, boolean abortTasks,
                                    JobConf conf, OutputCommitter committer) throws IOException {
    List<Record> expected = new ArrayList<>(RECORD_NUM * taskNum);

    Table table = IcebergSerializationUtil.table(conf, name);
    FileIO io = table.io();
    LocationProvider location = table.locationProvider();
    EncryptionManager encryption = table.encryption();
    Schema schema = IcebergSerializationUtil.schema(conf);
    PartitionSpec spec = table.spec();

    for (int i = 0; i < taskNum; ++i) {
      List<Record> records = TestHelper.generateRandomRecords(schema, RECORD_NUM, i + attemptNum);
      TaskAttemptID taskId = new TaskAttemptID(JOB_ID.getJtIdentifier(), JOB_ID.getId(), TaskType.MAP, i, attemptNum);
      OutputFileFactory outputFileFactory =
          new OutputFileFactory(spec, FileFormat.PARQUET, location, io, encryption, taskId.getTaskID().getId(),
              attemptNum, QUERY_ID + "-" + JOB_ID);
      HiveIcebergRecordWriter testWriter = new HiveIcebergRecordWriter(schema, spec, FileFormat.PARQUET,
          new GenericAppenderFactory(schema), outputFileFactory, io, TARGET_FILE_SIZE,
          TezUtil.taskAttemptWrapper(taskId), conf.get(Catalogs.NAME));

      Container<Record> container = new Container<>();

      for (Record record : records) {
        container.set(record);
        testWriter.write(container);
      }

      testWriter.close(false);
      if (commitTasks) {
        committer.commitTask(new TaskAttemptContextImpl(conf, taskId));
        expected.addAll(records);
      } else if (abortTasks) {
        committer.abortTask(new TaskAttemptContextImpl(conf, taskId));
      }
    }

    return expected;
  }

  private List<Record> writeRecords(String name, int taskNum, int attemptNum, boolean commitTasks, boolean abortTasks,
                                    JobConf conf) throws IOException {
    return writeRecords(name, taskNum, attemptNum, commitTasks, abortTasks, conf, new HiveIcebergOutputCommitter());
  }
}
