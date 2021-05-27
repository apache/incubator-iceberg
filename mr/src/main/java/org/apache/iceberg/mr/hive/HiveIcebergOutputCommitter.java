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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Iceberg table committer for adding data files to the Iceberg tables.
 * Currently independent of the Hive ACID transactions.
 */
public class HiveIcebergOutputCommitter extends OutputCommitter {
  private static final String FOR_COMMIT_EXTENSION = ".forCommit";

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) {
    // do nothing.
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) {
    // do nothing.
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) {
    // We need to commit if this is the last phase of a MapReduce process
    return TaskType.REDUCE.equals(context.getTaskAttemptID().getTaskID().getTaskType()) ||
        context.getJobConf().getNumReduceTasks() == 0;
  }

  /**
   * Collects the generated data files and creates a commit file storing the data file list.
   * @param originalContext The task attempt context
   * @throws IOException Thrown if there is an error writing the commit file
   */
  @Override
  public void commitTask(TaskAttemptContext originalContext) throws IOException {
    TaskAttemptContext context = TezUtil.enrichContextWithAttemptWrapper(originalContext);

    TaskAttemptID attemptID = context.getTaskAttemptID();
    JobConf jobConf = context.getJobConf();
    Collection<String> outputs = IcebergSerializationUtil.outputTables(context.getJobConf());
    Map<String, HiveIcebergRecordWriter> writers = Optional.ofNullable(HiveIcebergRecordWriter.getWriters(attemptID))
        .orElseGet(() -> {
          LOG.info("CommitTask found no writers for output tables: {}, attemptID: {}", outputs, attemptID);
          return ImmutableMap.of();
        });

    ExecutorService tableExecutor = tableExecutor(jobConf, outputs.size());
    try {
      // Generates commit files for the target tables in parallel
      Tasks.foreach(outputs)
          .retry(3)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(tableExecutor)
          .run(output -> {
            Table table = IcebergSerializationUtil.table(context.getJobConf(), output);
            if (table != null) {
              HiveIcebergRecordWriter writer = writers.get(output);
              DataFile[] closedFiles;
              if (writer != null) {
                closedFiles = writer.dataFiles();
              } else {
                LOG.info("CommitTask found no writer for specific table: {}, attemptID: {}", output, attemptID);
                closedFiles = new DataFile[0];
              }
                // Creating the file containing the data files generated by this task for this table
              String fileForCommitLocation = generateFileForCommitLocation(table.location(), jobConf,
                      attemptID.getJobID(), attemptID.getTaskID().getId());
              createFileForCommit(closedFiles, fileForCommitLocation, table.io());
            } else {
              // When using Tez multi-table inserts, we could have more output tables in config than
              // the actual tables this task has written to and has serialized in its config
              LOG.info("CommitTask found no serialized table in config for table: {}.", output);
            }
          }, IOException.class);
    } finally {
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    // remove the writer to release the object
    HiveIcebergRecordWriter.removeWriters(attemptID);
  }

  /**
   * Removes files generated by this task.
   * @param originalContext The task attempt context
   * @throws IOException Thrown if there is an error closing the writer
   */
  @Override
  public void abortTask(TaskAttemptContext originalContext) throws IOException {
    TaskAttemptContext context = TezUtil.enrichContextWithAttemptWrapper(originalContext);

    // Clean up writer data from the local store
    Map<String, HiveIcebergRecordWriter> writers = HiveIcebergRecordWriter.removeWriters(context.getTaskAttemptID());

    // Remove files if it was not done already
    if (writers != null) {
      for (HiveIcebergRecordWriter writer : writers.values()) {
        writer.close(true);
      }
    }
  }

  /**
   * Reads the commit files stored in the temp directories and collects the generated committed data files.
   * Appends the data files to the tables. At the end removes the temporary directories.
   * @param originalContext The job context
   * @throws IOException if there is a failure accessing the files
   */
  @Override
  public void commitJob(JobContext originalContext) throws IOException {
    JobContext jobContext = TezUtil.enrichContextWithVertexId(originalContext);
    JobConf jobConf = jobContext.getJobConf();

    long startTime = System.currentTimeMillis();
    LOG.info("Committing job {} has started", jobContext.getJobID());

    Collection<String> outputs = IcebergSerializationUtil.outputTables(jobContext.getJobConf());
    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();

    ExecutorService fileExecutor = fileExecutor(jobConf);
    ExecutorService tableExecutor = tableExecutor(jobConf, outputs.size());
    try {
      // Commits the changes for the output tables in parallel
      Tasks.foreach(outputs)
          .throwFailureWhenFinished()
          .stopOnFailure()
          .executeWith(tableExecutor)
          .run(output -> {
            Table table = IcebergSerializationUtil.table(jobConf, output);
            if (table != null) {
              String catalogName = IcebergSerializationUtil.catalogName(jobConf, output);
              jobLocations.add(generateJobLocation(table.location(), jobConf, jobContext.getJobID()));
              commitTable(table.io(), fileExecutor, jobContext, output, table.location(), catalogName);
            } else {
              LOG.info("CommitJob found no serialized table in config for table: {}. Skipping job commit.", output);
            }
          });
    } finally {
      fileExecutor.shutdown();
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    LOG.info("Commit took {} ms for job {}", System.currentTimeMillis() - startTime, jobContext.getJobID());

    cleanup(jobContext, jobLocations);
  }

  /**
   * Removes the generated data files if there is a commit file already generated for them.
   * The cleanup at the end removes the temporary directories as well.
   * @param originalContext The job context
   * @param status The status of the job
   * @throws IOException if there is a failure deleting the files
   */
  @Override
  public void abortJob(JobContext originalContext, int status) throws IOException {
    JobContext jobContext = TezUtil.enrichContextWithVertexId(originalContext);
    JobConf jobConf = jobContext.getJobConf();

    LOG.info("Job {} is aborted. Data file cleaning started", jobContext.getJobID());
    Collection<String> outputs = IcebergSerializationUtil.outputTables(jobContext.getJobConf());
    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();

    ExecutorService fileExecutor = fileExecutor(jobConf);
    ExecutorService tableExecutor = tableExecutor(jobConf, outputs.size());
    try {
      // Cleans up the changes for the output tables in parallel
      Tasks.foreach(outputs)
          .suppressFailureWhenFinished()
          .executeWith(tableExecutor)
          .onFailure((output, exc) -> LOG.warn("Failed cleanup table {} on abort job", output, exc))
          .run(output -> {
            LOG.info("Cleaning job for table {}", jobContext.getJobID(), output);

            Table table = IcebergSerializationUtil.table(jobConf, output);
            jobLocations.add(generateJobLocation(table.location(), jobConf, jobContext.getJobID()));
            Collection<DataFile> dataFiles = dataFiles(fileExecutor, table.location(), jobContext, table.io(), false);

            // Check if we have files already committed and remove data files if there are any
            if (dataFiles.size() > 0) {
              Tasks.foreach(dataFiles)
                  .retry(3)
                  .suppressFailureWhenFinished()
                  .executeWith(fileExecutor)
                  .onFailure((file, exc) -> LOG.warn("Failed to remove data file {} on abort job", file.path(), exc))
                  .run(file -> table.io().deleteFile(file.path().toString()));
            }
          });
    } finally {
      fileExecutor.shutdown();
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    LOG.info("Job {} is aborted. Data file cleaning finished", jobContext.getJobID());

    cleanup(jobContext, jobLocations);
  }

  /**
   * Collects the additions to a single table and adds/commits the new files to the Iceberg table.
   * @param io The io to read the forCommit files
   * @param executor The executor used to read the forCommit files
   * @param jobContext The job context
   * @param name The name of the table used for loading from the catalog
   * @param location The location of the table used for loading from the catalog
   * @param catalogName The name of the catalog that contains the table
   */
  private void commitTable(FileIO io, ExecutorService executor, JobContext jobContext, String name, String location,
                           String catalogName) {
    JobConf conf = jobContext.getJobConf();
    Properties catalogProperties = new Properties();
    catalogProperties.put(Catalogs.NAME, name);
    catalogProperties.put(Catalogs.LOCATION, location);
    if (catalogName != null) {
      catalogProperties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    }
    Table table = Catalogs.loadTable(conf, catalogProperties);

    long startTime = System.currentTimeMillis();
    LOG.info("Committing job has started for table: {}, using location: {}",
        table, generateJobLocation(location, conf, jobContext.getJobID()));

    Collection<DataFile> dataFiles = dataFiles(executor, location, jobContext, io, true);

    if (dataFiles.size() > 0) {
      // Appending data files to the table
      AppendFiles append = table.newAppend();
      dataFiles.forEach(append::appendFile);
      append.commit();
      LOG.info("Commit took {} ms for table: {} with {} file(s)", System.currentTimeMillis() - startTime, table,
          dataFiles.size());
      LOG.debug("Added files {}", dataFiles);
    } else {
      LOG.info("Commit took {} ms for table: {} with no new files", System.currentTimeMillis() - startTime, table);
    }
  }

  /**
   * Cleans up the jobs temporary locations. For every target table there is a temp dir to clean up.
   * @param jobContext The job context
   * @param jobLocations The locations to clean up
   * @throws IOException if there is a failure deleting the files
   */
  private void cleanup(JobContext jobContext, Collection<String> jobLocations) throws IOException {
    JobConf jobConf = jobContext.getJobConf();

    LOG.info("Cleaning for job {} started", jobContext.getJobID());

    // Remove the job's temp directories recursively.
    Tasks.foreach(jobLocations)
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure((jobLocation, exc) -> LOG.debug("Failed to remove directory {} on job cleanup", jobLocation, exc))
        .run(jobLocation -> {
          LOG.info("Cleaning location: {}", jobLocation);
          Path toDelete = new Path(jobLocation);
          FileSystem fs = Util.getFs(toDelete, jobConf);
          fs.delete(toDelete, true);
        }, IOException.class);

    LOG.info("Cleaning for job {} finished", jobContext.getJobID());
  }

  /**
   * Executor service for parallel handling of file reads. Should be shared when committing multiple tables.
   * @param conf The configuration containing the pool size
   * @return The generated executor service
   */
  private static ExecutorService fileExecutor(Configuration conf) {
    int size = conf.getInt(InputFormatConfig.COMMIT_FILE_THREAD_POOL_SIZE,
        InputFormatConfig.COMMIT_FILE_THREAD_POOL_SIZE_DEFAULT);
    return Executors.newFixedThreadPool(
        size,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setPriority(Thread.NORM_PRIORITY)
            .setNameFormat("iceberg-commit-file-pool-%d")
            .build());
  }

  /**
   * Executor service for parallel handling of table manipulation. Could return null, if no parallelism is possible.
   * @param conf The configuration containing the pool size
   * @param maxThreadNum The number of requests we want to handle (might be decreased further by configuration)
   * @return The generated executor service, or null if executor is not needed.
   */
  private static ExecutorService tableExecutor(Configuration conf, int maxThreadNum) {
    int size = conf.getInt(InputFormatConfig.COMMIT_TABLE_THREAD_POOL_SIZE,
        InputFormatConfig.COMMIT_TABLE_THREAD_POOL_SIZE_DEFAULT);
    size = Math.min(maxThreadNum, size);
    if (size > 1) {
      return Executors.newFixedThreadPool(
          size,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setPriority(Thread.NORM_PRIORITY)
              .setNameFormat("iceberg-commit-table-pool-%d")
              .build());
    } else {
      return null;
    }
  }

  /**
   * Get the committed data files for this table and job.
   * @param executor The executor used for reading the forCommit files parallel
   * @param location The location of the table
   * @param jobContext The job context
   * @param io The FileIO used for reading a files generated for commit
   * @param throwOnFailure If <code>true</code> then it throws an exception on failure
   * @return The list of the committed data files
   */
  private static Collection<DataFile> dataFiles(ExecutorService executor, String location, JobContext jobContext,
      FileIO io, boolean throwOnFailure) {
    JobConf conf = jobContext.getJobConf();
    // If there are reducers, then every reducer will generate a result file.
    // If this is a map only task, then every mapper will generate a result file.
    int expectedFiles = conf.getNumReduceTasks() > 0 ? conf.getNumReduceTasks() : conf.getNumMapTasks();

    Collection<DataFile> dataFiles = new ConcurrentLinkedQueue<>();

    // Reading the committed files. The assumption here is that the taskIds are generated in sequential order
    // starting from 0.
    Tasks.range(expectedFiles)
        .throwFailureWhenFinished(throwOnFailure)
        .executeWith(executor)
        .retry(3)
        .run(taskId -> {
          String taskFileName = generateFileForCommitLocation(location, conf, jobContext.getJobID(), taskId);
          dataFiles.addAll(Arrays.asList(readFileForCommit(taskFileName, io)));
        });

    return dataFiles;
  }

  /**
   * Generates the job temp location based on the job configuration.
   * Currently it uses TABLE_LOCATION/temp/QUERY_ID-jobId.
   * @param location The location of the table
   * @param conf The job's configuration
   * @param jobId The JobID for the task
   * @return The file to store the results
   */
  @VisibleForTesting
  static String generateJobLocation(String location, Configuration conf, JobID jobId) {
    String queryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    return location + "/temp/" + queryId + "-" + jobId;
  }

  /**
   * Generates file location based on the task configuration and a specific task id.
   * This file will be used to store the data required to generate the Iceberg commit.
   * Currently it uses TABLE_LOCATION/temp/QUERY_ID-jobId/task-[0..numTasks).forCommit.
   * @param location The location of the table
   * @param conf The job's configuration
   * @param jobId The jobId for the task
   * @param taskId The taskId for the commit file
   * @return The file to store the results
   */
  private static String generateFileForCommitLocation(String location, Configuration conf, JobID jobId, int taskId) {
    return generateJobLocation(location, conf, jobId) + "/task-" + taskId + FOR_COMMIT_EXTENSION;
  }

  private static void createFileForCommit(DataFile[] closedFiles, String location, FileIO io)
      throws IOException {

    OutputFile fileForCommit = io.newOutputFile(location);
    try (ObjectOutputStream oos = new ObjectOutputStream(fileForCommit.createOrOverwrite())) {
      oos.writeObject(closedFiles);
    }
    LOG.debug("Iceberg committed file is created {}", fileForCommit);
  }

  private static DataFile[] readFileForCommit(String fileForCommitLocation, FileIO io) {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(fileForCommitLocation).newStream())) {
      return (DataFile[]) ois.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse committed file: %s", fileForCommitLocation);
    }
  }
}
