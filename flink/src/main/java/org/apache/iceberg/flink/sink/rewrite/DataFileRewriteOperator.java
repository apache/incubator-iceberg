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

package org.apache.iceberg.flink.sink.rewrite;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class DataFileRewriteOperator extends
    AbstractRewriteOperator<FileScanTaskGenerateOperatorOut, DataFileRewriteOperatorOut> {
  private TaskWriterFactory<RowData> writerFactory;
  private FileIO io;
  private Schema schema;
  private EncryptionManager encryptionManager;
  private boolean caseSensitive;
  private String nameMapping;

  private transient RowDataRewriter rowDataRewriter;

  public DataFileRewriteOperator(TableLoader tableLoader,
                                 Schema schema,
                                 FileIO io,
                                 EncryptionManager encryptionManager,
                                 boolean caseSensitive,
                                 Map<String, String> tblProperties,
                                 TaskWriterFactory<RowData> writerFactory) {
    super(tableLoader);
    this.io = io;
    this.schema = schema;
    this.encryptionManager = encryptionManager;
    this.writerFactory = writerFactory;
    this.caseSensitive = caseSensitive;
    this.nameMapping = tblProperties.get(TableProperties.DEFAULT_NAME_MAPPING);
  }

  @Override
  public void open() throws Exception {
    writerFactory.initialize(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getAttemptNumber());
    this.rowDataRewriter = new RowDataRewriter(io, schema, nameMapping, encryptionManager,
        writerFactory, caseSensitive);
  }

  @Override
  public void processElement(StreamRecord<FileScanTaskGenerateOperatorOut> element) throws Exception {
    if (element != null) {
      CombinedScanTask task = element.getValue().getOneCombineScanTask();
      int totalCombinedScanTaskNums = element.getValue().getTotalCombineScanTasks();
      long taskMillis = element.getValue().getCurrentTaskMillis();

      List<FileScanTask> fileScanTasks = Lists.newArrayList(task.files());

      List<DataFile> currentDataFiles = fileScanTasks.stream()
          .map(FileScanTask::file)
          .collect(Collectors.toList());
      // filter files which don't need to be rewrote.
      List<FileScanTask> filterFileScanTasks = fileScanTasks.stream()
          .filter(fileScanTask -> fileScanTask.file().fileSizeInBytes() >= getTargetSizeInBytes())
          .collect(Collectors.toList());
      // Remove the scan task which don't need to rewrite. we should return these after rewrite.
      fileScanTasks.removeAll(filterFileScanTasks);
      if (fileScanTasks.size() > 0) {
        LOG.info("There are {} file need to be rewrite.", fileScanTasks.size());
        CombinedScanTask needRewriteScanTask = new BaseCombinedScanTask(fileScanTasks);
        List<DataFile> rewriteDataFiles = rowDataRewriter.rewriteDataForTask(needRewriteScanTask);
        List<DataFile> noNeedRewriteDataFiles = filterFileScanTasks.stream()
            .map(FileScanTask::file)
            .collect(Collectors.toList());
        // return back no-rewrite datafiles
        rewriteDataFiles.addAll(noNeedRewriteDataFiles);

        DataFileRewriteOperatorOut result = new DataFileRewriteOperatorOut(rewriteDataFiles, currentDataFiles,
            taskMillis, totalCombinedScanTaskNums, getRuntimeContext().getIndexOfThisSubtask());
        output.collect(new StreamRecord<>(result));
      }

      LOG.info("Finish current DataFile rewrite");
    } else {
      LOG.warn("Get empty element.");
    }
  }

  @Override
  public void endInput() throws Exception {
  }
}
