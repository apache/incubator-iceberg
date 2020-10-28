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
import org.apache.iceberg.DataFile;

public class DataFileRewriteOperatorOut {
  private long taskTriggerMillis;
  private int taskNums;
  private int subTaskId;
  private List<DataFile> addedDataFiles;
  private List<DataFile> currentDataFiles;

  /**
   * @param addedDataFiles new datafiles generated by rewrite operator.
   * @param currentDataFiles current datafiles need to be replaced.
   * @param taskMillis current merge task trigger time.
   * @param taskNums total taskNums
   * @param subTaskId current flink subtask id.
   */
  public DataFileRewriteOperatorOut(List<DataFile> addedDataFiles, List<DataFile> currentDataFiles,
                                    long taskMillis, int taskNums, int subTaskId) {
    this.addedDataFiles = addedDataFiles;
    this.taskNums = taskNums;
    this.subTaskId = subTaskId;
    this.taskTriggerMillis = taskMillis;
    this.currentDataFiles = currentDataFiles;
  }

  public List<DataFile> getAddedDataFiles() {
    return addedDataFiles;
  }

  public List<DataFile> getCurrentDataFiles() {
    return currentDataFiles;
  }

  public long getTaskTriggerMillis() {
    return taskTriggerMillis;
  }

  public int getTaskNums() {
    return taskNums;
  }

  public int getSubTaskId() {
    return subTaskId;
  }
}
