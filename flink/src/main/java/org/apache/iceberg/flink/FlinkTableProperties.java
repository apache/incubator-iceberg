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

package org.apache.iceberg.flink;

/**
 * Table Properties when using Flink IcebergSink.
 */
public class FlinkTableProperties {

  private FlinkTableProperties() {
  }

  public static final String CASE_SENSITIVE = "case-sensitive";
  public static final boolean CASE_SENSITIVE_DEFAULT = false;

  public static final String NUMS_DATAFILE_MERGE = "nums.datafile.merge";
  public static final int NUMS_DATAFILE_MERGE_DEFAULT = 100;

  public static final String NUMS_NEW_SNAPSHOT_MERGE = "nums.new-snapshot.merge";
  public static final int NUMS_NEW_SNAPSHOT_MERGE_DEFAULT = 100;

  public static final String SNAPSHOT_RETAIN_LAST_MINUTES = "snapshot.retain.last.minutes";
  public static final int SNAPSHOT_RETAIN_LAST_MINUTES_DEFAULT = -1;

  public static final String SNAPSHOT_RETAIN_LAST_NUMS = "snapshot.retain.last.nums";
  public static final int SNAPSHOT_RETAIN_LAST_NUMS_DEFAULT = 100;
}
