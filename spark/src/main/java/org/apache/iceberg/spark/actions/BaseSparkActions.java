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

package org.apache.iceberg.spark.actions;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.RemoveOrphanFiles;
import org.apache.iceberg.actions.RemoveReachableFiles;
import org.apache.iceberg.actions.RepairManifests;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.spark.sql.SparkSession;

abstract class BaseSparkActions implements ActionsProvider {

  private final SparkSession spark;

  protected BaseSparkActions(SparkSession spark) {
    this.spark = spark;
  }

  protected SparkSession spark() {
    return spark;
  }

  @Override
  public RemoveOrphanFiles removeOrphanFiles(Table table) {
    return new BaseRemoveOrphanFilesSparkAction(spark, table);
  }

  @Override
  public RewriteManifests rewriteManifests(Table table) {
    return new BaseRewriteManifestsSparkAction(spark, table);
  }

  @Override
  public RepairManifests repairManifests(Table table) {
    return new BaseRepairManifestsSparkAction(spark, table);
  }

  @Override
  public ExpireSnapshots expireSnapshots(Table table) {
    return new BaseExpireSnapshotsSparkAction(spark, table);
  }

  @Override
  public RemoveReachableFiles removeReachableFiles(String metadataLocation) {
    return new BaseRemoveReachableFilesSparkAction(spark, metadataLocation);
  }
}
