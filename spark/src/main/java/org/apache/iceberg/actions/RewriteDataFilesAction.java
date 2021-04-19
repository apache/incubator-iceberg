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

package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.RowDataRewriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class RewriteDataFilesAction
    extends BaseRewriteDataFilesAction<RewriteDataFilesAction> {

  private final JavaSparkContext sparkContext;
  private FileIO fileIO;

  RewriteDataFilesAction(SparkSession spark, Table table) {
    super(table);
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }

  @Override
  protected FileIO fileIO() {
    if (this.fileIO == null) {
      this.fileIO = SparkUtil.serializableFileIO(table());
    }
    return this.fileIO;
  }

  @Override
  protected RewriteResult rewriteDataForTasks(List<CombinedScanTask> combinedScanTasks) {
    JavaRDD<CombinedScanTask> taskRDD = sparkContext.parallelize(combinedScanTasks, combinedScanTasks.size());
    Broadcast<FileIO> io = sparkContext.broadcast(fileIO());
    Broadcast<EncryptionManager> encryption = sparkContext.broadcast(encryptionManager());
    RowDataRewriter rowDataRewriter =
        new RowDataRewriter(table(), spec(), caseSensitive(), io, encryption);
    return rowDataRewriter.rewriteDataForTasks(taskRDD);
  }
}
