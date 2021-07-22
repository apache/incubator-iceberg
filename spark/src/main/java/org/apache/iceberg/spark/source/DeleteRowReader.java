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

import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;

public class DeleteRowReader extends RowDataReader {
  private final Schema expectedSchema;
  private final FileContent deleteSelector;

  public DeleteRowReader(CombinedScanTask task, Table table, Schema expectedSchema,
                         boolean caseSensitive, FileContent deleteContent) {
    super(task, table, expectedSchema, caseSensitive);
    this.expectedSchema = expectedSchema;
    this.deleteSelector = deleteContent;
  }

  @Override
  CloseableIterator<InternalRow> open(FileScanTask task) {
    SparkDeleteFilter matches = new SparkDeleteFilter(task, tableSchema(), expectedSchema);

    // schema or rows returned by readers
    Schema requiredSchema = matches.requiredSchema();
    Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, RowDataReader::convertConstant);
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    if (deleteSelector == null) {
      return matches.keepRowsFromDeletes(open(task, requiredSchema, idToConstant)).iterator();
    } else if (deleteSelector.equals(FileContent.EQUALITY_DELETES)) {
      return matches.keepRowsFromEqualityDeletes(open(task, requiredSchema, idToConstant)).iterator();
    } else {
      return matches.keepRowsFromPosDeletes(open(task, requiredSchema, idToConstant)).iterator();
    }
  }
}
