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

import java.io.Serializable;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.RowDataIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDataRewriter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataRewriter.class);

  private final Schema schema;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final String nameMapping;
  private final TaskWriterFactory<RowData> taskWriterFactory;
  private final boolean caseSensitive;

  public RowDataRewriter(FileIO io, Schema schema, String nameMapping, EncryptionManager encryptionManager,
                         TaskWriterFactory<RowData> taskWriterFactory, boolean caseSensitive) {
    this.schema = schema;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
    this.taskWriterFactory = taskWriterFactory;
  }

  public List<DataFile> rewriteDataForTask(CombinedScanTask task) throws Exception {

    TaskWriter<RowData> writer = taskWriterFactory.create();
    try (RowDataIterator dataIterator = new RowDataIterator(
        task, io, encryptionManager, schema, schema, nameMapping, caseSensitive)) {
      while (dataIterator.hasNext()) {
        RowData row = dataIterator.next();
        writer.write(row);
      }
      writer.close();
      return Lists.newArrayList(writer.complete());
    } catch (Throwable originalThrowable) {
      try {
        writer.abort();
        LOG.error("Aborting task.", originalThrowable);
      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }
      }

      if (originalThrowable instanceof Exception) {
        throw originalThrowable;
      } else {
        throw new RuntimeException(originalThrowable);
      }
    }
  }
}
