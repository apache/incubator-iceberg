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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

/**
 * Helper for appending {@link DataFile} to a table or appending {@link Record}s to a table.
 */
public class GenericAppenderHelper {

  private final Table table;
  private final FileFormat fileFormat;
  private final TemporaryFolder tmp;

  public GenericAppenderHelper(Table table, FileFormat fileFormat, TemporaryFolder tmp) {
    this.table = table;
    this.fileFormat = fileFormat;
    this.tmp = tmp;
  }

  public void appendToTable(DataFile... dataFiles) {
    Preconditions.checkNotNull(table, "table not set");

    AppendFiles append = table.newAppend();

    for (DataFile dataFile : dataFiles) {
      append = append.appendFile(dataFile);
    }

    append.commit();
  }

  public void appendToTable(List<Record> records) throws IOException {
    appendToTable(null, records);
  }

  public void appendToTable(StructLike partition, List<Record> records) throws IOException {
    appendToTable(writeFile(partition, records));
  }

  public DataFile writeFile(StructLike partition, List<Record> records) throws IOException {
    Preconditions.checkNotNull(table, "table not set");
    File file = tmp.newFile();
    Assert.assertTrue(file.delete());
    return appendToLocalFile(table, file, fileFormat, partition, records);
  }

  private static DataFile appendToLocalFile(
      Table table, File file, FileFormat format, StructLike partition, List<Record> records)
      throws IOException {
    FileAppender<Record> appender = new GenericAppenderFactory(table).newAppender(
        Files.localOutput(file), format);
    try (FileAppender<Record> fileAppender = appender) {
      fileAppender.addAll(records);
    }

    return DataFiles.builder(table.spec())
        .withRecordCount(records.size())
        .withFileSizeInBytes(file.length())
        .withPath(Files.localInput(file).location())
        .withMetrics(appender.metrics())
        .withFormat(format)
        .withPartition(partition)
        .build();
  }
}
