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

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

public class ParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, ParquetValueReader<?>> readerFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;

  public ParquetReader(InputFile input, Schema expectedSchema, ParquetReadOptions options,
                       Function<MessageType, ParquetValueReader<?>> readerFunc, NameMapping nameMapping,
                       Expression filter, boolean reuseContainers, boolean caseSensitive) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.readerFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
  }

  private ReadConf<T> conf = null;

  private ReadConf<T> init() {
    if (conf == null) {
      ReadConf<T> readConf = new ReadConf<>(
          input, options, expectedSchema, filter, readerFunc, null, nameMapping, reuseContainers,
          caseSensitive, null);
      this.conf = readConf.copy();
      return readConf;
    }
    return conf;
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init());
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final ParquetFileReader reader;
    private final ParquetValueReader<T> model;
    private final long totalValues;
    private final boolean reuseContainers;
    private final long[] rowGroupsStartRowPos;
    private final boolean hasRecordFilter;

    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    FileIterator(ReadConf<T> conf) {
      this.reader = conf.reader();
      this.model = conf.model();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
      this.rowGroupsStartRowPos = conf.startRowPositions();
      this.hasRecordFilter = conf.hasRecordFilter();
    }

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      if (reuseContainers) {
        this.last = model.read(last);
      } else {
        this.last = model.read(null);
      }
      valuesRead += 1;

      return last;
    }

    private void advance() {
      PageReadStore pages;
      try {
        // Because of the issue of PARQUET-1901, we cannot blindly call readNextFilteredRowGroup()
        if (hasRecordFilter) {
          pages = reader.readNextFilteredRowGroup();
        } else {
          pages = reader.readNextRowGroup();
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      long rowPosition = rowGroupsStartRowPos[nextRowGroup];
      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;

      model.setPageSource(pages, rowPosition);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
