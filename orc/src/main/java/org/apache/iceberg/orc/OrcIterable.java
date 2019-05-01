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

package org.apache.iceberg.orc;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Iterable used to read rows from ORC.
 */
public class OrcIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Schema schema;
  private final Function<Schema, OrcValueReader<?>> readerFunction;
  private final VectorizedRowBatchIterator orcIter;

  public OrcIterable(InputFile file, Configuration config, Schema schema,
                     Long start, Long length,
                     Function<Schema, OrcValueReader<?>> readerFunction) {
    this.schema = schema;
    this.readerFunction = readerFunction;
    final Reader orcFileReader = newFileReader(file, config);
    this.orcIter = newOrcIterator(file, TypeConversion.toOrc(schema, new ColumnIdMap()),
        start, length, orcFileReader);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<T> iterator() {
    return new OrcIterator(orcIter, (OrcValueReader<T>) readerFunction.apply(schema));
  }

  private static VectorizedRowBatchIterator newOrcIterator(InputFile file, TypeDescription orcSchema,
                                                           Long start, Long length,
                                                           Reader orcFileReader) {
    final Reader.Options options = orcFileReader.options();
    if (start != null) {
      options.range(start, length);
    }
    options.schema(orcSchema);

    try {
      return new VectorizedRowBatchIterator(file.location(), orcSchema, orcFileReader.rows(options));
    }
    catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get ORC rows for file: %s", file);
    }
  }

  private static Reader newFileReader(InputFile file, Configuration config) {
    try {
      return OrcFile.createReader(new Path(file.location()),
          OrcFile.readerOptions(config));
    }
    catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file);
    }
  }

  private class OrcIterator implements Iterator<T> {

    private int nextRow;
    private VectorizedRowBatch current;

    final VectorizedRowBatchIterator batchIter;
    final OrcValueReader<T> reader;

    OrcIterator(VectorizedRowBatchIterator batchIter, OrcValueReader<T> reader) {
      this.batchIter = batchIter;
      this.reader = reader;
      current = null;
      nextRow = 0;
    }

    @Override
    public boolean hasNext() {
      return (current != null && nextRow < current.size) || batchIter.hasNext();
    }

    @Override
    public T next() {
      if (current == null || nextRow >= current.size) {
        current = batchIter.next();
        nextRow = 0;
      }

      return this.reader.read(current, nextRow++);
    }
  }

}
