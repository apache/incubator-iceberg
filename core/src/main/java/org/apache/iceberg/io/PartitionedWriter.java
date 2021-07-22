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

package org.apache.iceberg.io;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PartitionedWriter<T> extends BaseTaskWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedWriter.class);

  private final Set<PartitionKey> completedPartitions = Sets.newHashSet();

  private PartitionKey currentKey = null;
  private RollingFileWriter currentWriter = null;

  protected PartitionedWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                              OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                              Map<String, String> properties) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, properties);
  }

  /**
   * Create a PartitionKey from the values in row.
   * <p>
   * Any PartitionKey returned by this method can be reused by the implementation.
   *
   * @param row a data row
   */
  protected abstract PartitionKey partition(T row);

  @Override
  public void write(T row) throws IOException {
    PartitionKey key = partition(row);

    if (!key.equals(currentKey)) {
      if (currentKey != null) {
        // if the key is null, there was no previous current key and current writer.
        currentWriter.close();
        completedPartitions.add(currentKey);
      }

      if (completedPartitions.contains(key)) {
        // if rows are not correctly grouped, detect and fail the write
        PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
        LOG.warn("Duplicate key: {} == {}", existingKey, key);
        throw new IllegalStateException("Already closed files for partition: " + key.toPath());
      }

      currentKey = key.copy();
      currentWriter = new RollingFileWriter(currentKey);
    }

    currentWriter.write(row);
  }

  @Override
  public void close() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();
    }
  }
}
