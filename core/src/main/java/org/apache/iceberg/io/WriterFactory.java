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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;

/**
 * A factory for creating data and delete writers.
 */
public interface WriterFactory<T> {

  /**
   * Creates a new {@link DataWriter}.
   *
   * @param file the output file
   * @param format the file format to use
   * @param spec the partition spec the written data belongs to
   * @param partition the partition the written data belongs to
   * @return the constructed data writer
   */
  DataWriter<T> newDataWriter(EncryptedOutputFile file, FileFormat format,
                              PartitionSpec spec, StructLike partition);

  /**
   * Creates a new {@link EqualityDeleteWriter}.
   *
   * @param file the output file
   * @param format the file format to use
   * @param spec the partition spec the written deletes belong to
   * @param partition the partition the written deletes belong to
   * @return the constructed equality delete writer
   */
  EqualityDeleteWriter<T> newEqualityDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                  PartitionSpec spec, StructLike partition);

  /**
   * Creates a new {@link PositionDeleteWriter}.
   *
   * @param file the output file
   * @param format the file format to use
   * @param spec the partition spec the written deletes belong to
   * @param partition the partition the written deletes belong to
   * @return the constructed position delete writer
   */
  PositionDeleteWriter<T> newPositionDeleteWriter(EncryptedOutputFile file, FileFormat format,
                                                  PartitionSpec spec, StructLike partition);
}
