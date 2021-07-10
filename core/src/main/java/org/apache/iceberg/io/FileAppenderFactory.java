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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;

/**
 * Factory to create a new {@link FileAppender} to write records.
 *
 * @param <T> data type of the rows to append.
 */
public interface FileAppenderFactory<T> {

  /**
   * Create a new {@link FileAppender}.
   *
   * @param outputFile an OutputFile used to create an output stream.
   * @param fileFormat File format.
   * @return a newly created {@link FileAppender}
   */
  FileAppender<T> newAppender(OutputFile outputFile, FileFormat fileFormat);

  // TODO document this, or change the previous function
  default FileAppender<T> newAppender(EncryptedOutputFile outputFile, FileFormat fileFormat) {
    return null;
  }

  /**
   * Create a new {@link DataWriter}.
   *
   * @param outputFile an OutputFile used to create an output stream.
   * @param format     a file format
   * @param partition  a tuple of partition values
   * @return a newly created {@link DataWriter} for rows
   */
  DataWriter<T> newDataWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition);

  /**
   * Create a new {@link EqualityDeleteWriter}.
   *
   * @param outputFile an OutputFile used to create an output stream.
   * @param format     a file format
   * @param partition  a tuple of partition values
   * @return a newly created {@link EqualityDeleteWriter} for equality deletes
   */
  EqualityDeleteWriter<T> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition);

  /**
   * Create a new {@link PositionDeleteWriter}.
   *
   * @param outputFile an OutputFile used to create an output stream.
   * @param format     a file format
   * @param partition  a tuple of partition values
   * @return a newly created {@link EqualityDeleteWriter} for position deletes
   */
  PositionDeleteWriter<T> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition);
}
