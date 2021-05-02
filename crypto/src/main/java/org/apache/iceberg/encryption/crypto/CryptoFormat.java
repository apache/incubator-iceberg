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

package org.apache.iceberg.encryption.crypto;

import java.io.Serializable;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * A {@link CryptoFormat} provides methods to encrypt an output stream and decrypt and input stream
 * given a dek.
 */
public interface CryptoFormat extends Serializable {
  SeekableInputStream decryptionStream(
      SeekableInputStream encryptedStream, Long rawLength, byte[] plaintextDek, byte[] iv);

  PositionOutputStream encryptionStream(
      PositionOutputStream plaintextStream, byte[] plaintextDek, byte[] iv);

  long plaintextLength(long encryptedLength);

  /** @return the dek length in number of bytes which this {@link CryptoFormat} supports */
  int dekLength();

  /** @return the iv length in number of bytes which this {@link CryptoFormat} supports */
  default int ivLength() {
    return 0;
  }
}
