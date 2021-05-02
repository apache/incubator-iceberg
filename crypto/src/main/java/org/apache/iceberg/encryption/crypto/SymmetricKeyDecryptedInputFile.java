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

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class SymmetricKeyDecryptedInputFile implements InputFile {
  private final InputFile encryptedInputFile;
  private final CryptoFormat cryptoFormat;
  private final byte[] plaintextDek;
  private final byte[] iv;

  public SymmetricKeyDecryptedInputFile(
      InputFile encryptedInputFile,
      CryptoFormat cryptoFormat,
      byte[] plaintextDek,
      byte[] iv) {
    this.encryptedInputFile = encryptedInputFile;
    this.cryptoFormat = cryptoFormat;
    this.plaintextDek = plaintextDek;
    this.iv = iv;
  }

  @Override
  public long getLength() {
    return cryptoFormat.plaintextLength(encryptedInputFile.getLength());
  }

  @Override
  public SeekableInputStream newStream() {
    SeekableInputStream encryptedInputStream = encryptedInputFile.newStream();
    return cryptoFormat.decryptionStream(
        encryptedInputStream, encryptedInputFile.getLength(), plaintextDek, iv);
  }

  @Override
  public String location() {
    return encryptedInputFile.location();
  }

  @Override
  public boolean exists() {
    return encryptedInputFile.exists();
  }
}
