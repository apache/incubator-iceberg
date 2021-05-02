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
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class SymmetricKeyEncryptedOutputFile implements OutputFile {
  private final OutputFile plaintextOutputFile;
  private final CryptoFormat cryptoFormat;
  private final byte[] plaintextDek;
  private final byte[] iv;

  public SymmetricKeyEncryptedOutputFile(
      OutputFile plaintextOutputFile,
      CryptoFormat cryptoFormat,
      byte[] plaintextDek,
      byte[] iv) {
    this.plaintextOutputFile = plaintextOutputFile;
    this.plaintextDek = plaintextDek;
    this.cryptoFormat = cryptoFormat;
    this.iv = iv;
  }

  @Override
  public PositionOutputStream create() {
    PositionOutputStream plaintextOutputStream = plaintextOutputFile.create();
    return cryptoFormat.encryptionStream(plaintextOutputStream, plaintextDek, iv);
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    PositionOutputStream plaintextOutputStream = plaintextOutputFile.createOrOverwrite();
    return cryptoFormat.encryptionStream(plaintextOutputStream, plaintextDek, iv);
  }

  @Override
  public String location() {
    return plaintextOutputFile.location();
  }

  @Override
  public InputFile toInputFile() {
    return plaintextOutputFile.toInputFile();
  }
}
