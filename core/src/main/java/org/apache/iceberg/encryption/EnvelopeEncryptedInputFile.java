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

package org.apache.iceberg.encryption;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * An output file that decrypts data based on the given envelope encryption metadata.
 * TODO: currently just pass through, will add concrete logic in a separated PR.
 */
public class EnvelopeEncryptedInputFile implements InputFile {

  private final InputFile encryptedInput;
  private final EnvelopeMetadata metadata;

  /**
   * Constructor
   *
   * @param encryptedInput encrypted input file
   * @param metadata metadata of type {@link BaseEncryptionKeyMetadata} that only wraps the bytes
   */
  public EnvelopeEncryptedInputFile(InputFile encryptedInput, EnvelopeMetadata metadata) {
    this.encryptedInput = encryptedInput;
    this.metadata = metadata;
  }

  @Override
  public long getLength() {
    return encryptedInput.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return new EnvelopeDecryptingInputStream(encryptedInput.newStream(), metadata);
  }

  @Override
  public String location() {
    return encryptedInput.location();
  }

  @Override
  public boolean exists() {
    return encryptedInput.exists();
  }
}
