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

import org.apache.iceberg.io.OutputFile;

// TODO update class description
/**
 * Thin wrapper around a {@link OutputFile} that is encrypting bytes written to the underlying
 * file system, via an encryption key that is symbolized by the enclosed
 * {@link EncryptionKeyMetadata}.
 * <p>
 * The {@link EncryptionManager} returns instances of these when passed output files that should
 * be encrypted as they are being written to the backing file system.
 */
public interface EncryptedOutputFile {

  /**
   * Use flat filestream encryption (default) or native format encryption
   */
  default boolean useNativeEncryption() {
    return false;
  }

  /**
   * An OutputFile instance that encrypts the bytes that are written to its output streams.
   */
  OutputFile encryptingOutputFile();

  /**
   * Metadata about the encryption key that is being used to encrypt the associated
   * {@link #encryptingOutputFile()}.
   */
  EncryptionKeyMetadata keyMetadata();

  default NativeFileEncryptParams nativeEncryptionParameters() {
    return null;
  }
}
