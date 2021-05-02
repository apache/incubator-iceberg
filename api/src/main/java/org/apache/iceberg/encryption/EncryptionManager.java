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

import java.io.Serializable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Module for encrypting and decrypting table data files.
 * <p>
 * This must be serializable because an instance may be instantiated in one place and sent across
 * the wire in some Iceberg integrations, notably Spark.
 */
public interface EncryptionManager extends Serializable {

  /**
   * Given an {@link EncryptedInputFile#encryptedInputFile()} representing the raw encrypted bytes
   * from the underlying file system, and given metadata about how the file was encrypted via
   * {@link EncryptedInputFile#keyMetadata()}, return an {@link InputFile} that returns decrypted
   * input streams.
   */
  InputFile decrypt(EncryptedInputFile encrypted);

  /**
   * Variant of {@link #decrypt(EncryptedInputFile)} that provides a sequence of files that all
   * need to be decrypted in a single context.
   * <p>
   * By default this calls the single-file decryption method for each element in the iterator.
   * Implementations can override this for a variety of optimizations. For example, an
   * implementation can perform lookahead on the input iterator and fetch encryption keys in batch.
   */
  default Iterable<InputFile> decrypt(Iterable<EncryptedInputFile> encrypted) {
    return Iterables.transform(encrypted, this::decrypt);
  }

  /**
   * Given a handle on an {@link OutputFile} that writes raw bytes to the underlying file system,
   * return a bundle of an {@link EncryptedOutputFile#encryptingOutputFile()} that writes encrypted
   * bytes to the underlying file system, and the {@link EncryptedOutputFile#keyMetadata()} that
   * points to the encryption key that is being used to encrypt this file.
   */
  EncryptedOutputFile encrypt(OutputFile rawOutput);

  /**
   * Variant of {@link #encrypt(OutputFile)} that provides a sequence of files that all need to be
   * encrypted in a single context.
   * <p>
   * By default this calls the single-file encryption method for each element in the iterator.
   * Implementations can override this for a variety of optimizations. For example, an
   * implementation can perform lookahead on the input iterator and fetch encryption keys in batch.
   */
  default Iterable<EncryptedOutputFile> encrypt(Iterable<OutputFile> rawOutput) {
    return Iterables.transform(rawOutput, this::encrypt);
  }

  /**
   * Encrypt a manifest list
   * @param rawOutput raw output file
   * @return encrypted output file
   */
  default EncryptedOutputFile encryptManifestList(OutputFile rawOutput) {
    return encrypt(rawOutput);
  }

  /**
   * Encrypt a manifest file
   * @param rawOutput raw output file
   * @return encrypted output file
   */
  default EncryptedOutputFile encryptManifestFile(OutputFile rawOutput) {
    return encrypt(rawOutput);
  }

}
