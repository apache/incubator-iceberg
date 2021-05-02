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

/**
 * Algorithm supported for file encryption.
 */
public enum EncryptionAlgorithm {
  /**
   * Counter mode (CTR) allows fast encryption with high throughput.
   * It is an encryption only cipher and does not ensure content integrity.
   * Inputs to CTR cipher are:
   *  1. encryption key
   *  2. a 16-byte initialization vector (12-byte nonce, 4-byte counter)
   *  3. plaintext data
   */
  AES_CTR,
  /**
   * Galois/Counter mode (GCM) combines CTR with the new Galois mode of authentication.
   * It not only ensures data confidentiality, but also ensures data integrity.
   * Inputs to GCM cipher are:
   *  1. encryption key
   *  2. a 12-byte initialization vector
   *  3. additional authenticated data
   *  4. plaintext data
   */
  AES_GCM,
  /**
   * A combination of GCM and CTR that can be used for file types like Parquet,
   * so that all modules except pages are encrypted by GCM to ensure integrity,
   * and CTR is used for efficient encryption of bulk data.
   * The tradeoff is that attackers would be able to tamper page data.
   */
  AES_GCM_CTR
}
