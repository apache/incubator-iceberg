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
 * Encryption properties that are stored in {@link EnvelopeConfig#properties()}
 */
public class EncryptionProperties {

  private EncryptionProperties() {
  }

  /**
   * Type of a static data mask that can be applied to a specific column.
   */
  public static final String MASK_TYPE = "mask.type";

  /**
   * With a null mask, reader with no access to the encrypted column
   * will see all column values as null.
   */
  public static final String MASK_TYPE_NULL = "null";

  /**
   * With a redact mask, reader with no access to the encrypted column
   * will see a partially hidden text value, such as 1XX-XXXX-XXX8
   */
  public static final String MASK_TYPE_REDACT = "redact";

  /**
   * With a hash mask, reader with no access to the encrypted column
   * will see the hash value of the original value.
   */
  public static final String MASK_TYPE_HASH = "hash";

  /**
   * A custom mask, writer can define what the reader sees
   * when the reader has no access to the encrypted column.
   * It requires loading certain mask implementation based on the underlying file format.
   */
  public static final String MASK_TYPE_CUSTOM = "custom";

  /**
   * Prefix for any key-value pairs that should be added as a part of
   * the additional authenticated data (AAD) during key generation.
   */
  public static final String AAD_PREFIX = "aad.";
}
