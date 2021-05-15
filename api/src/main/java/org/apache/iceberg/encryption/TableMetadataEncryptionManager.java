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
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * An encryption manager to handle top level table metadata file encryption.
 * <p>
 * Unlike other Iceberg metadata such as manifest list and manifest,
 * because table metadata is the top level Iceberg metadata,
 * we do not leverage any other file to store its encryption information.
 * Therefore, this encryption manager assumes all encryption metadata are externally managed,
 * and directly transforms the file to its encrypting or decrypting form.
 */
public interface TableMetadataEncryptionManager extends Serializable {

  /**
   * Decrypt a table metadata file.
   * @param encrypted encrypted input file
   * @return decrypting input file
   */
  InputFile decrypt(InputFile encrypted);

  /**
   * Encrypt a table metadata file.
   * @param rawOutput raw output file
   * @return encrypting output file
   */
  OutputFile encrypt(OutputFile rawOutput);

  /**
   * Initialize the encryption manager from catalog properties.
   * @param properties catalog properties
   */
  default void initialize(Map<String, String> properties) {
  }
}
