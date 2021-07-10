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
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Per-data-file decryption parameters.
 * The data keys and AADPrefix should be retrieved/unwrapped centrally (e.g., in a driver), by parsing the
 * manifest key_metadata entry for a data file; and then sent to the worker that reads/decrypts this file in a native
 * format.
 * Key unwrapping requires authorization checks, and can involve interaction with a KMS. Therefore, unwrap only
 * projected columns.
 */
public interface NativeFileDecryptParams extends Serializable  {

  /**
   * Data encryption keys for a single file.
   * NOTE: pass keys only for projected columns.
   * Passed as a Map (dekId to dek).
   * dekId is unique only within single file scope.
   * dekIds are retrieved from manifest key_metadata field, along with the wrapped DEKs.
   */
  Map<String, ByteBuffer> fileDataKeys();

  ByteBuffer aadPrefix();
}
