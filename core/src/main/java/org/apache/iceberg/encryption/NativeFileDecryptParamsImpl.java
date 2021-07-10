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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * The data keys and other parameters should be retrieved/unwrapped centrally (e.g., in a driver), by parsing the
 * manifest key_metadata entry for a data file; and then sent to the worker that reads/decrypts this file in a native
 * format.
 * Key unwrapping requires authorization checks, and can involve interaction with a KMS. Therefore, unwrap only
 * projected columns.
 */
public class NativeFileDecryptParamsImpl implements NativeFileDecryptParams {
  private ByteBuffer fileAadPrefix;
  private Map<String, ByteBuffer> fileDataKeys;

  private NativeFileDecryptParamsImpl(Map<String, ByteBuffer> fileDataKeys, ByteBuffer fileAadPrefix) {
    this.fileDataKeys = fileDataKeys;
    this.fileAadPrefix = fileAadPrefix;
  }

  /**
   * Data encryption keys for a single file.
   * NOTE: pass keys only for projected columns.
   * @param dataKeys Map dekId -> dek.
   *                 dekId is unique only within single file scope.
   *                 dekIds are retrieved from manifest key_metadata field, along with the wrapped DEKs.
   */
  public static Builder create(Map<String, ByteBuffer> dataKeys) {
    return new Builder(dataKeys);
  }

  public static class Builder {
    private ByteBuffer fileAadPrefix;
    private Map<String, ByteBuffer> fileDataKeys;

    private Builder(Map<String, ByteBuffer> dataKeys) {
      this.fileDataKeys = dataKeys;
    }

    public Builder aadPrefix(ByteBuffer aadPrefix) {
      this.fileAadPrefix = aadPrefix;
      return this;
    }

    public NativeFileDecryptParamsImpl build() {
      return new NativeFileDecryptParamsImpl(fileDataKeys, fileAadPrefix);
    }
  }

  @Override
  public ByteBuffer aadPrefix() {
    return fileAadPrefix;
  }

  @Override
  public Map<String, ByteBuffer> fileDataKeys() {
    return fileDataKeys;
  }
}
