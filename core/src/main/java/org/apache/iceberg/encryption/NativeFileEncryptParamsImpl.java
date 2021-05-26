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
 * The data keys and other parameters should be generated centrally (e.g., in a driver).
 * Each object (/set of keys) must be created for one data file only, and sent to the worker that writes/encrypts
 * this file in a native format.
 * The central process, that generates the data keys, will wrap them (encrypt with master keys) and store in the
 * manifest key_metadata entry for the data file. Key wrapping can involve interaction with a KMS.
 */
public class NativeFileEncryptParamsImpl implements NativeFileEncryptParams {
  private ByteBuffer fileAadPrefix;
  private Map<String, ByteBuffer> fileDataKeys;
  private String fileDekId;
  private Map<String, String> columnDekIds;

  private NativeFileEncryptParamsImpl(Map<String, ByteBuffer> fileDataKeys, String fileDekId,
                                      Map<String, String> columnDekIds, ByteBuffer fileAadPrefix) {
    // TODO check
    this.fileDataKeys = fileDataKeys;
    this.fileDekId = fileDekId;
    this.columnDekIds = columnDekIds;
    this.fileAadPrefix = fileAadPrefix;
  }

  /**
   * Data encryption keys for a single file.
   * @param dataKeys Map dekId -> dek.
   *                 dekId is unique only within single file scope, can be a simple counter.
   *                 dekIds must be stored in manifest key_metadata field, along with the wrapped DEKs.
   */
  public static Builder create(Map<String, ByteBuffer> dataKeys) {
    return new Builder(dataKeys);
  }

  public static class Builder {
    private ByteBuffer fileAadPrefix;
    private Map<String, ByteBuffer> fileDataKeys;
    private String fileDekId;
    private Map<String, String> columnDekIds;

    private Builder(Map<String, ByteBuffer> dataKeys) {
      this.fileDataKeys = dataKeys;
    }

    public Builder fileKeyId(String keyId) {
      this.fileDekId = keyId;
      return this;
    }

    public Builder columnKeyIds(Map<String, String> columnKeyIds) {
      this.columnDekIds = columnKeyIds;
      return this;
    }

    public Builder aadPrefix(ByteBuffer aadPrefix) {
      this.fileAadPrefix = aadPrefix;
      return this;
    }

    public NativeFileEncryptParamsImpl build() {
      return new NativeFileEncryptParamsImpl(fileDataKeys, fileDekId, columnDekIds, fileAadPrefix);
    }
  }

  @Override
  public ByteBuffer aadPrefix() {
    return fileAadPrefix;
  }

  @Override
  public String fileDekId() {
    return fileDekId;
  }

  @Override
  public Map<String, ByteBuffer> fileDataKeys() {
    return fileDataKeys;
  }

  @Override
  public Map<String, String> columnDekIds() {
    return columnDekIds;
  }
}
