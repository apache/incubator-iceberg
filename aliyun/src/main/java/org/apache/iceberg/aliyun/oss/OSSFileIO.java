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

package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.OSS;
import java.util.Map;
import org.apache.iceberg.aliyun.AliyunClientFactory;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * FileIO implementation backend by OSS.
 * <p>
 * Locations used must follow the conventions for OSS URIs (e.g. oss://bucket/path...).
 * URIs with schema https are also treated as oss file paths.
 * Using this FileIO with other schemas with result in {@link org.apache.iceberg.exceptions.ValidationException}
 */
public class OSSFileIO implements FileIO {

  private SerializableSupplier<OSS> oss;
  private AliyunProperties aliyunProperties;
  private transient OSS client;

  /**
   * No-arg constructor to load the FileIO dynamically.
   * <p>
   * All fields are initialized by calling {@link OSSFileIO#initialize(Map)} later.
   */
  public OSSFileIO() {
  }

  /**
   * Constructor with custom oss supplier and default aliyun properties.
   * <p>
   * Calling {@link OSSFileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param oss oss supplier
   */
  public OSSFileIO(SerializableSupplier<OSS> oss) {
    this.oss = oss;
    this.aliyunProperties = new AliyunProperties();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new OSSInputFile(client(), new OSSURI(path), aliyunProperties);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new OSSOutputFile(client(), new OSSURI(path), aliyunProperties);
  }

  @Override
  public void deleteFile(String path) {
    OSSURI location = new OSSURI(path);
    client().deleteObject(location.bucket(), location.key());
  }

  private OSS client() {
    if (client == null) {
      client = oss.get();
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AliyunClientFactory factory = AliyunClientFactory.load(properties);
    this.aliyunProperties = factory.aliyunProperties();
    this.oss = factory::oss;
  }
}
