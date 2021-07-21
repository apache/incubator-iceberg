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
import org.apache.iceberg.aliyun.AliyunProperties;

abstract class BaseOSSFile {
  private final OSS client;
  private final OSSURI uri;
  private final AliyunProperties aliyunProperties;

  BaseOSSFile(OSS client, OSSURI uri, AliyunProperties aliyunProperties) {
    this.client = client;
    this.uri = uri;
    this.aliyunProperties = aliyunProperties;
  }

  public String location() {
    return uri.location();
  }

  public OSS client() {
    return client;
  }

  public OSSURI uri() {
    return uri;
  }

  AliyunProperties aliyunProperties() {
    return aliyunProperties;
  }

  boolean doesObjectExists() {
    return client.doesObjectExist(uri.bucket(), uri.key());
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
