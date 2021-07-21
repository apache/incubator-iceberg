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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

class OSSInputFile extends BaseOSSFile implements InputFile {

  OSSInputFile(OSS client, OSSURI uri, AliyunProperties aliyunProperties) {
    super(client, uri, aliyunProperties);
  }

  @Override
  public long getLength() {
    return client().getSimplifiedObjectMeta(uri().bucket(), uri().key()).getSize();
  }

  @Override
  public SeekableInputStream newStream() {
    return new OSSInputStream(client(), uri());
  }

  @Override
  public boolean exists() {
    return doesObjectExists();
  }
}
