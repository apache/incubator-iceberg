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

package org.apache.iceberg.aliyun;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class TestAliyunClientFactory {

  @Test
  public void testLoad() {
    AssertHelpers.assertThrows("Invalid argument " + AliyunProperties.OSS_MULTIPART_UPLOAD_THREADS,
        IllegalArgumentException.class,
        () -> AliyunClientFactory.load(
            ImmutableMap.of(AliyunProperties.OSS_MULTIPART_UPLOAD_THREADS, Integer.toString(-1)))
    );

    AssertHelpers.assertThrows("Invalid argument " + AliyunProperties.OSS_MULTIPART_SIZE,
        IllegalArgumentException.class,
        () -> AliyunClientFactory.load(
            ImmutableMap.of(AliyunProperties.OSS_MULTIPART_SIZE, Integer.toString(99 * 1024)))
    );

    AssertHelpers.assertThrows("Invalid argument " + AliyunProperties.OSS_MULTIPART_SIZE,
        IllegalArgumentException.class,
        () -> AliyunClientFactory.load(
            ImmutableMap.of(AliyunProperties.OSS_MULTIPART_SIZE, Long.toString(5 * 1024 * 1024 * 1024L + 1)))
    );

    AssertHelpers.assertThrows("Invalid argument " + AliyunProperties.OSS_MULTIPART_THRESHOLD_SIZE_DEFAULT,
        IllegalArgumentException.class,
        () -> AliyunClientFactory.load(
            ImmutableMap.of(AliyunProperties.OSS_MULTIPART_THRESHOLD_SIZE, Integer.toString(0)))
    );

    AssertHelpers.assertThrows("Invalid argument " + AliyunProperties.OSS_MULTIPART_THRESHOLD_SIZE_DEFAULT,
        IllegalArgumentException.class,
        () -> AliyunClientFactory.load(
            ImmutableMap.of(
                AliyunProperties.OSS_MULTIPART_THRESHOLD_SIZE, Integer.toString(99 * 1024),
                AliyunProperties.OSS_MULTIPART_SIZE, Integer.toString(100 * 1024)
            )));
  }
}
