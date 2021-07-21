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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public abstract class OSSTestBase {

  @ClassRule
  public static final OSSTestRule OSS_TEST_RULE = OSSTestRule.initialize();

  private final SerializableSupplier<OSS> oss = OSS_TEST_RULE::createOSSClient;
  private final String bucketName = OSS_TEST_RULE.testBucketName();
  private final String keyPrefix = OSS_TEST_RULE.keyPrefix();

  private OSSFileIO fileIO;

  @Before
  public void before() {
    fileIO = new OSSFileIO(oss);

    OSS_TEST_RULE.setUpBucket(bucketName);
  }

  @After
  public void after() {
    OSS_TEST_RULE.tearDownBucket(bucketName);
  }

  protected String location(String key) {
    return String.format("oss://%s/%s%s", bucketName, keyPrefix, key);
  }

  protected SerializableSupplier<OSS> oss() {
    return oss;
  }

  protected FileIO fileIO() {
    return fileIO;
  }
}
