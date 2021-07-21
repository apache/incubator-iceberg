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
import java.util.UUID;
import org.apache.iceberg.aliyun.oss.mock.OSSMockRule;
import org.apache.iceberg.common.DynConstructors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public interface OSSTestRule extends TestRule {
  Logger LOG = LoggerFactory.getLogger(OSSTestRule.class);
  UUID RANDOM_UUID = java.util.UUID.randomUUID();

  String OSS_TEST_RULE_CLASS_IMPL = "OSS_TEST_RULE_CLASS_IMPL";

  /**
   * Start the Aliyun Object storage services application that the OSS client could connect to.
   */
  void start();

  /**
   * Stop the Aliyun object storage services.
   */
  void stop();

  /**
   * Returns an newly created {@link OSS} client.
   */
  OSS createOSSClient();

  /**
   * Returns a specific bucket name for testing purpose.
   */
  default String testBucketName() {
    return String.format("oss-testing-bucket-%s", RANDOM_UUID);
  }

  /**
   * Returns the common key prefix for those newly created objects in test cases. For example, we set the test bucket
   * to be 'oss-testing-bucket' and the key prefix to be 'iceberg-objects/', then the produced objects in test cases
   * will be:
   * <pre>
   *   oss://oss-testing-bucket/iceberg-objects/a.dat
   *   oss://oss-testing-bucket/iceberg-objects/b.dat
   *   ...
   * </pre>
   */
  String keyPrefix();

  /**
   * Preparation work of bucket for the test case, for example we need to check the existence of specific bucket.
   */
  void setUpBucket(String bucket);

  /**
   * Clean all the objects that created from this test suite in the bucket.
   */
  void tearDownBucket(String bucket);

  @Override
  default Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        try {
          base.evaluate();
        } finally {
          stop();
        }
      }
    };
  }

  static OSSTestRule initialize() {
    String implClass = System.getenv(OSSIntegrationTestRule.OSS_TEST_RULE_CLASS_IMPL);

    LOG.info("The initializing OSSTestRule implementation is: {}", implClass);

    OSSTestRule testRule;
    if (!StringUtils.isEmpty(implClass)) {
      DynConstructors.Ctor<OSSTestRule> ctor;
      try {
        ctor = DynConstructors.builder(OSSTestRule.class).impl(implClass).buildChecked();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(String.format(
            "Cannot initialize OSSTestRule, missing no-arg constructor: %s", implClass), e);
      }

      try {
        testRule = ctor.newInstance();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Cannot initialize OSSTestRule, %s does not implement OSSTestRule.", implClass), e);
      }

    } else {
      testRule = OSSMockRule.builder().silent().build();
    }

    return testRule;
  }
}
