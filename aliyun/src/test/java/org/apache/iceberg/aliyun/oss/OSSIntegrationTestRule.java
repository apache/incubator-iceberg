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
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * It's used for integration test. Add those environment variables for integration testing.
 * <pre>
 * export OSS_TEST_RULE_CLASS_IMPL=org.apache.iceberg.aliyun.oss.OSSIntegrationTestRule
 * export OSS_TEST_ENDPOINT=${your-oss-endpoint}
 * export OSS_TEST_ACCESS_KEY=${your-oss-access-key}
 * export OSS_TEST_ACCESS_SECRET=${your-oss-access-secret}
 * export OSS_TEST_BUCKET_NAME=${your-oss-bucket-name}
 * export OSS_TEST_KEY_PREFIX=${your-oss-object-key-prefix}
 * </pre>
 */
public class OSSIntegrationTestRule implements OSSTestRule {

  private static final String OSS_TEST_ENDPOINT = "OSS_TEST_ENDPOINT";
  private static final String OSS_TEST_ACCESS_KEY = "OSS_TEST_ACCESS_KEY";
  private static final String OSS_TEST_ACCESS_SECRET = "OSS_TEST_ACCESS_SECRET";
  private static final String OSS_TEST_BUCKET_NAME = "OSS_TEST_BUCKET_NAME";
  private static final String OSS_TEST_KEY_PREFIX = "OSS_TEST_KEY_PREFIX";

  private String endpoint;
  private String accessKey;
  private String accessSecret;
  private String testBucketName;
  private String keyPrefix;

  private OSS lazyClient = null;

  @Override
  public void start() {
    endpoint = System.getenv(OSS_TEST_ENDPOINT);
    Preconditions.checkNotNull(endpoint, "Does not set '%s' environment variable", OSS_TEST_ENDPOINT);

    accessKey = System.getenv(OSS_TEST_ACCESS_KEY);
    Preconditions.checkNotNull(accessKey, "Does not set '%s' environment variable", OSS_TEST_ACCESS_KEY);

    accessSecret = System.getenv(OSS_TEST_ACCESS_SECRET);
    Preconditions.checkNotNull(accessSecret, "Does not set '%s' environment variable", OSS_TEST_ACCESS_SECRET);

    testBucketName = System.getenv(OSS_TEST_BUCKET_NAME);
    Preconditions.checkNotNull(testBucketName, "Does not set '%s' environment variable", OSS_TEST_BUCKET_NAME);

    keyPrefix = System.getenv(OSS_TEST_KEY_PREFIX);
    if (keyPrefix == null) {
      keyPrefix = String.format("iceberg-oss-testing-%s", UUID.randomUUID());
    }
  }

  @Override
  public void stop() {

  }

  private OSS client() {
    if (lazyClient == null) {
      synchronized (OSSIntegrationTestRule.class) {
        if (lazyClient == null) {
          lazyClient = createOSSClient();
        }
      }
    }

    return lazyClient;
  }

  @Override
  public String testBucketName() {
    return testBucketName;
  }

  @Override
  public OSS createOSSClient() {
    Preconditions.checkNotNull(endpoint, "OSS endpoint cannot be null");
    Preconditions.checkNotNull(accessKey, "OSS access key cannot be null");
    Preconditions.checkNotNull(accessSecret, "OSS access secret cannot be null");

    return new OSSClientBuilder().build(endpoint, accessKey, accessSecret);
  }

  @Override
  public String keyPrefix() {
    return keyPrefix;
  }

  @Override
  public void setUpBucket(String bucket) {
    Preconditions.checkArgument(client().doesBucketExist(bucket),
        "Bucket %s does not exist, please create it firstly.", bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    ObjectListing objectListing = client().listObjects(
        new ListObjectsRequest(bucket)
            .withPrefix(keyPrefix)
    );

    for (OSSObjectSummary s : objectListing.getObjectSummaries()) {
      client().deleteObject(bucket, s.getKey());
    }
  }
}
