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

package org.apache.iceberg.aliyun.oss.mock;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aliyun.oss.OSSTestRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestLocalOSS {

  @ClassRule
  public static final OSSTestRule OSS_TEST_RULE = OSSMockRule.builder().silent().build();

  private final OSS oss = OSS_TEST_RULE.createOSSClient();
  private final String bucketName = OSS_TEST_RULE.testBucketName();
  private final Random random = new Random(1);

  @Before
  public void before() {
    OSS_TEST_RULE.setUpBucket(bucketName);
  }

  @After
  public void after() {
    OSS_TEST_RULE.tearDownBucket(bucketName);
  }

  @Test
  public void testBuckets() {
    Assert.assertTrue(doesBucketExist(bucketName));
    assertThrows(() -> oss.createBucket(bucketName), OSSErrorCode.BUCKET_ALREADY_EXISTS);

    oss.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    oss.createBucket(bucketName);
    Assert.assertTrue(doesBucketExist(bucketName));
  }

  @Test
  public void testDeleteBucket() {
    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> oss.deleteBucket(bucketNotExist), OSSErrorCode.NO_SUCH_BUCKET);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    oss.putObject(bucketName, "object1", wrap(bytes));

    oss.putObject(bucketName, "object2", wrap(bytes));

    assertThrows(() -> oss.deleteBucket(bucketName), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject(bucketName, "object1");
    assertThrows(() -> oss.deleteBucket(bucketName), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject(bucketName, "object2");
    oss.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    oss.createBucket(bucketName);
  }

  @Test
  public void testPutObject() throws IOException {
    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);

    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> oss.putObject(bucketNotExist, "object", wrap(bytes)), OSSErrorCode.NO_SUCH_BUCKET);

    PutObjectResult result = oss.putObject(bucketName, "object", wrap(bytes));
    Assert.assertEquals(LocalStore.md5sum(wrap(bytes)), result.getETag());
  }

  @Test
  public void testDoesObjectExist() {
    Assert.assertFalse(oss.doesObjectExist(bucketName, "key"));

    Assert.assertFalse(oss.doesObjectExist(bucketName, "key"));

    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);
    oss.putObject(bucketName, "key", wrap(bytes));

    Assert.assertTrue(oss.doesObjectExist(bucketName, "key"));
    oss.deleteObject(bucketName, "key");
  }

  @Test
  public void testGetObject() throws IOException {
    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> oss.getObject(bucketNotExist, "key"), OSSErrorCode.NO_SUCH_BUCKET);

    assertThrows(() -> oss.getObject(bucketName, "key"), OSSErrorCode.NO_SUCH_KEY);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    oss.putObject(bucketName, "key", new ByteArrayInputStream(bytes));

    byte[] actual = new byte[2000];
    IOUtils.readFully(oss.getObject(bucketName, "key").getObjectContent(), actual);

    Assert.assertArrayEquals(bytes, actual);
    oss.deleteObject(bucketName, "key");
  }

  private InputStream wrap(byte[] data) {
    return new ByteArrayInputStream(data);
  }

  private boolean doesBucketExist(String bucket) {
    try {
      oss.createBucket(bucket);
      oss.deleteBucket(bucket);
      return false;
    } catch (OSSException e) {
      if (Objects.equals(e.getErrorCode(), OSSErrorCode.BUCKET_ALREADY_EXISTS)) {
        return true;
      }
      throw e;
    }
  }

  private static void assertThrows(Runnable runnable, String errorCode) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown, expected errorCode: " + errorCode);
    } catch (OSSException e) {
      Assert.assertEquals(e.getErrorCode(), errorCode);
    }
  }
}
