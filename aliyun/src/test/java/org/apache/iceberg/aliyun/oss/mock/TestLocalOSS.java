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
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aliyun.oss.OSSTestRule;
import org.apache.iceberg.aliyun.oss.OSSURI;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.util.StringUtils;

public class TestLocalOSS {

  @ClassRule
  public static final OSSTestRule OSS_TEST_RULE = OSSMockRule.builder().silent().build();

  private final OSS oss = OSS_TEST_RULE.createOSSClient();
  private final String bucketName = OSS_TEST_RULE.testBucketName();
  private final String keyPrefix = OSS_TEST_RULE.keyPrefix();
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

  @Test
  public void testMultiUpload() throws Exception {
    OSSURI uri = new OSSURI(location("normal-multi-upload-key.dat"));

    // Step.1 Initialize the multi-upload request.
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(uri.bucket(), uri.key());
    InitiateMultipartUploadResult result = oss.initiateMultipartUpload(request);
    Assert.assertEquals(bucketName, result.getBucketName());
    Assert.assertEquals(uri.key(), result.getKey());

    String uploadId = result.getUploadId();
    Assert.assertFalse(StringUtils.isEmpty(uploadId));

    // Step.2 Upload multi parts.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    List<PartETag> completedPartEtags = Lists.newArrayList();
    byte[] data = new byte[100 * 1024];
    for (int i = 1; i <= 10; i++) {
      random.nextBytes(data);
      completedPartEtags.add(uploadPart(uri, uploadId, i, data));
      out.write(data);
    }

    // Step.3 Complete the uploading.
    CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(uri.bucket(), uri.key(),
        uploadId, completedPartEtags);
    CompleteMultipartUploadResult completeResult = oss.completeMultipartUpload(completeRequest);
    Assert.assertEquals(LocalStore.md5sum(wrap(out.toByteArray())), completeResult.getETag());
  }

  @Test
  public void testAbortMultiUpload() throws Exception {
    OSSURI uri = new OSSURI(location("abort-multi-upload-key.dat"));

    // Step.1 Initialize the multi-upload request.
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(uri.bucket(), uri.key());
    InitiateMultipartUploadResult result = oss.initiateMultipartUpload(request);
    Assert.assertEquals(bucketName, result.getBucketName());
    Assert.assertEquals(uri.key(), result.getKey());

    String uploadId = result.getUploadId();
    Assert.assertFalse(StringUtils.isEmpty(uploadId));

    // Step.2 Upload multi parts.
    List<PartETag> completedPartEtags = Lists.newArrayList();
    byte[] data = new byte[100 * 1024];
    for (int i = 1; i <= 10; i++) {
      random.nextBytes(data);
      completedPartEtags.add(uploadPart(uri, uploadId, i, data));
    }

    // Step.3 Abort the uploading.
    AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(uri.bucket(), uri.key(), uploadId);
    oss.abortMultipartUpload(abortRequest);

    // Step.4 Check the existence of the object.
    random.nextBytes(data);
    PutObjectResult putObjectResult = oss.putObject(bucketName, "object", wrap(data));
    Assert.assertEquals(LocalStore.md5sum(wrap(data)), putObjectResult.getETag());
  }

  private PartETag uploadPart(OSSURI uri, String uploadId, int partNumber, byte[] data) {
    UploadPartRequest request = new UploadPartRequest(uri.bucket(), uri.key(), uploadId, partNumber,
        wrap(data), data.length);

    UploadPartResult result = oss.uploadPart(request);
    Assert.assertEquals(result.getPartNumber(), partNumber);
    Assert.assertEquals(result.getPartSize(), data.length);

    return result.getPartETag();
  }

  private String location(String key) {
    return String.format("oss://%s/%s%s", bucketName, keyPrefix, key);
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

  private static void assertThrows(Runnable runnable, String expectedErrorCode) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown, expected errorCode: " + expectedErrorCode);
    } catch (OSSException e) {
      Assert.assertEquals(expectedErrorCode, e.getErrorCode());
    }
  }
}
