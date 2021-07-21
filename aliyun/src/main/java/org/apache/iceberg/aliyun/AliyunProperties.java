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

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

public class AliyunProperties implements Serializable {

  /**
   * The domain name used to access OSS. OSS uses HTTP Restful APIs to provide services. Different regions are accessed
   * by using different endpoints. For the same region, access over the internal network or over the Internet also uses
   * different endpoints. For more information, see:
   * https://www.alibabacloud.com/help/doc-detail/31837.htm
   */
  public static final String OSS_ENDPOINT = "oss.endpoint";

  /**
   * OSS uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to implement symmetric
   * encryption and verify the identity of a requester. The AccessKey ID is used to identify a user.
   * <p>
   * For more information about how to obtain an AccessKey pair, see:
   * https://www.alibabacloud.com/help/doc-detail/53045.htm
   */
  public static final String OSS_ACCESS_KEY_ID = "oss.access.key.id";

  /**
   * OSS uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to implement symmetric
   * encryption and verify the identity of a requester.  The AccessKey secret is used to encrypt and verify the
   * signature string.
   * <p>
   * For more information about how to obtain an AccessKey pair, see:
   * https://www.alibabacloud.com/help/doc-detail/53045.htm
   */
  public static final String OSS_ACCESS_KEY_SECRET = "oss.access.key.secret";

  /**
   * Number of threads to use for uploading parts to OSS (shared pool across all output streams),
   * default to {@link Runtime#availableProcessors()}
   */
  public static final String OSS_MULTIPART_UPLOAD_THREADS = "oss.multipart.num-threads";

  /**
   * The size of a single part for multipart update requests in bytes (default: 100KB) based on the OSS requirement,
   * the part size must be at least 100KB. To ensure performance of the reader and writer, the part size must be less
   * than 5GB.
   * <p>
   * For more details, please see: https://www.alibabacloud.com/help/doc-detail/31850.htm
   */
  public static final String OSS_MULTIPART_SIZE = "oss.multipart.part-size-bytes";
  public static final long OSS_MULTIPART_SIZE_DEFAULT = 100 * 1024;
  public static final long OSS_MULTIPART_SIZE_MIN = 100 * 1024;
  public static final long OSS_MULTIPART_SIZE_MAX = 5 * 1024 * 1024 * 1024L;

  /**
   * Location to put staging files for uploading to OSS, default to temp directory set in java.io.tmpdir.
   */
  public static final String OSS_STAGING_DIRECTORY = "oss.staging-dir";

  /**
   * The threshold expressed as a object byte size at which to switch from uploading using a single put object request
   * to uploading using multipart.
   */
  public static final String OSS_MULTIPART_THRESHOLD_SIZE = "oss.multipart.threshold.size-bytes";
  public static final long OSS_MULTIPART_THRESHOLD_SIZE_DEFAULT = 5 * 1024 * 1024 * 1024L;

  private final String ossEndpoint;
  private final String ossAccessKeyId;
  private final String ossAccessKeySecret;
  private final int ossMultipartUploadThreads;
  private final long ossMultiPartSize;
  private final String ossStagingDirectory;
  private final long ossMultipartThresholdSize;

  public AliyunProperties() {
    this(ImmutableMap.of());
  }

  public AliyunProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Aliyun properties should not be null");

    // OSS endpoint, accessKeyId, accessKeySecret.
    this.ossEndpoint = properties.get(OSS_ENDPOINT);
    this.ossAccessKeyId = properties.get(OSS_ACCESS_KEY_ID);
    this.ossAccessKeySecret = properties.get(OSS_ACCESS_KEY_SECRET);

    // OSS multipart upload threads.
    this.ossMultipartUploadThreads = PropertyUtil.propertyAsInt(properties, OSS_MULTIPART_UPLOAD_THREADS,
        Runtime.getRuntime().availableProcessors());
    Preconditions.checkArgument(ossMultipartUploadThreads > 0, "%s must be positive.", OSS_MULTIPART_THRESHOLD_SIZE);

    // OOS multiple part size.
    try {
      this.ossMultiPartSize = PropertyUtil.propertyAsLong(properties,
          OSS_MULTIPART_SIZE, OSS_MULTIPART_SIZE_DEFAULT);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Input malformed or exceed maximum multipart upload size 5GB: %s" + properties.get(OSS_MULTIPART_SIZE));
    }
    Preconditions.checkArgument(ossMultiPartSize >= OSS_MULTIPART_SIZE_MIN,
        "Minimum multipart upload object size must be larger than 100KB.");
    Preconditions.checkArgument(ossMultiPartSize <= OSS_MULTIPART_SIZE_MAX,
        "Maximum multipart upload object size must be less than 5GB.");

    // OSS staging directory.
    this.ossStagingDirectory = PropertyUtil.propertyAsString(properties, OSS_STAGING_DIRECTORY,
        System.getProperty("java.io.tmpdir"));

    // OSS threshold to use multipart upload.
    this.ossMultipartThresholdSize = PropertyUtil.propertyAsLong(properties,
        OSS_MULTIPART_THRESHOLD_SIZE, OSS_MULTIPART_THRESHOLD_SIZE_DEFAULT);
    Preconditions.checkArgument(ossMultipartThresholdSize > 0,
        "%s must be positive, the recommend value is 5GB.", OSS_MULTIPART_THRESHOLD_SIZE);
    Preconditions.checkArgument(ossMultipartThresholdSize >= ossMultiPartSize,
        "%s must be not less than %s (value: %s)", OSS_MULTIPART_THRESHOLD_SIZE, OSS_MULTIPART_SIZE, ossMultiPartSize);
  }

  public String ossEndpoint() {
    return ossEndpoint;
  }

  public String ossAccessKeyId() {
    return ossAccessKeyId;
  }

  public String ossAccessKeySecret() {
    return ossAccessKeySecret;
  }

  public int ossMultipartUploadThreads() {
    return ossMultipartUploadThreads;
  }

  public long ossMultiPartSize() {
    return ossMultiPartSize;
  }

  public String ossStagingDirectory() {
    return ossStagingDirectory;
  }

  public long ossMultipartThresholdSize() {
    return ossMultipartThresholdSize;
  }
}
