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
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestOSSFileIO extends OSSTestBase {
  private static final String OSS_IMPL_CLASS = OSSFileIO.class.getName();

  private final Random random = new Random(1);
  private final Configuration conf = new Configuration();

  @Test
  public void newInputFile() throws IOException {
    String location = location("key.txt");
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = fileIO().newInputFile(location);
    Assert.assertFalse(in.exists());

    OutputFile out = fileIO().newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    Assert.assertTrue(in.exists());

    byte[] actual = new byte[1024 * 1024];
    try (InputStream is = in.newStream()) {
      IOUtils.readFully(is, actual);
    }
    Assert.assertArrayEquals(expected, actual);

    fileIO().deleteFile(in);
    Assert.assertFalse(fileIO().newInputFile(location).exists());
  }

  @Test
  public void testLoadFileIO() {
    FileIO fileIO = CatalogUtil.loadFileIO(OSS_IMPL_CLASS, ImmutableMap.of(), conf);
    Assert.assertTrue("Should be OSSFileIO", fileIO instanceof OSSFileIO);

    byte[] data = SerializationUtil.serializeToBytes(fileIO);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    Assert.assertTrue("The deserialized FileIO should be OSSFileIO", expectedFileIO instanceof OSSFileIO);
  }

  @Test
  public void serializeClient() throws URISyntaxException {
    String endpoint = "iceberg-test-oss.aliyun.com";
    String accessKeyId = UUID.randomUUID().toString();
    String accessSecret = UUID.randomUUID().toString();
    SerializableSupplier<OSS> pre = () -> new OSSClientBuilder().build(endpoint, accessKeyId, accessSecret);

    byte[] data = SerializationUtil.serializeToBytes(pre);
    SerializableSupplier<OSS> post = SerializationUtil.deserializeFromBytes(data);

    OSS client = post.get();
    Assert.assertTrue(client instanceof OSSClient);

    OSSClient ossClient = (OSSClient) client;
    Assert.assertEquals(new URI("http://" + endpoint), ossClient.getEndpoint());
    Assert.assertEquals(accessKeyId, ossClient.getCredentialsProvider().getCredentials().getAccessKeyId());
    Assert.assertEquals(accessSecret, ossClient.getCredentialsProvider().getCredentials().getSecretAccessKey());
  }
}
