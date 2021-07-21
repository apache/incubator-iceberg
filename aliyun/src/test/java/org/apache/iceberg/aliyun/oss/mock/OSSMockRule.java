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
import com.aliyun.oss.OSSClientBuilder;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.aliyun.oss.OSSTestRule;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSSMockRule implements OSSTestRule {
  private static final Logger LOG = LoggerFactory.getLogger(OSSMockRule.class);

  private final Map<String, Object> properties;

  private OSSMockApplication ossMockApp;

  private OSSMockRule(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public void start() {
    ossMockApp = OSSMockApplication.start(properties);
  }

  @Override
  public void stop() {
    ossMockApp.stop();
  }

  @Override
  public OSS createOSSClient() {
    String endpoint = String.format("http://localhost:%s", properties.getOrDefault(OSSMockApplication.PROP_HTTP_PORT,
        OSSMockApplication.PORT_HTTP_PORT_DEFAULT));
    return new OSSClientBuilder().build(endpoint, "foo", "bar");
  }

  @Override
  public String keyPrefix() {
    return "mock-objects/";
  }

  private File rootDir() {
    Object rootDir = properties.get(OSSMockApplication.PROP_ROOT_DIR);
    Preconditions.checkNotNull(rootDir, "Root directory cannot be null");
    return new File(rootDir.toString());
  }

  @Override
  public void setUpBucket(String bucket) {
    createOSSClient().createBucket(bucket);
  }

  @Override
  public void tearDownBucket(String bucket) {
    try {
      Files.walk(rootDir().toPath())
          .filter(p -> p.toFile().isFile())
          .forEach(p -> {
            try {
              Files.delete(p);
            } catch (IOException e) {
              // delete this files quietly.
            }
          });

      createOSSClient().deleteBucket(bucket);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Map<String, Object> props = Maps.newHashMap();

    public Builder withRootDir(String rootDir) {
      props.put(OSSMockApplication.PROP_ROOT_DIR, rootDir);
      return this;
    }

    public Builder withHttpPort(int httpPort) {
      props.put(OSSMockApplication.PROP_HTTP_PORT, httpPort);
      return this;
    }

    public Builder silent() {
      props.put(OSSMockApplication.PROP_SILENT, true);
      return this;
    }

    public OSSMockRule build() {
      if (props.get(OSSMockApplication.PROP_ROOT_DIR) == null) {
        withRootDir(createRootDir().getAbsolutePath());
      }

      return new OSSMockRule(props);
    }

    private File createRootDir() {
      String rootDir = (String) props.get(OSSMockApplication.PROP_ROOT_DIR);

      File root;
      if (rootDir == null || rootDir.isEmpty()) {
        root = new File(FileUtils.getTempDirectory(), "oss-mock-file-store" + System.currentTimeMillis());
      } else {
        root = new File(rootDir);
      }

      root.deleteOnExit();
      root.mkdir();

      LOG.info("Root directory of local OSS store is {}", root);

      return root;
    }
  }
}
