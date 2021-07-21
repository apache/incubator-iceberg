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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestOSSURI {

  @Test
  public void testLocationParsing() {
    String p1 = "oss://bucket/path/to/file";
    OSSURI uri1 = new OSSURI(p1);

    Assert.assertEquals("bucket", uri1.bucket());
    Assert.assertEquals("path/to/file", uri1.key());
    Assert.assertEquals(p1, uri1.toString());
  }

  @Test
  public void testEncodedString() {
    String p1 = "oss://bucket/path%20to%20file";
    OSSURI uri1 = new OSSURI(p1);

    Assert.assertEquals("bucket", uri1.bucket());
    Assert.assertEquals("path%20to%20file", uri1.key());
    Assert.assertEquals(p1, uri1.toString());
  }

  @Test
  public void missingKey() {
    AssertHelpers.assertThrows("Missing key", ValidationException.class, () -> new OSSURI("https://bucket/"));
  }

  @Test
  public void relativePathing() {
    AssertHelpers.assertThrows("Cannot use relative oss path.", ValidationException.class,
        () -> new OSSURI("/path/to/file"));
  }

  @Test
  public void invalidScheme() {
    AssertHelpers.assertThrows("Invalid schema", ValidationException.class, () -> new OSSURI("invalid://bucket/"));
  }

  @Test
  public void testQueryAndFragment() {
    String p1 = "oss://bucket/path/to/file?query=foo#bar";
    OSSURI uri1 = new OSSURI(p1);

    Assert.assertEquals("bucket", uri1.bucket());
    Assert.assertEquals("path/to/file", uri1.key());
    Assert.assertEquals(p1, uri1.toString());
  }

  @Test
  public void testValidSchemes() {
    for (String scheme : Lists.newArrayList("https", "oss")) {
      OSSURI uri = new OSSURI(scheme + "://bucket/path/to/file");
      Assert.assertEquals("bucket", uri.bucket());
      Assert.assertEquals("path/to/file", uri.key());
    }
  }
}
