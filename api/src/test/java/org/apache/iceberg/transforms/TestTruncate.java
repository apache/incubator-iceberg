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

package org.apache.iceberg.transforms;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestTruncate {
  @Test
  public void testTruncateInteger() {
    Truncate<Integer> trunc = Truncate.get(Types.IntegerType.get(), 10);
    Assert.assertEquals(0, (int) trunc.apply(0));
    Assert.assertEquals(0, (int) trunc.apply(1));
    Assert.assertEquals(0, (int) trunc.apply(5));
    Assert.assertEquals(0, (int) trunc.apply(9));
    Assert.assertEquals(10, (int) trunc.apply(10));
    Assert.assertEquals(10, (int) trunc.apply(11));
    Assert.assertEquals(-10, (int) trunc.apply(-1));
    Assert.assertEquals(-10, (int) trunc.apply(-5));
    Assert.assertEquals(-10, (int) trunc.apply(-10));
    Assert.assertEquals(-20, (int) trunc.apply(-11));
  }

  @Test
  public void testTruncateLong() {
    Truncate<Long> trunc = Truncate.get(Types.LongType.get(), 10);
    Assert.assertEquals(0L, (long) trunc.apply(0L));
    Assert.assertEquals(0L, (long) trunc.apply(1L));
    Assert.assertEquals(0L, (long) trunc.apply(5L));
    Assert.assertEquals(0L, (long) trunc.apply(9L));
    Assert.assertEquals(10L, (long) trunc.apply(10L));
    Assert.assertEquals(10L, (long) trunc.apply(11L));
    Assert.assertEquals(-10L, (long) trunc.apply(-1L));
    Assert.assertEquals(-10L, (long) trunc.apply(-5L));
    Assert.assertEquals(-10L, (long) trunc.apply(-10L));
    Assert.assertEquals(-20L, (long) trunc.apply(-11L));
  }

  @Test
  public void testTruncateDecimal() {
    // decimal truncation works by applying the decimal scale to the width: 10 scale 2 = 0.10
    Truncate<BigDecimal> trunc = Truncate.get(Types.DecimalType.of(9, 2), 10);
    Assert.assertEquals(new BigDecimal("12.30"), trunc.apply(new BigDecimal("12.34")));
    Assert.assertEquals(new BigDecimal("12.30"), trunc.apply(new BigDecimal("12.30")));
    Assert.assertEquals(new BigDecimal("12.20"), trunc.apply(new BigDecimal("12.29")));
    Assert.assertEquals(new BigDecimal("0.00"), trunc.apply(new BigDecimal("0.05")));
    Assert.assertEquals(new BigDecimal("-0.10"), trunc.apply(new BigDecimal("-0.05")));
  }

  @Test
  public void testTruncateString() {
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 5);
    Assert.assertEquals("Should truncate strings longer than length",
        "abcde", trunc.apply("abcdefg"));
    Assert.assertEquals("Should not pad strings shorter than length",
        "abc", trunc.apply("abc"));
    Assert.assertEquals("Should not alter strings equal to length",
        "abcde", trunc.apply("abcde"));
  }

  @Test
  public void testTruncateByteBuffer() throws Exception {
    Truncate<ByteBuffer> trunc = Truncate.get(Types.BinaryType.get(), 4);
    Assert.assertEquals("Should truncate binary longer than length",
        ByteBuffer.wrap("abcd".getBytes("UTF-8")),
        trunc.apply(ByteBuffer.wrap("abcdefg".getBytes("UTF-8"))));
    Assert.assertEquals("Should not pad binary shorter than length",
        ByteBuffer.wrap("abc".getBytes("UTF-8")),
        trunc.apply(ByteBuffer.wrap("abc".getBytes("UTF-8"))));
  }

  @Test
  public void testVerifiedIllegalWidth() {
    AssertHelpers.assertThrows("Should fail if width is less than or equal to zero",
        IllegalArgumentException.class,
        "Invalid truncate width: 0 (must be > 0)",
        () -> Truncate.get(Types.IntegerType.get(), 0));
  }
}
