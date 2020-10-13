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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBucketing {
  private static final HashFunction MURMUR3 = Hashing.murmur3_32();
  private static Constructor<UUID> uuidBytesConstructor;

  @BeforeClass
  public static void getUUIDConstructor() {
    try {
      uuidBytesConstructor = UUID.class.getDeclaredConstructor(byte[].class);
      uuidBytesConstructor.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private Random testRandom = null;

  @Before
  public void initRandom() {
    // reinitialize random for each test to avoid dependence on run order
    this.testRandom = new Random(314358);
  }

  @Test
  public void testSpecValues() {
    Assert.assertEquals("Spec example: hash(true) = 1392991556",
        1392991556, Bucket.<Integer>get(Types.IntegerType.get(), 100).hash(1));
    Assert.assertEquals("Spec example: hash(34) = 2017239379",
        2017239379, Bucket.<Integer>get(Types.IntegerType.get(), 100).hash(34));
    Assert.assertEquals("Spec example: hash(34L) = 2017239379",
        2017239379, Bucket.<Long>get(Types.LongType.get(), 100).hash(34L));
    Assert.assertEquals("Spec example: hash(17.11F) = -142385009",
        -142385009, new Bucket.BucketFloat(100).hash(1.0F));
    Assert.assertEquals("Spec example: hash(17.11D) = -142385009",
        -142385009, new Bucket.BucketDouble(100).hash(1.0D));
    Assert.assertEquals("Spec example: hash(decimal2(14.20)) = -500754589",
        -500754589,
        Bucket.<BigDecimal>get(Types.DecimalType.of(9, 2), 100).hash(new BigDecimal("14.20")));
    Assert.assertEquals("Spec example: hash(decimal2(14.20)) = -500754589",
        -500754589,
        Bucket.<BigDecimal>get(Types.DecimalType.of(9, 2), 100).hash(new BigDecimal("14.20")));

    Literal<Integer> date = Literal.of("2017-11-16").to(Types.DateType.get());
    Assert.assertEquals("Spec example: hash(2017-11-16) = -653330422",
        -653330422,
        Bucket.<Integer>get(Types.DateType.get(), 100).hash(date.value()));

    Literal<Long> timeValue = Literal.of("22:31:08").to(Types.TimeType.get());
    Assert.assertEquals("Spec example: hash(22:31:08) = -662762989",
        -662762989,
        Bucket.<Long>get(Types.TimeType.get(), 100).hash(timeValue.value()));

    Literal<Long> timestampVal = Literal.of("2017-11-16T22:31:08")
        .to(Types.TimestampType.withoutZone());
    Assert.assertEquals("Spec example: hash(2017-11-16T22:31:08) = -2047944441",
        -2047944441,
        Bucket.<Long>get(Types.TimestampType.withoutZone(), 100).hash(timestampVal.value()));

    Literal<Long> timestamptzVal = Literal.of("2017-11-16T14:31:08-08:00")
        .to(Types.TimestampType.withZone());
    Assert.assertEquals("Spec example: hash(2017-11-16T14:31:08-08:00) = -2047944441",
        -2047944441,
        Bucket.<Long>get(Types.TimestampType.withZone(), 100).hash(timestamptzVal.value()));

    Assert.assertEquals("Spec example: hash(\"iceberg\") = 1210000089",
        1210000089, Bucket.<String>get(Types.StringType.get(), 100).hash("iceberg"));
    Assert.assertEquals("Spec example: hash(\"iceberg\") = 1210000089",
        1210000089, Bucket.<Utf8>get(Types.StringType.get(), 100).hash(new Utf8("iceberg")));

    Literal<UUID> uuid = Literal.of("f79c3e09-677c-4bbd-a479-3f349cb785e7")
        .to(Types.UUIDType.get());
    Assert.assertEquals("Spec example: hash(f79c3e09-677c-4bbd-a479-3f349cb785e7) = 1488055340",
        1488055340, Bucket.<UUID>get(Types.UUIDType.get(), 100).hash(uuid.value()));

    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    Assert.assertEquals("Spec example: hash([00 01 02 03]) = -188683207",
        -188683207, Bucket.<ByteBuffer>get(Types.BinaryType.get(), 100).hash(bytes));
    Assert.assertEquals("Spec example: hash([00 01 02 03]) = -188683207",
        -188683207, Bucket.<ByteBuffer>get(Types.BinaryType.get(), 100).hash(bytes));
  }

  @Test
  public void testInteger() {
    int num = testRandom.nextInt();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong((long) num);

    Bucket<Integer> bucketFunc = Bucket.get(Types.IntegerType.get(), 100);

    Assert.assertEquals("Integer hash should match hash of little-endian bytes",
        hashBytes(buffer.array()), bucketFunc.hash(num));
  }

  @Test
  public void testLong() {
    long num = testRandom.nextLong();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(num);

    Bucket<Long> bucketFunc = Bucket.get(Types.LongType.get(), 100);

    Assert.assertEquals("Long hash should match hash of little-endian bytes",
        hashBytes(buffer.array()), bucketFunc.hash(num));
  }

  @Test
  public void testIntegerTypePromotion() {
    Bucket<Integer> bucketInts = Bucket.get(Types.IntegerType.get(), 100);
    Bucket<Long> bucketLongs = Bucket.get(Types.LongType.get(), 100);

    int randomInt = testRandom.nextInt();

    Assert.assertEquals("Integer and Long bucket results should match",
        bucketInts.apply(randomInt), bucketLongs.apply((long) randomInt));
  }

  @Test
  public void testFloat() {
    float num = testRandom.nextFloat();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putDouble((double) num);

    Bucket<Float> bucketFunc = new Bucket.BucketFloat(100);

    Assert.assertEquals("Float hash should match hash of little-endian bytes",
        hashBytes(buffer.array()), bucketFunc.hash(num));
  }

  @Test
  public void testDouble() {
    double num = testRandom.nextDouble();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putDouble(num);

    Bucket<Double> bucketFunc = new Bucket.BucketDouble(100);

    Assert.assertEquals("Double hash should match hash of little-endian bytes",
        hashBytes(buffer.array()), bucketFunc.hash(num));
  }

  @Test
  public void testFloatTypePromotion() {
    Bucket<Float> bucketFloats = new Bucket.BucketFloat(100);
    Bucket<Double> bucketDoubles = new Bucket.BucketDouble(100);

    float randomFloat = testRandom.nextFloat();

    Assert.assertEquals("Float and Double bucket results should match",
        bucketFloats.apply(randomFloat), bucketDoubles.apply((double) randomFloat));
  }

  @Test
  public void testDecimal() {
    double num = testRandom.nextDouble();
    BigDecimal decimal = BigDecimal.valueOf(num);
    byte[] unscaledBytes = decimal.unscaledValue().toByteArray();

    Bucket<BigDecimal> bucketFunc = Bucket.get(Types.DecimalType.of(9, 2), 100);

    Assert.assertEquals("Decimal hash should match hash of backing bytes",
        hashBytes(unscaledBytes), bucketFunc.hash(decimal));
  }

  @Test
  public void testString() {
    String string = "string to test murmur3 hash";
    byte[] asBytes = string.getBytes(StandardCharsets.UTF_8);

    Bucket<CharSequence> bucketFunc = Bucket.get(Types.StringType.get(), 100);

    Assert.assertEquals("String hash should match hash of UTF-8 bytes",
        hashBytes(asBytes), bucketFunc.hash(string));
  }

  @Test
  public void testUtf8() {
    Utf8 utf8 = new Utf8("string to test murmur3 hash");
    byte[] asBytes = utf8.toString().getBytes(StandardCharsets.UTF_8);

    Bucket<CharSequence> bucketFunc = Bucket.get(Types.StringType.get(), 100);

    Assert.assertEquals("String hash should match hash of UTF-8 bytes",
        hashBytes(asBytes), bucketFunc.hash(utf8));
  }

  @Test
  public void testByteBufferOnHeap() {
    byte[] bytes = randomBytes(128);
    ByteBuffer buffer = ByteBuffer.wrap(bytes, 5, 100);

    Bucket<ByteBuffer> bucketFunc = Bucket.get(Types.BinaryType.get(), 100);

    Assert.assertEquals(
        "HeapByteBuffer hash should match hash for correct slice",
        hashBytes(bytes, 5, 100), bucketFunc.hash(buffer));

    // verify that the buffer was not modified
    Assert.assertEquals("Buffer position should not change", 5, buffer.position());
    Assert.assertEquals("Buffer limit should not change", 105, buffer.limit());
  }

  @Test
  public void testByteBufferOffHeap() {
    byte[] bytes = randomBytes(128);
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);

    // copy to the middle of the off-heap buffer
    buffer.position(5);
    buffer.limit(105);
    buffer.mark();
    buffer.put(bytes, 5, 100);
    buffer.reset();

    Bucket<ByteBuffer> bucketFunc = Bucket.get(Types.BinaryType.get(), 100);

    Assert.assertEquals(
        "DirectByteBuffer hash should match hash for correct slice",
        hashBytes(bytes, 5, 100), bucketFunc.hash(buffer));

    // verify that the buffer was not modified
    Assert.assertEquals("Buffer position should not change", 5, buffer.position());
    Assert.assertEquals("Buffer limit should not change", 105, buffer.limit());
  }

  @Test
  public void testUUIDHash() {
    byte[] uuidBytes = randomBytes(16);
    UUID uuid = newUUID(uuidBytes);

    Bucket<UUID> bucketFunc = Bucket.get(Types.UUIDType.get(), 100);

    Assert.assertEquals("UUID hash should match hash of backing bytes",
        hashBytes(uuidBytes), bucketFunc.hash(uuid));
  }

  @Test
  public void testVerifiedIllegalNumBuckets() {
    AssertHelpers.assertThrows("Should fail if numBucket is less than or equal to zero",
        IllegalArgumentException.class,
        "Invalid number of buckets: 0 (must be > 0)",
        () -> Bucket.get(Types.IntegerType.get(), 0));
  }

  private byte[] randomBytes(int length) {
    byte[] bytes = new byte[length];
    testRandom.nextBytes(bytes);
    return bytes;
  }

  private int hashBytes(byte[] bytes) {
    return hashBytes(bytes, 0, bytes.length);
  }

  private int hashBytes(byte[] bytes, int offset, int length) {
    return MURMUR3.hashBytes(bytes, offset, length).asInt();
  }

  /**
   * This method returns a UUID for the bytes in the array without modification.
   * @param bytes a 16-byte array
   * @return a UUID for the bytes
   */
  private static UUID newUUID(byte[] bytes) {
    try {
      return uuidBytesConstructor.newInstance((Object) bytes);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
