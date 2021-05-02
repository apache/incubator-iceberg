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

package org.apache.iceberg.encryption;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

public class TestEnvelopeMetadataParser {

  @Test
  public void testParseMetadataRoundTrip() {
    EnvelopeMetadata columnMetadata = new EnvelopeMetadata(
        "mekId", "kekId",
        ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)),
        EncryptionAlgorithm.AES_CTR,
        ImmutableMap.of("key", "val"),
        ImmutableSet.of(1, 2),
        ImmutableSet.of());

    EnvelopeMetadata metadata = new EnvelopeMetadata(
        "mekId", "kekId",
        ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)),
        EncryptionAlgorithm.AES_CTR,
        ImmutableMap.of("key", "val"),
        ImmutableSet.of(),
        ImmutableSet.of(columnMetadata));
    ByteBuffer buffer = EnvelopeMetadataParser.toJson(metadata);
    System.out.println(StandardCharsets.UTF_8.decode(buffer).toString());
    EnvelopeMetadata metadata2 = EnvelopeMetadataParser.fromJson(buffer);
    System.out.println(metadata2.toString());
    Assert.assertEquals(metadata, metadata2);
  }

}
