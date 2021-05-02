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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class EnvelopeMetadataParser {

  private EnvelopeMetadataParser() {
  }

  private static final String MEK_ID = "mek-id";
  private static final String KEK_ID = "kek-id";
  private static final String ENCRYPTED_KEK = "encrypted-kek";
  private static final String ENCRYPTED_DEK = "encrypted-dek";
  private static final String IV = "iv";
  private static final String ALGORITHM = "algorithm";
  private static final String PROPERTIES = "properties";
  private static final String AAD_TAG = "aad-tag";
  private static final String COLUMN_IDS = "column-ids";
  private static final String COLUMN_METADATA = "column-metadata";

  public static void toJson(EnvelopeMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    JsonUtil.writeStringIfExists(metadata::mekId, MEK_ID, generator);
    JsonUtil.writeStringIfExists(metadata::kekId, KEK_ID, generator);
    JsonUtil.writeBinaryIfExists(metadata::encryptedKek, ENCRYPTED_KEK, generator);
    JsonUtil.writeBinaryIfExists(metadata::encryptedDek, ENCRYPTED_DEK, generator);
    generator.writeStringField(ALGORITHM, metadata.algorithm().name());
    JsonUtil.writeBinaryIfExists(metadata::iv, IV, generator);
    JsonUtil.writeBinaryIfExists(metadata::aadTag, AAD_TAG, generator);

    generator.writeObjectFieldStart(PROPERTIES);
    for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
      generator.writeStringField(keyValue.getKey(), keyValue.getValue());
    }

    generator.writeEndObject();

    if (!metadata.columnIds().isEmpty()) {
      generator.writeArrayFieldStart(COLUMN_IDS);
      for (int columnId : metadata.columnIds()) {
        generator.writeNumber(columnId);
      }

      generator.writeEndArray();
    }

    if (!metadata.columnMetadata().isEmpty()) {
      generator.writeArrayFieldStart(COLUMN_METADATA);
      for (EnvelopeMetadata columnMetadata : metadata.columnMetadata()) {
        toJson(columnMetadata, generator);
      }

      generator.writeEndArray();
    }

    generator.writeEndObject();
  }

  public static ByteBuffer toJson(EnvelopeMetadata metadata) {
    try {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      JsonGenerator generator = JsonUtil.factory().createGenerator(stream);
      toJson(metadata, generator);
      generator.flush();
      return ByteBuffer.wrap(stream.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static EnvelopeMetadata fromJson(JsonNode json) throws IOException {
    Preconditions.checkArgument(json.isObject(), "Cannot parse envelope metadata from non-object: %s", json);
    String mekId = JsonUtil.getStringOrNull(MEK_ID, json);
    String kekId = JsonUtil.getStringOrNull(KEK_ID, json);
    ByteBuffer encryptedKek = JsonUtil.getBytesOrNull(ENCRYPTED_KEK, json);
    ByteBuffer encryptedDek = JsonUtil.getBytesOrNull(ENCRYPTED_DEK, json);
    ByteBuffer iv = JsonUtil.getBytesOrNull(IV, json);
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.valueOf(JsonUtil.getString(ALGORITHM, json));
    ByteBuffer aadTag = JsonUtil.getBytesOrNull(AAD_TAG, json);
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, json);
    Set<Integer> columnIds = JsonUtil.getIntegerSetOrNull(COLUMN_IDS, json);
    Set<EnvelopeMetadata> columnMetadata = JsonUtil.getSetOrNull(COLUMN_METADATA, json,
        EnvelopeMetadataParser::fromJsonUnchecked, null);
    EnvelopeMetadata metadata = new EnvelopeMetadata(mekId, kekId, encryptedKek, encryptedDek, iv, algorithm,
        properties, columnIds, columnMetadata);
    metadata.setAadTag(aadTag);
    return metadata;
  }

  private static EnvelopeMetadata fromJsonUnchecked(JsonNode json) {
    try {
      return fromJson(json);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static EnvelopeMetadata fromJson(ByteBuffer buffer) {
    try {
      return fromJson(JsonUtil.mapper().readValue(buffer.array(), JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
