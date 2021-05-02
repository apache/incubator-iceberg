package org.apache.iceberg.encryption.serde;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.*;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.KEY_PROVIDER_ID_FIELD;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.OBJECT_MAPPER;

/** */
public class JsonWritableKeyMetadata implements FlatWritableKeyMetadata {
  private static final Base64.Encoder encoder = Base64.getEncoder();
  private final Map<String, String> tempMap;

  public JsonWritableKeyMetadata() {
    tempMap = new HashMap<>();
  }

  @Override
  public JsonWritableKeyMetadata setField(String field, String value) {
    tempMap.put(field, value);
    return this;
  }

  @Override
  public JsonWritableKeyMetadata withKeyProviderId(String keyProviderId) {
    setField(KEY_PROVIDER_ID_FIELD, keyProviderId);
    return this;
  }

  @Override
  public JsonWritableKeyMetadata withIv(byte[] iv) {
    String base64Iv = new String(encoder.encode(iv), StandardCharsets.UTF_8);
    setField(IV_FIELD, base64Iv);
    return this;
  }

  @Override
  public byte[] getBytes() {
    if (!tempMap.containsKey(KEY_PROVIDER_ID_FIELD)) {
      throw new IllegalStateException("keyProviderId not set");
    }
    if (!tempMap.containsKey(IV_FIELD)) {
      throw new IllegalStateException("iv not set");
    }
    try {
      return OBJECT_MAPPER.writeValueAsBytes(tempMap); // uses UTF-8
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
