package org.apache.iceberg.encryption.serde;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.*;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.IV_FIELD;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.JSON_OBJECT_STRUCTURE;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.KEY_PROVIDER_ID_FIELD;
import static org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde.OBJECT_MAPPER;

/**
 * A {@link org.apache.iceberg.encryption.serde.JsonReadableKeyMetadata} is the reader part of the serde protocol defined by {@link
 * JsonKeyMetadataSerde}.
 *
 * <p>It will read the bytes in the argument into a Map<String, String> and allow read access to it.
 */
public class JsonReadableKeyMetadata implements FlatReadableKeyMetadata {
  private final Map<String, String> keyMaterialJson;
  private static final Base64.Decoder decoder = Base64.getDecoder();

  public JsonReadableKeyMetadata(ByteBuffer bytes) throws IOException {
    try {
      keyMaterialJson =
          OBJECT_MAPPER.readValue(
              new StringReader( // TODO can we have a byte reader from byte buffer instead of making
                  // string?
                  new String(
                      bytes.array(), // TODO ByteBuffers can be read only. maybe copy full array?
                      StandardCharsets.UTF_8)),
              JSON_OBJECT_STRUCTURE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public String get(String fieldName) {
    return keyMaterialJson.get(fieldName);
  }

  @Override
  public String getKeyProviderId() {
    return get(KEY_PROVIDER_ID_FIELD);
  }

  @Override
  public byte[] getIv() {
    return decoder.decode(get(IV_FIELD).getBytes(StandardCharsets.UTF_8));
  }
}
