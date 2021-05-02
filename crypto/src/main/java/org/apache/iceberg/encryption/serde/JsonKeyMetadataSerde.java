package org.apache.iceberg.encryption.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A {@link org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde} supports serialization and deserialization of {@link KeyMetadata}
 * in a flat JSON format.
 *
 * <p>Common Fields are: provider: the keyProviderId for this key metadata. iv: base 64 encoded
 * string of the iv bytes used for DataFile encryption (not key generation).
 *
 * <p>The rest of the fields are {@link org.apache.iceberg.encryption.keys.KeyProvider}
 * specific. These fields are expected to have String values and exist at the top level of the json
 * object.
 *
 * @param <R>
 * @param <W>
 */
public class JsonKeyMetadataSerde
    implements KeyMetadataSerde<JsonReadableKeyMetadata, JsonWritableKeyMetadata> {
  // TODO scope down public static values to just readers and writers
  public static final String KEY_PROVIDER_ID_FIELD = "provider";
  public static final String IV_FIELD = "iv";

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final TypeReference<Map<String, String>> JSON_OBJECT_STRUCTURE =
      new TypeReference<Map<String, String>>() {};

  public static final JsonKeyMetadataSerde INSTANCE = new JsonKeyMetadataSerde();

  private JsonKeyMetadataSerde() {}

  @Override
  public JsonReadableKeyMetadata toReadableKeyMetadata(ByteBuffer bytes) {
    try {
      return new JsonReadableKeyMetadata(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public JsonWritableKeyMetadata getWritableKeyMetadata() {
    return new JsonWritableKeyMetadata();
  }
}
