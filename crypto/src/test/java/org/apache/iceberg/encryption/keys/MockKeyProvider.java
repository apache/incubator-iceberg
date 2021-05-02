package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.serde.FlatReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.FlatWritableKeyMetadata;
import org.apache.iceberg.encryption.serde.ReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.WritableKeyMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public abstract class MockKeyProvider extends KeyProvider<MockKeyMaterials> {

  @Override
  public KeyAndKeyMaterials<MockKeyMaterials> createKey(int keyLength) {
    byte[] key = new byte[keyLength];
    new Random().nextBytes(key);

    return new KeyAndKeyMaterials<>(new Key(key), new MockKeyMaterials(key, id()));
  }

  @Override
  public Key decrypt(MockKeyMaterials keyMaterials) {
    if (!keyMaterials.mockId().equals(id())) {
      throw new IllegalArgumentException("Incorrect mock key materials.");
    }
    return new Key(keyMaterials.plaintext());
  }

  @Override
  public WritableKeyMetadata setKeyMaterials(
      MockKeyMaterials keyMaterials, WritableKeyMetadata writableKeyMetadata) {
    if (!keyMaterials.mockId().equals(id())) {
      throw new IllegalArgumentException("Incorrect mock key materials.");
    }
    FlatWritableKeyMetadata keyMetadata = (FlatWritableKeyMetadata) writableKeyMetadata;
    return keyMetadata
        .setField(
            "key",
            new String(
                Base64.getEncoder().encode(keyMaterials.plaintext()), StandardCharsets.UTF_8))
        .setField("mockVersion", id());
  }

  @Override
  public MockKeyMaterials extractKeyMaterials(ReadableKeyMetadata readableKeyMetadata) {
    FlatReadableKeyMetadata keyMetadata = (FlatReadableKeyMetadata) readableKeyMetadata;
    MockKeyMaterials keyMaterials =
        new MockKeyMaterials(
            Base64.getDecoder().decode(keyMetadata.get("key")), keyMetadata.get("mockVersion"));
    if (!keyMaterials.mockId().equals(id())) {
      throw new IllegalArgumentException("Incorrect mock key materials.");
    }
    return keyMaterials;
  }
}
