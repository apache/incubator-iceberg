package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.serde.JsonKeyMetadataSerde;
import org.apache.iceberg.encryption.serde.JsonReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.WritableKeyMetadata;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.iceberg.SerializableUtil.deserialize;
import static org.apache.iceberg.SerializableUtil.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestKeyProviderDelegator {
  private final Mock1KeyProvider mock1DekProvider = new Mock1KeyProvider();
  private final Mock2KeyProvider mock2DekProvider = new Mock2KeyProvider();
  Map<String, KeyProvider<? extends KeyMaterials>> providers = new HashMap<>();

  {
    providers.put(mock1DekProvider.id(), mock1DekProvider);
    providers.put(mock2DekProvider.id(), mock2DekProvider);
  }

  KeyProviderDelegator uut = new KeyProviderDelegator(providers, mock1DekProvider);

  @Test
  public void testCreateKey() {
    KeyAndKeyRetrievalInfo<?> keyAndKeyRetrievalInfo = uut.createKey(10);
    KeyMaterials keyMaterials = keyAndKeyRetrievalInfo.keyMaterials();
    assert (keyMaterials instanceof MockKeyMaterials);
    MockKeyMaterials mockKeyMaterials = (MockKeyMaterials) keyMaterials;
    assertEquals(mock1DekProvider.id(), mockKeyMaterials.mockId());
    assertArrayEquals(keyAndKeyRetrievalInfo.key().plaintext(), mockKeyMaterials.plaintext());
  }

  @Test
  public void testDecrypt() {
    byte[] plaintext = new byte[10];
    new Random().nextBytes(plaintext);
    MockKeyMaterials mockKeyMaterials = new MockKeyMaterials(plaintext, mock2DekProvider.id());
    WritableKeyMetadata writableKeyMetadata =
        mock2DekProvider
            .setKeyMaterials(
                mockKeyMaterials, JsonKeyMetadataSerde.INSTANCE.getWritableKeyMetadata())
            .withKeyProviderId(mock2DekProvider.id())
            .withIv(new byte[0]);

    JsonReadableKeyMetadata readableKeyMetadata =
        JsonKeyMetadataSerde.INSTANCE.toReadableKeyMetadata(
            ByteBuffer.wrap(writableKeyMetadata.getBytes()));

    Key decryptedKey = uut.decrypt(readableKeyMetadata);

    assertArrayEquals(plaintext, decryptedKey.plaintext());
  }

  @Test
  public void testSetKeyRetrievalInfo() {
    byte[] plaintext = new byte[10];
    new Random().nextBytes(plaintext);
    MockKeyMaterials mockKeyMaterials = new MockKeyMaterials(plaintext, mock2DekProvider.id());
    KeyAndKeyRetrievalInfo<MockKeyMaterials> mockKeyMaterialsKeyAndKeyRetrievalInfo =
        new KeyAndKeyRetrievalInfo<>(
            new KeyAndKeyMaterials<>(new Key(plaintext), mockKeyMaterials), mock2DekProvider);

    byte[] iv = new byte[7];
    new Random().nextBytes(iv);

    WritableKeyMetadata writableKeyMetadata =
        uut.setKeyRetrievalInfo(
                mockKeyMaterialsKeyAndKeyRetrievalInfo,
                JsonKeyMetadataSerde.INSTANCE.getWritableKeyMetadata())
            .withIv(iv);

    JsonReadableKeyMetadata readableKeyMetadata =
        JsonKeyMetadataSerde.INSTANCE.toReadableKeyMetadata(
            ByteBuffer.wrap(writableKeyMetadata.getBytes()));

    MockKeyMaterials readMockKeyMaterials =
        mock2DekProvider.extractKeyMaterials(readableKeyMetadata);
    assertEquals(mock2DekProvider.id(), readableKeyMetadata.getKeyProviderId());
    assertEquals(mock2DekProvider.id(), mockKeyMaterials.mockId());
    assertArrayEquals(readableKeyMetadata.getIv(), iv);
    assertArrayEquals(plaintext, mockKeyMaterials.plaintext());
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    byte[] serialized = serialize(uut);
    Object deserialized = deserialize(serialized);
  }
}
