package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.keys.kms.*;
import org.apache.iceberg.encryption.serde.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSingleWrapKmsKeyProvider {
  @Mock MockKms mockKms;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private final int numBytes = 2;
  private final String myKekId = "myKey";
  private final String myOldKekId = "myOldKey";
  private final String expectedKeyMaterials =
      "{\"provider\":\"kms\",\"encodedDek\":\"Y2Q=\",\"iv\":\"\",\"kekId\":\"myOldKey\"}";
  private final byte[] myKey = {'a', 'b'};
  private final byte[] myEncryptedKey = {'c', 'd'};
  private SingleWrapKmsKeyProvider uut;

  @Before
  public void before() {
    uut = new MockSingleWrapKmsKeyProvider(mockKms, myKekId);
  }

  @Test
  public void testCreateKey() {
    when(mockKms.createKey(myKekId, numBytes))
        .thenReturn(new EncryptedAndPlaintextKey(myEncryptedKey, new Key(myKey)));
    KeyAndKeyMaterials<SingleWrapKmsKeyMaterials> keyAndKeyMaterials = uut.createKey(numBytes);

    assertArrayEquals(myKey, keyAndKeyMaterials.key().plaintext());
    assertEquals(myKekId, keyAndKeyMaterials.keyMaterials().kekId());
    assertArrayEquals(myEncryptedKey, keyAndKeyMaterials.keyMaterials().encryptedKey());
    verify(mockKms, times(1)).createKey(myKekId, numBytes);
  }

  @Test
  public void testDecrypt() {
    when(mockKms.decrypt(eq(myOldKekId), aryEq(myEncryptedKey))).thenReturn(new Key(myKey));

    SingleWrapKmsKeyMaterials keyMaterials =
        new SingleWrapKmsKeyMaterials(myOldKekId, myEncryptedKey);
    Key decrypt = uut.decrypt(keyMaterials);
    assertArrayEquals(myKey, decrypt.plaintext());
    verify(mockKms, times(1)).decrypt(eq(myOldKekId), aryEq(myEncryptedKey));
  }

  @Test
  public void testSetKeyMaterials() {
    SingleWrapKmsKeyMaterials keyMaterials =
        new SingleWrapKmsKeyMaterials(myOldKekId, myEncryptedKey);
    WritableKeyMetadata writableKeyMetadata =
        uut.setKeyMaterials(keyMaterials, JsonKeyMetadataSerde.INSTANCE.getWritableKeyMetadata())
            .withKeyProviderId(uut.id())
            .withIv(new byte[0]);
    String myString = new String(writableKeyMetadata.getBytes(), StandardCharsets.UTF_8);
    assertEquals(expectedKeyMaterials, myString);
  }

  @Test
  public void testGetKeyMaterials() {
    JsonReadableKeyMetadata readableKeyMetadata =
        JsonKeyMetadataSerde.INSTANCE.toReadableKeyMetadata(
            ByteBuffer.wrap(expectedKeyMaterials.getBytes(StandardCharsets.UTF_8)));

    SingleWrapKmsKeyMaterials keyMaterialsFromKeyMetadata =
        uut.extractKeyMaterials(readableKeyMetadata);
    assertArrayEquals(myEncryptedKey, keyMaterialsFromKeyMetadata.encryptedKey());
    assertEquals(myOldKekId, keyMaterialsFromKeyMetadata.kekId());
  }

  @Test(expected = UnsupportedReadableKeyMetadataException.class)
  public void testBadReadable() {
    SingleWrapKmsKeyMaterials keyMaterialsFromKeyMetadata =
        uut.extractKeyMaterials(
            new ReadableKeyMetadata() {
              @Override
              public String getKeyProviderId() {
                return null;
              }

              @Override
              public byte[] getIv() {
                return new byte[0];
              }
            });
  }

  @Test(expected = UnsupportedWritableKeyMetadataException.class)
  public void testBadWritable() {
    SingleWrapKmsKeyMaterials keyMaterials =
        new SingleWrapKmsKeyMaterials(myOldKekId, myEncryptedKey);
    uut.setKeyMaterials(
        keyMaterials,
        new WritableKeyMetadata() {
          @Override
          public WritableKeyMetadata withKeyProviderId(String keyProviderId) {
            return null;
          }

          @Override
          public WritableKeyMetadata withIv(byte[] iv) {
            return null;
          }

          @Override
          public byte[] getBytes() {
            return new byte[0];
          }
        });
  }
}
