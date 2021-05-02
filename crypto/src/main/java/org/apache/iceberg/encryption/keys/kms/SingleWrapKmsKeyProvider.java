package org.apache.iceberg.encryption.keys.kms;

import org.apache.iceberg.encryption.keys.Key;
import org.apache.iceberg.encryption.keys.KeyAndKeyMaterials;
import org.apache.iceberg.encryption.keys.KeyMaterials;
import org.apache.iceberg.encryption.keys.KeyProvider;
import org.apache.iceberg.encryption.serde.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.apache.iceberg.encryption.keys.kms.SingleWrapKmsKeyMaterials.ENCODED_DEK_FIELD;
import static org.apache.iceberg.encryption.keys.kms.SingleWrapKmsKeyMaterials.KEK_ID_FIELD;

/**
 * The {@link SingleWrapKmsKeyProvider} Uses a kekId and an encrypted key (without an iv) as the
 * {@link KeyMaterials}.
 *
 * <p>It should reach out to an external store which will generate and encrypted a dek. The
 * encrypted and plaintext dek should be provided as a response.
 */
public abstract class SingleWrapKmsKeyProvider extends KeyProvider<SingleWrapKmsKeyMaterials> {
  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final Base64.Decoder decoder = Base64.getDecoder();

  private final String kekId; // TODO change to current kmsKeyId with a comment

  public SingleWrapKmsKeyProvider(String kekId) {
    this.kekId = kekId;
  }

  protected abstract EncryptedAndPlaintextKey createEncryptedKey(String kekId, int numBytes);

  public abstract Key decrypt(String kekId, byte[] encryptedDek);

  public Key decrypt(SingleWrapKmsKeyMaterials keyMaterials) {
    return decrypt(keyMaterials.kekId(), keyMaterials.encryptedKey());
  }

  public KeyAndKeyMaterials<SingleWrapKmsKeyMaterials> createKey(int keyLength) {
    EncryptedAndPlaintextKey encryptedAndPlaintextKey = createEncryptedKey(kekId, keyLength);
    SingleWrapKmsKeyMaterials singleWrapKmsKeyMaterials =
        new SingleWrapKmsKeyMaterials(kekId, encryptedAndPlaintextKey.encryptedKey());
    return new org.apache.iceberg.encryption.keys.KeyAndKeyMaterials<>(encryptedAndPlaintextKey.key(), singleWrapKmsKeyMaterials);
  }

  public WritableKeyMetadata setKeyMaterials(
      SingleWrapKmsKeyMaterials keyMaterials, WritableKeyMetadata writableKeyMetadata) {
    if (!(writableKeyMetadata instanceof FlatWritableKeyMetadata)) {
      throw new UnsupportedWritableKeyMetadataException(
          id(), writableKeyMetadata, FlatWritableKeyMetadata.class);
    }
    FlatWritableKeyMetadata flatKeyMetadata = (FlatWritableKeyMetadata) writableKeyMetadata;
    return flatKeyMetadata
        .setField(KEK_ID_FIELD, keyMaterials.kekId())
        .setField(
            ENCODED_DEK_FIELD,
            new String(encoder.encode(keyMaterials.encryptedKey()), StandardCharsets.UTF_8));
  }

  public SingleWrapKmsKeyMaterials extractKeyMaterials(ReadableKeyMetadata readableKeyMetadata) {
    if (!(readableKeyMetadata instanceof FlatReadableKeyMetadata)) {
      throw new UnsupportedReadableKeyMetadataException(
          id(), readableKeyMetadata, FlatReadableKeyMetadata.class);
    }
    FlatReadableKeyMetadata flatKeyMetadata = (FlatReadableKeyMetadata) readableKeyMetadata;
    String kekId = flatKeyMetadata.get(KEK_ID_FIELD); // TODO throw better
    return new SingleWrapKmsKeyMaterials(
        flatKeyMetadata.get(KEK_ID_FIELD),
        decoder.decode(flatKeyMetadata.get(ENCODED_DEK_FIELD).getBytes(StandardCharsets.UTF_8)));
  }
}
