package org.apache.iceberg.encryption.keys.kms;

import org.apache.iceberg.encryption.keys.KeyMaterials;

/**
 * The materials used during a single wrapping of the key materials.
 *
 * <p>See PARQUET-1373 for the difference between a single wrap and double wrap.
 *
 * <p>In PARQUET-1373, the kekId in this class is equivalent to the MEK.
 */
public class SingleWrapKmsKeyMaterials implements KeyMaterials {
  public static final String KEK_ID_FIELD = "kekId";
  public static final String ENCODED_DEK_FIELD = "encodedDek";

  private final String kekId;
  private final byte[] encryptedKey;

  public SingleWrapKmsKeyMaterials(String kekId, byte[] encryptedKey) {
    this.kekId = kekId;
    this.encryptedKey = encryptedKey;
  }

  public String kekId() {
    return kekId;
  }

  public byte[] encryptedKey() {
    return encryptedKey;
  }
}
