package org.apache.iceberg.encryption.keys.kms;

import org.apache.iceberg.encryption.keys.Key;

/** Container for the response from KMS which generates and encrypts the dek. */
public class EncryptedAndPlaintextKey {
  private final byte[] encryptedKey;
  private final Key plaintextKey;

  public EncryptedAndPlaintextKey(byte[] encryptedKey, Key plaintextKey) {
    this.encryptedKey = encryptedKey;
    this.plaintextKey = plaintextKey;
  }

  public byte[] encryptedKey() {
    return encryptedKey;
  }

  public Key key() {
    return plaintextKey;
  }
}
