package org.apache.iceberg.encryption.keys.kms;

import org.apache.iceberg.encryption.keys.Key;

public class MockSingleWrapKmsKeyProvider extends SingleWrapKmsKeyProvider {
  private final MockKms kms;

  public MockSingleWrapKmsKeyProvider(MockKms kms, String kmsKeyId) {
    super(kmsKeyId);
    this.kms = kms;
  }

  @Override
  protected EncryptedAndPlaintextKey createEncryptedKey(String kekId, int numBytes) {
    return kms.createKey(kekId, numBytes);
  }

  @Override
  public Key decrypt(String kekId, byte[] encryptedDek) {
    return kms.decrypt(kekId, encryptedDek);
  }

  @Override
  public String id() {
    return "kms";
  }
}
