package org.apache.iceberg.encryption.keys.kms;

import org.apache.iceberg.encryption.keys.Key;

import java.io.Serializable;

public interface MockKms extends Serializable {
  EncryptedAndPlaintextKey createKey(String kmsKekId, int numBytes);

  Key decrypt(String kmsKekId, byte[] encryptedDek);
}
