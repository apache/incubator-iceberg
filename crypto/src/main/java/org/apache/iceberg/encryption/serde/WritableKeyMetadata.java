package org.apache.iceberg.encryption.serde;

import org.apache.iceberg.encryption.BasicEncryptionMetadata;
import org.apache.iceberg.encryption.keys.KeyMetadata;

import java.io.Serializable;

/**
 * {@link org.apache.iceberg.encryption.serde.WritableKeyMetadata} serializes and provides methods to set values in the {@link
 * KeyMetadata}.
 *
 * <p>WARNING: this classes with this interface are not necessarily immutable and should always be
 * treated as mutable and immutable.
 */
public interface WritableKeyMetadata extends Serializable {
  /**
   * Sets the key provider id
   *
   * @param keyProviderId
   * @return
   */
  org.apache.iceberg.encryption.serde.WritableKeyMetadata withKeyProviderId(String keyProviderId);

  org.apache.iceberg.encryption.serde.WritableKeyMetadata withIv(byte[] iv);

  byte[] getBytes();

  default BasicEncryptionMetadata toBasicEncryptionKeyMetadata() {
    return new BasicEncryptionMetadata(getBytes());
  }
}
