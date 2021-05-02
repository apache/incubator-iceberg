package org.apache.iceberg.encryption.serde;

import org.apache.iceberg.encryption.keys.KeyMetadata;

/**
 * {@link org.apache.iceberg.encryption.serde.ReadableKeyMetadata} deserializes and provides methods to read values from the {@link
 * KeyMetadata}.
 */
public interface ReadableKeyMetadata {
  String getKeyProviderId();

  byte[] getIv();
}
