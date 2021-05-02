package org.apache.iceberg.encryption;

import org.apache.iceberg.encryption.keys.KeyMetadata;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * {@link org.apache.iceberg.encryption.BasicEncryptionMetadata} is a bridge from an arbitrary byte array into iceberg key
 * metadata.
 *
 * <p>TODO: consider removing this class and using the {@link KeyMetadata} directly.
 */
public class BasicEncryptionMetadata implements EncryptionKeyMetadata {
  private final byte[] metadata;

  public BasicEncryptionMetadata(byte[] metadata) {
    this.metadata = metadata;
  }

  public ByteBuffer buffer() {
    return ByteBuffer.wrap(metadata);
  }

  public EncryptionKeyMetadata copy() {
    return new BasicEncryptionMetadata(Arrays.copyOf(metadata, metadata.length));
  }
}
