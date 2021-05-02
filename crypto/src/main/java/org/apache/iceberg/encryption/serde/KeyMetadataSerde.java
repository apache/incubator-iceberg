package org.apache.iceberg.encryption.serde;

import org.apache.iceberg.encryption.keys.KeyMetadata;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A {@link org.apache.iceberg.encryption.serde.KeyMetadataSerde} provides a compatible {@link WritableKeyMetadata} and {@link
 * ReadableKeyMetadata} to support serialization and deserialization of {@link KeyMetadata}
 *
 * <p>TODO think about combining the writable key metadata and readable key metadata into a single
 * key metadata which can be read and written. Kinda bad that the immutable key metadata can be
 * changed though.
 *
 * @param <R>
 * @param <W>
 */
public interface KeyMetadataSerde<R extends ReadableKeyMetadata, W extends WritableKeyMetadata>
    extends Serializable {
  R toReadableKeyMetadata(ByteBuffer bytes);

  W getWritableKeyMetadata();
}
