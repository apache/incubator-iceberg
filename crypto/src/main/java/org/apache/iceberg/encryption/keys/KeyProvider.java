package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.serde.ReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.WritableKeyMetadata;

import java.io.Serializable;

/**
 * A {@link KeyProvider} is responsible for generating and reterieving keys {@link Key}.
 *
 * <p>It should be able to create a {@link Key} of a specified length along with an accompanying
 * {@link KeyMaterials} which can be used to retrieve the {@link Key}.
 *
 * <p>The {@link KeyMaterials} should be able to be persisted within the {@link KeyMetadata} by
 * using a supported {@link ReadableKeyMetadata} and {@link WritableKeyMetadata}.
 *
 * @param <R>
 */
public abstract class KeyProvider<R extends KeyMaterials> implements Serializable {
  public abstract org.apache.iceberg.encryption.keys.KeyAndKeyMaterials<R> createKey(int keyLength);

  public abstract org.apache.iceberg.encryption.keys.Key decrypt(R keyMaterials);

  public abstract String id();

  public abstract WritableKeyMetadata setKeyMaterials(
      R keyMaterials, WritableKeyMetadata writableKeyMetadata);

  public abstract R extractKeyMaterials(ReadableKeyMetadata readableKeyMetadata);
}
