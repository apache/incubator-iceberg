package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.SymmetricKeyEncryptionManager;


/**
 * A {@link org.apache.iceberg.encryption.keys.KeyAndKeyRetrievalInfo} is the information passed from the {@link
 * org.apache.iceberg.encryption.keys.KeyProviderDelegator} back to the {@link
 * SymmetricKeyEncryptionManager} when generating a new key.
 *
 * <p>It should contain the new {@link Key} for the {@link SymmetricKeyEncryptionManager} to use
 * during encryption.
 *
 * <p>It should also contain enough information to be able to persist information into the {@link
 * KeyMetadata} pertaining to key retrieval. This is the keyProviderId and {@link KeyMaterials}.
 *
 * <p>The {@link KeyProvider} used to generate the {@link Key} is passed around within this object
 * because each key provider is responsible for persisting their own {@link KeyMaterials}
 *
 * @param <R>
 */
public class KeyAndKeyRetrievalInfo<R extends KeyMaterials> {
  private final Key key;
  private final R keyMaterials;
  private final KeyProvider<R> keyProvider;

  public KeyAndKeyRetrievalInfo(
      KeyAndKeyMaterials<R> keyAndKeyMaterials, KeyProvider<R> keyProvider) {
    this(keyAndKeyMaterials.key(), keyAndKeyMaterials.keyMaterials(), keyProvider);
  }

  public KeyAndKeyRetrievalInfo(Key key, R keyMaterials, KeyProvider<R> keyProvider) {
    this.key = key;
    this.keyMaterials = keyMaterials;
    this.keyProvider = keyProvider;
  }

  public Key key() {
    return key;
  }

  public R keyMaterials() {
    return keyMaterials;
  }

  public KeyProvider<R> keyProvider() {
    return keyProvider;
  }
}
