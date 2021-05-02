package org.apache.iceberg.encryption.keys;

import org.apache.iceberg.encryption.serde.ReadableKeyMetadata;
import org.apache.iceberg.encryption.serde.WritableKeyMetadata;

import java.io.Serializable;
import java.util.Map;

/**
 * The {@link org.apache.iceberg.encryption.keys.KeyProviderDelegator} is responsible for managing all of the existing {@link
 * KeyProvider}s.
 *
 * <p>It will keep track of which {@link KeyProvider} generated each {@link org.apache.iceberg.encryption.keys.Key} and corresponding
 * {@link KeyMaterials}.
 *
 * <p>On new key creation it will keep track of which {@link KeyProvider} generated the {@link org.apache.iceberg.encryption.keys.Key}
 * in the {@link KeyMetadata}
 *
 * <p>On decrypt, it will request that the correct {@link KeyProvider} to extract {@link
 * KeyMaterials} from the {@link KeyMetadata}. It will then request that they retrieve the {@link
 * org.apache.iceberg.encryption.keys.Key} from those {@link KeyMaterials}.
 */
public class KeyProviderDelegator implements Serializable {
  private final Map<String, KeyProvider<? extends KeyMaterials>> providers;
  private final KeyProvider<? extends KeyMaterials> providerForNewKeys;

  public KeyProviderDelegator(
      Map<String, KeyProvider<? extends KeyMaterials>> providers,
      KeyProvider<? extends KeyMaterials> providerForNewKeys) {
    this.providers = providers;
    this.providerForNewKeys = providerForNewKeys;
  }

  public org.apache.iceberg.encryption.keys.KeyAndKeyRetrievalInfo<?> createKey(int dekLength) {
    return createKeyHelper(dekLength, providerForNewKeys);
  }

  /** Helper method to make types work. */
  private <M extends KeyMaterials> org.apache.iceberg.encryption.keys.KeyAndKeyRetrievalInfo<M> createKeyHelper(
      int dekLength, KeyProvider<M> provider) {
    return new org.apache.iceberg.encryption.keys.KeyAndKeyRetrievalInfo<M>(provider.createKey(dekLength), provider);
  }

  public org.apache.iceberg.encryption.keys.Key decrypt(ReadableKeyMetadata readableKeyMetadata) {
    String keyProviderId = readableKeyMetadata.getKeyProviderId();
    return decrypt(keyProviderId, readableKeyMetadata);
  }

  public org.apache.iceberg.encryption.keys.Key decrypt(String keyProviderId, ReadableKeyMetadata readableKeyMetadata) {
    KeyProvider<? extends KeyMaterials> keyProvider = providers.get(keyProviderId);
    if (keyProvider == null) {
      throw new UnknownKeyProviderException(keyProviderId);
    }
    return decryptHelper(keyProvider, readableKeyMetadata);
  }

  /** Helper method to make types work. */
  private <M extends KeyMaterials> org.apache.iceberg.encryption.keys.Key decryptHelper(
      KeyProvider<M> provider, ReadableKeyMetadata readableKeyMetadata) {
    M keyMaterials = provider.extractKeyMaterials(readableKeyMetadata);
    return provider.decrypt(keyMaterials);
  }

  public <M extends KeyMaterials> WritableKeyMetadata setKeyRetrievalInfo(
      org.apache.iceberg.encryption.keys.KeyAndKeyRetrievalInfo<M> keyAndKeyRetrievalInfo, WritableKeyMetadata writableKeyMetadata) {
    M keyMaterials = keyAndKeyRetrievalInfo.keyMaterials();
    KeyProvider<M> keyProvider = keyAndKeyRetrievalInfo.keyProvider();

    return keyProvider
        .setKeyMaterials(keyMaterials, writableKeyMetadata)
        .withKeyProviderId(keyProvider.id());
  }
}
