package org.apache.iceberg.encryption.keys;

/**
 * {@link KeyMetadata} metadata that is persistable into a byte array within a ManifestFile.
 *
 * <p>individual dekProviders are resposible for serializing and deserializing their
 * dekReterievalMaterials.
 *
 * <p>TODO WARNING: This class isn't ever instantiated but it is helpful for understanding so I'm
 * keeping it around for now. Only {@link org.apache.iceberg.encryption.serde.ReadableKeyMetadata} and {@link
 * org.apache.iceberg.encryption.serde.WritableKeyMetadata} are used.
 */
public class KeyMetadata<R extends KeyMaterials> {
  private final String keyproviderId;
  private final R keyMaterials;

  /**
   * iv is used along with key during {@link org.apache.iceberg.DataFile} encryption. not used to
   * retrieve the key. TODO consider moving iv into {@link EncryptionKeyMetadata} and consider
   * KeyMetadata only about key retrieval.
   */
  private final byte[] iv;

  public KeyMetadata(String keyproviderId, R keyMaterials, byte[] iv) {
    // TODO check not null
    this.keyproviderId = keyproviderId;
    this.keyMaterials = keyMaterials;
    this.iv = iv;
  }
}
