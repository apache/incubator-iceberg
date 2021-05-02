package org.apache.iceberg.encryption.keys;

public class UnknownKeyProviderException extends CorruptKeyMetadataException {
  public UnknownKeyProviderException(String keyProviderId) {
    super(String.format("Key provider id is unknown: %s", keyProviderId));
  }
}
