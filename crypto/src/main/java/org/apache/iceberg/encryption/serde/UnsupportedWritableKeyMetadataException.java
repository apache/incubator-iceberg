package org.apache.iceberg.encryption.serde;

public class UnsupportedWritableKeyMetadataException extends RuntimeException {
  public UnsupportedWritableKeyMetadataException(
      String keyProviderId,
      WritableKeyMetadata writableKeyMetadata,
      Class<? extends WritableKeyMetadata> supported) {
    super(
        String.format(
            "KeyProvider %s received %s but supports %s",
            keyProviderId, writableKeyMetadata.getClass().toString(), supported.toString()));
  }
}
