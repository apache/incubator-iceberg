package org.apache.iceberg.encryption.serde;

public class UnsupportedReadableKeyMetadataException extends RuntimeException {
  public UnsupportedReadableKeyMetadataException(
      String keyProviderId,
      ReadableKeyMetadata readableKeyMetadata,
      Class<? extends ReadableKeyMetadata> supported) {
    super(
        String.format(
            "KeyProvider %s received %s but supports %s",
            keyProviderId, readableKeyMetadata.getClass().toString(), supported.toString()));
  }
}
