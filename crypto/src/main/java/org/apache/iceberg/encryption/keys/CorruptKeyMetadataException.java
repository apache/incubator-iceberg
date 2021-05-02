package org.apache.iceberg.encryption.keys;

public class CorruptKeyMetadataException extends RuntimeException {
  public CorruptKeyMetadataException(String message) {
    super(message);
  }
}
