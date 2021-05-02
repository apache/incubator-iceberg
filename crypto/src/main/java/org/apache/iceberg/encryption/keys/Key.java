package org.apache.iceberg.encryption.keys;

/** {@link Key} a plaintext key. */
public class Key {
  private final byte[] plaintext;

  public Key(byte[] plaintext) {
    this.plaintext = plaintext;
  }

  public byte[] plaintext() {
    return plaintext;
  }
}
