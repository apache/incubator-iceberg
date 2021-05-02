package org.apache.iceberg.encryption.keys;

public class MockKeyMaterials implements KeyMaterials {
  private final byte[] plaintext;
  private final String mockId;

  public MockKeyMaterials(byte[] plaintext, String mockId) {
    this.plaintext = plaintext;
    this.mockId = mockId;
  }

  public byte[] plaintext() {
    return plaintext;
  }

  public String mockId() {
    return mockId;
  }
}
