package org.apache.iceberg.encryption.keys;

public class Mock1KeyProvider extends org.apache.iceberg.encryption.keys.MockKeyProvider {
  @Override
  public String id() {
    return "mock1";
  }
}
