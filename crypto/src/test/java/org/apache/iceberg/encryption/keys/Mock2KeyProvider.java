package org.apache.iceberg.encryption.keys;

public class Mock2KeyProvider extends org.apache.iceberg.encryption.keys.MockKeyProvider {
  @Override
  public String id() {
    return "mock2";
  }
}
