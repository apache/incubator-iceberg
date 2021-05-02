package org.apache.iceberg.encryption.keys;

/** A plaintext key and secure materials to retrieve/regenerate the key. */
public class KeyAndKeyMaterials<R extends KeyMaterials> {
  private final Key key;
  private final R materials;

  public KeyAndKeyMaterials(Key key, R materials) {
    this.key = key;
    this.materials = materials;
  }

  public Key key() {
    return key;
  }

  public R keyMaterials() {
    return materials;
  }
}
