package org.apache.iceberg.encryption.serde;

import org.apache.iceberg.encryption.keys.KeyMetadata;

/**
 * A {@link FlatReadableKeyMetadata} is a {@link ReadableKeyMetadata} for a flat {@link KeyMetadata}
 * structure where all values are persisted at the top level.
 */
public interface FlatReadableKeyMetadata extends ReadableKeyMetadata {
  String get(String fieldName);
}
