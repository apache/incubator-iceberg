package org.apache.iceberg.encryption.serde;

import org.apache.iceberg.encryption.keys.KeyMetadata;

/**
 * A {@link FlatWritableKeyMetadata} is a {@link WritableKeyMetadata} for a flat {@link KeyMetadata}
 * structure where all values are persisted at the top level.
 */
public interface FlatWritableKeyMetadata extends WritableKeyMetadata {
  FlatWritableKeyMetadata setField(String field, String value);
}
