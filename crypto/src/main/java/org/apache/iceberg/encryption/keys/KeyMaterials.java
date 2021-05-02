package org.apache.iceberg.encryption.keys;

import java.io.Serializable;

/**
 * {@link KeyProvider} specific config in order to retrieve a dek from a provider.
 *
 * <p>This information is probably nested within the {@link KeyMetadata}
 */
public interface KeyMaterials extends Serializable {}
