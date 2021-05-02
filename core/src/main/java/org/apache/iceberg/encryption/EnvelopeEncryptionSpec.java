/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.encryption;

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * The envelope encryption spec defines the envelope encryption configurations
 * to follow by a writer when writing data to Iceberg.
 * <p>
 * It allows encrypting different parts of the Iceberg metadata tree,
 * data files and table columns using different encryption keys.
 * The spec is not versioned, because all the encryption details
 * are written one level above and serialized as {@link EnvelopeMetadata}.
 * For example, information needed to decrypt a data file
 * are all written in the manifest's corresponding DataFile entry.
 */
public class EnvelopeEncryptionSpec implements Serializable {

  private static final EnvelopeEncryptionSpec NOT_ENCRYPTED = new EnvelopeEncryptionSpec(
      new Schema(), null, null, null);

  private final Schema schema;
  private final EnvelopeConfig manifestListConfig;
  private final EnvelopeConfig manifestFileConfig;
  private final EnvelopeConfig dataFileConfig;

  public EnvelopeEncryptionSpec(
      Schema schema,
      EnvelopeConfig manifestListConfig,
      EnvelopeConfig manifestFileConfig,
      EnvelopeConfig dataFileConfig) {
    this.schema = schema;
    this.manifestListConfig = manifestListConfig;
    this.manifestFileConfig = manifestFileConfig;
    this.dataFileConfig = dataFileConfig;
  }

  public Schema schema() {
    return schema;
  }

  public EnvelopeConfig manifestListConfig() {
    return manifestListConfig;
  }

  public EnvelopeConfig manifestFileConfig() {
    return manifestFileConfig;
  }

  public EnvelopeConfig dataFileConfig() {
    return dataFileConfig;
  }

  public static EnvelopeEncryptionSpec notEncrypted() {
    return NOT_ENCRYPTED;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("schema", schema)
        .add("manifestListConfig", manifestListConfig)
        .add("manifestFileConfig", manifestFileConfig)
        .add("dataFileConfig", dataFileConfig)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EnvelopeEncryptionSpec that = (EnvelopeEncryptionSpec) o;
    return Objects.equals(schema, that.schema) &&
        Objects.equals(manifestListConfig, that.manifestListConfig) &&
        Objects.equals(manifestFileConfig, that.manifestFileConfig) &&
        Objects.equals(dataFileConfig, that.dataFileConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, manifestListConfig, manifestFileConfig, dataFileConfig);
  }

  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * A builder used to create valid {@link EnvelopeEncryptionSpec}.
   * <p>
   * Call {@link #builderFor(Schema)} to create a new builder.
   */
  public static class Builder {

    private final Schema schema;
    private EnvelopeConfig manifestListConfig;
    private EnvelopeConfig manifestFileConfig;
    private EnvelopeConfig dataFileConfig;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder addManifestListConfig(EnvelopeConfig config) {
      this.manifestListConfig = config;
      return this;
    }

    public Builder addManifestFileConfig(EnvelopeConfig config) {
      this.manifestFileConfig = config;
      return this;
    }

    public Builder addDataFileConfig(EnvelopeConfig config) {
      this.dataFileConfig = config;
      return this;
    }

    public EnvelopeEncryptionSpec build() {
      return new EnvelopeEncryptionSpec(schema, manifestListConfig, manifestFileConfig, dataFileConfig);
    }
  }
}
