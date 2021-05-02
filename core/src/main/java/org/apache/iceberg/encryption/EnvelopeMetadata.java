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

package org.apache.iceberg.encryption;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;

/**
 * Envelope encryption metadata used to encrypt and decrypt data.
 * Information except plaintext KEK and DEK can be serialized and stored.
 * <p>
 * The metadata itself is defined recursively,
 * as each file level metadata can be associated with some column level metadata.
 * Each file level metadata is expected to contain an array of column level metadata.
 * Each column level metadata is expected to specify an array of source column IDs that shares the metadata.
 */
public class EnvelopeMetadata implements EncryptionKeyMetadata {

  private final String mekId;
  private final String kekId;
  private final byte[] encryptedKek;
  private final byte[] encryptedDek;
  private final byte[] iv;
  private final EncryptionAlgorithm algorithm;
  private final Map<String, String> properties;

  private final int[] columnIds;
  private final EnvelopeMetadata[] columnMetadata;

  // fields that are expected to be updated later
  private byte[] kek;
  private byte[] dek;
  private byte[] aadTag;

  private transient volatile Set<Integer> columnIdSet;
  private transient volatile Set<EnvelopeMetadata> columnMetadataSet;
  private transient volatile Map<Integer, EnvelopeMetadata> columnMetadataMap;

  public EnvelopeMetadata(
      String mekId,
      String kekId,
      ByteBuffer encryptedKek,
      ByteBuffer encryptedDek,
      ByteBuffer iv,
      EncryptionAlgorithm algorithm,
      Map<String, String> properties,
      Set<Integer> columnIdSet,
      Set<EnvelopeMetadata> columnMetadataSet) {
    this.mekId = mekId;
    this.kekId = kekId;
    Preconditions.checkArgument(mekId != null || kekId != null,
        "Cannot construct envelope metadata because either MEK or KEK ID should be specified");
    this.encryptedKek = encryptedKek == null ? null : encryptedKek.array();
    this.encryptedDek = encryptedDek == null ? null : encryptedDek.array();
    this.iv = iv == null ? null : iv.array();
    this.algorithm = Preconditions.checkNotNull(algorithm,
        "Cannot construct envelope metadata because encryption algorithm must not be null");
    this.properties = properties == null ? Maps.newHashMap() : properties;
    this.columnIds = columnIdSet == null || columnIdSet.isEmpty() ? null : Ints.toArray(columnIdSet);
    this.columnMetadata = columnMetadataSet == null || columnMetadataSet.isEmpty() ?
        null : columnMetadataSet.toArray(new EnvelopeMetadata[0]);
  }

  public String mekId() {
    return mekId;
  }

  public String kekId() {
    return kekId;
  }

  public ByteBuffer kek() {
    return kek == null ? null : ByteBuffer.wrap(kek);
  }

  public ByteBuffer encryptedKek() {
    return ByteBuffer.wrap(encryptedKek);
  }

  public ByteBuffer dek() {
    return dek == null ? null : ByteBuffer.wrap(dek);
  }

  public ByteBuffer encryptedDek() {
    return ByteBuffer.wrap(encryptedDek);
  }

  public ByteBuffer iv() {
    return ByteBuffer.wrap(iv);
  }

  public EncryptionAlgorithm algorithm() {
    return algorithm;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public ByteBuffer aadTag() {
    return aadTag == null ? null : ByteBuffer.wrap(aadTag);
  }

  public void setKek(ByteBuffer kekBuffer) {
    this.kek = kekBuffer == null ? null : kekBuffer.array();
  }

  public void setDek(ByteBuffer dekBuffer) {
    this.dek = dekBuffer == null ? null : dekBuffer.array();
  }

  public void setAadTag(ByteBuffer aadTagBuffer) {
    this.aadTag = aadTagBuffer == null ? null : aadTagBuffer.array();
  }

  public Set<Integer> columnIds() {
    return lazyColumnIdSet();
  }

  private Set<Integer> lazyColumnIdSet() {
    if (columnIdSet == null) {
      synchronized (this) {
        if (columnIdSet == null) {
          if (columnIds == null) {
            return ImmutableSet.of();
          }

          columnIdSet = ImmutableSet.copyOf(Ints.asList(columnIds));
        }
      }
    }

    return columnIdSet;
  }

  public Set<EnvelopeMetadata> columnMetadata() {
    return lazyColumnMetadataSet();
  }

  private Set<EnvelopeMetadata> lazyColumnMetadataSet() {
    if (columnMetadataSet == null) {
      synchronized (this) {
        if (columnMetadataSet == null) {
          if (columnMetadata == null) {
            return ImmutableSet.of();
          }

          columnMetadataSet = ImmutableSet.copyOf(columnMetadata);
        }
      }
    }

    return columnMetadataSet;
  }

  public Map<Integer, EnvelopeMetadata> columnMetadataMap() {
    return lazyColumnMetadataMap();
  }

  private Map<Integer, EnvelopeMetadata> lazyColumnMetadataMap() {
    if (columnMetadataMap == null) {
      synchronized (this) {
        if (columnMetadataMap == null) {
          columnMetadataMap = Maps.newHashMap();
          for (EnvelopeMetadata field : columnMetadata) {
            for (int columnId : field.columnIds()) {
              columnMetadataMap.put(columnId, field);
            }
          }
        }
      }
    }

    return columnMetadataMap;
  }

  @Override
  public ByteBuffer buffer() {
    return EnvelopeMetadataParser.toJson(this);
  }

  @Override
  public EncryptionKeyMetadata copy() {
    EnvelopeMetadata metadata = new EnvelopeMetadata(mekId(), kekId(), encryptedKek(), encryptedDek(), iv(),
        algorithm(), properties(), columnIds(), columnMetadata());
    metadata.setDek(dek());
    metadata.setKek(kek());
    metadata.setAadTag(aadTag());
    return metadata;
  }

  @SuppressWarnings("CyclomaticComplexity")
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    EnvelopeMetadata metadata = (EnvelopeMetadata) other;
    return Objects.equals(mekId, metadata.mekId) &&
        Objects.equals(kekId, metadata.kekId) &&
        Arrays.equals(encryptedKek, metadata.encryptedKek) &&
        Arrays.equals(encryptedDek, metadata.encryptedDek) &&
        Arrays.equals(iv, metadata.iv) &&
        algorithm == metadata.algorithm &&
        Objects.equals(properties, metadata.properties) &&
        Arrays.equals(kek, metadata.kek) &&
        Arrays.equals(dek, metadata.dek) &&
        Arrays.equals(aadTag, metadata.aadTag) &&
        Objects.equals(columnIds(), metadata.columnIds()) &&
        Objects.equals(columnMetadata(), metadata.columnMetadata());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(mekId, kekId, algorithm, properties, columnIds(), columnMetadata());
    result = 31 * result + Arrays.hashCode(encryptedKek);
    result = 31 * result + Arrays.hashCode(encryptedDek);
    result = 31 * result + Arrays.hashCode(iv);
    result = 31 * result + Arrays.hashCode(kek);
    result = 31 * result + Arrays.hashCode(dek);
    result = 31 * result + Arrays.hashCode(aadTag);
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("mekId", mekId)
        .add("kekId", kekId)
        .add("encryptedKek", encryptedKek)
        .add("encryptedDek", encryptedDek)
        .add("iv", iv)
        .add("algorithm", algorithm)
        .add("properties", properties)
        .add("columnIds", columnIds())
        .add("columnMetadata", columnMetadata())
        .add("kek", kek == null ? "null" : "(redacted)")
        .add("dek", dek == null ? "null" : "(redacted)")
        .add("aadTag", aadTag)
        .toString();
  }
}
