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

package org.apache.iceberg.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Serializer;
import org.apache.iceberg.types.Serializers;
import org.apache.iceberg.types.Types;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

public class RocksDBStructLikeMap extends AbstractMap<StructLike, StructLike> implements Map<StructLike, StructLike> {

  static {
    RocksDB.loadLibrary();
  }

  public static RocksDBStructLikeMap create(String path,
                                            Types.StructType keyType,
                                            Types.StructType valType) {
    // Create the RocksDB directory if not exists.
    Path rocksDBDir = Paths.get(path);
    if (!Files.exists(rocksDBDir)) {
      try {
        Files.createDirectory(rocksDBDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (!Files.isDirectory(rocksDBDir)) {
      throw new ValidationException("The existing path %s is not a directory", path);
    }

    return new RocksDBStructLikeMap(path, keyType, valType);
  }

  private final String path;
  private final WriteOptions writeOptions;
  private final RocksDB db;
  private final Types.StructType keyType;
  private final Types.StructType valType;

  private final Serializer<StructLike> keySerializer;
  private final Serializer<StructLike> valSerializer;

  // It's expensive to get the RocksDB's data size, so we maintain the size when put/delete rows.
  private int size = 0;

  private RocksDBStructLikeMap(String path, Types.StructType keyType, Types.StructType valType) {
    this.path = path;
    this.writeOptions = new WriteOptions().setDisableWAL(true);
    try {
      Options options = new Options().setCreateIfMissing(true);
      this.db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    this.keyType = keyType;
    this.valType = valType;
    this.keySerializer = Serializers.forType(keyType);
    this.valSerializer = Serializers.forType(valType);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size <= 0;
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        return db.get(keyData) != null;
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    if (value instanceof StructLike) {
      byte[] valData = valSerializer.serialize((StructLike) value);
      try (RocksIterator iter = db.newIterator()) {
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          if (Arrays.equals(valData, iter.value())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public StructLike get(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        byte[] valData = db.get(keyData);
        if (valData == null) {
          return null;
        }

        return valSerializer.deserialize(valData);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public StructLike put(StructLike key, StructLike value) {
    byte[] keyData = keySerializer.serialize(key);
    byte[] newValue = valSerializer.serialize(value);
    try {
      byte[] oldValue = db.get(keyData);
      db.put(writeOptions, keyData, newValue);

      if (oldValue == null) {
        // Add a new row into the map.
        size += 1;
        return null;
      } else {
        // Replace the old row with the new row.
        return valSerializer.deserialize(oldValue);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StructLike remove(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        byte[] valData = db.get(keyData);
        if (valData != null) {
          db.delete(writeOptions, keyData);

          size -= 1;
          return valSerializer.deserialize(valData);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public void clear() {
    size = 0;
    db.close();
    try {
      FileUtils.cleanDirectory(new File(path));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Set<StructLike> keySet() {
    StructLikeSet keySet = StructLikeSet.create(keyType);

    try (RocksIterator iter = db.newIterator()) {
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        keySet.add(keySerializer.deserialize(iter.key()));
      }
    }

    return keySet;
  }

  @Override
  public Collection<StructLike> values() {
    Set<StructLike> valueSet = Sets.newHashSet();

    try (RocksIterator iter = db.newIterator()) {
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        valueSet.add(valSerializer.deserialize(iter.value()));
      }
    }

    return valueSet;
  }

  @Override
  public Set<Entry<StructLike, StructLike>> entrySet() {
    Set<Entry<StructLike, StructLike>> entrySet = Sets.newHashSet();
    try (RocksIterator iter = db.newIterator()) {
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        StructLikeEntry entry = new StructLikeEntry(
            keySerializer.deserialize(iter.key()),
            valSerializer.deserialize(iter.value()));
        entrySet.add(entry);
      }
      return entrySet;
    }
  }

  private class StructLikeEntry implements Entry<StructLike, StructLike> {

    private final StructLike key;
    private final StructLike value;

    private StructLikeEntry(StructLike key, StructLike value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public StructLike getKey() {
      return key;
    }

    @Override
    public StructLike getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof StructLikeEntry)) {
        return false;
      } else {
        StructLikeEntry that = (StructLikeEntry) o;
        return Objects.equals(
            StructLikeWrapper.forType(keyType).set(key),
            StructLikeWrapper.forType(keyType).set(that.key)) &&
            Objects.equals(
                StructLikeWrapper.forType(valType).set(value),
                StructLikeWrapper.forType(valType).set(that.value)
            );
      }
    }

    @Override
    public int hashCode() {
      int hashCode = 0;
      if (key != null) {
        hashCode ^= StructLikeWrapper.forType(keyType).set(key).hashCode();
      }
      if (value != null) {
        hashCode ^= StructLikeWrapper.forType(valType).set(value).hashCode();
      }
      return hashCode;
    }

    @Override
    public StructLike setValue(StructLike newValue) {
      throw new UnsupportedOperationException("Does not support setValue.");
    }
  }
}
