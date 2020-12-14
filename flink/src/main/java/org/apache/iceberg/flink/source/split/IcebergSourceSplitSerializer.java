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

package org.apache.iceberg.flink.source.split;

import java.io.IOException;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

/**
 * TODO: use Java serialization for now.
 * will switch to more stable serializer from issue-1698.
 */
public class IcebergSourceSplitSerializer implements SimpleVersionedSerializer<IcebergSourceSplit> {

  public static final IcebergSourceSplitSerializer INSTANCE = new IcebergSourceSplitSerializer();

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergSourceSplit split) throws IOException {
    if (split.serializedFormCache() == null) {
      final byte[] result = serializeV1(split);
      split.serializedFormCache(result);
    }
    return split.serializedFormCache();
  }

  @Override
  public IcebergSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return deserializeV1(serialized);
      default:
        throw new IOException("Unknown version: " + version);
    }
  }

  @VisibleForTesting
  byte[] serializeV1(IcebergSourceSplit split) throws IOException {
    return InstantiationUtil.serializeObject(split);
  }

  @VisibleForTesting
  IcebergSourceSplit deserializeV1(byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }
}
