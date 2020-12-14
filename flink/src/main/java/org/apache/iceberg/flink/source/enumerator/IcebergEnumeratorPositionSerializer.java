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

package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

class IcebergEnumeratorPositionSerializer implements SimpleVersionedSerializer<IcebergEnumeratorPosition> {

  public static final IcebergEnumeratorPositionSerializer INSTANCE = new IcebergEnumeratorPositionSerializer();

  private static final int VERSION = 1;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(128));

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergEnumeratorPosition position) throws IOException {
    return serializeV1(position);
  }

  @Override
  public IcebergEnumeratorPosition deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return deserializeV1(serialized);
      default:
        throw new IOException("Unknown version: " + version);
    }
  }

  private byte[] serializeV1(IcebergEnumeratorPosition position) throws IOException {
    final DataOutputSerializer out = SERIALIZER_CACHE.get();
    out.writeBoolean(position.startSnapshotId() != null);
    if (position.startSnapshotId() != null) {
      out.writeLong(position.startSnapshotId());
    }
    out.writeBoolean(position.startSnapshotTimestampMs() != null);
    if (position.startSnapshotTimestampMs() != null) {
      out.writeLong(position.startSnapshotTimestampMs());
    }
    out.writeLong(position.endSnapshotId());
    out.writeLong(position.endSnapshotTimestampMs());
    final byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  private IcebergEnumeratorPosition deserializeV1(byte[] serialized) throws IOException {
    final DataInputDeserializer in = new DataInputDeserializer(serialized);
    final IcebergEnumeratorPosition.Builder builder = IcebergEnumeratorPosition.builder();
    if (in.readBoolean()) {
      builder.startSnapshotId(in.readLong());
    }
    if (in.readBoolean()) {
      builder.startSnapshotTimestampMs(in.readLong());
    }
    builder.endSnapshotId(in.readLong());
    builder.endSnapshotTimestampMs(in.readLong());
    return builder.build();
  }
}
