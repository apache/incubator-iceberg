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

package org.apache.iceberg.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ByteBuffers;

public class Serializers {
  private static final int NULL_MARK = -1;

  private Serializers() {
  }

  public static Serializer<StructLike> forType(Types.StructType struct) {
    return new StructLikeSerializer(struct);
  }

  public static <T> Serializer<List<T>> forType(Types.ListType list) {
    return new ListSerializer<>(list);
  }

  public static <T> Serializer<T> forType(Type.PrimitiveType type) {
    return new PrimitiveSerializer<>(type);
  }

  @SuppressWarnings("unchecked")
  private static <T> Serializer<T> internal(Type type) {
    if (type.isPrimitiveType()) {
      return forType(type.asPrimitiveType());
    } else if (type.isStructType()) {
      return (Serializer<T>) forType(type.asStructType());
    } else if (type.isListType()) {
      return (Serializer<T>) forType(type.asListType());
    }
    throw new UnsupportedOperationException("Cannot determine serializer for type: " + type);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> internalClass(Type type) {
    if (type.isPrimitiveType()) {
      return (Class<T>) type.typeId().javaClass();
    } else if (type.isStructType()) {
      return (Class<T>) StructLike.class;
    } else if (type.isListType()) {
      return (Class<T>) List.class;
    } else if (type.isMapType()) {
      return (Class<T>) Map.class;
    }

    throw new UnsupportedOperationException("Cannot determine expected class for type: " + type);
  }

  private static class StructLikeSerializer implements Serializer<StructLike> {
    private final Types.StructType struct;
    private final Serializer<Object>[] serializers;
    private final Class<?>[] classes;

    private StructLikeSerializer(Types.StructType struct) {
      this.struct = struct;
      this.serializers = struct.fields().stream()
          .map(field -> internal(field.type()))
          .toArray((IntFunction<Serializer<Object>[]>) Serializer[]::new);
      this.classes = struct.fields().stream()
          .map(field -> internalClass(field.type()))
          .toArray(Class<?>[]::new);
    }

    @Override
    public byte[] serialize(StructLike object) {
      if (object == null) {
        return null;
      }

      try (ByteArrayOutputStream out = new ByteArrayOutputStream();
           DataOutputStream dos = new DataOutputStream(out)) {

        for (int i = 0; i < serializers.length; i += 1) {
          Class<?> valueClass = classes[i];

          byte[] fieldData = serializers[i].serialize(object.get(i, valueClass));
          if (fieldData == null) {
            dos.writeInt(NULL_MARK);
          } else {
            dos.writeInt(fieldData.length);
            dos.write(fieldData);
          }
        }
        return out.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public StructLike deserialize(byte[] data) {
      if (data == null) {
        return null;
      }

      try (ByteArrayInputStream in = new ByteArrayInputStream(data);
           DataInputStream dis = new DataInputStream(in)) {

        GenericRecord record = GenericRecord.create(struct);
        for (int i = 0; i < serializers.length; i += 1) {
          int length = dis.readInt();

          if (length == NULL_MARK) {
            record.set(i, null);
          } else {
            byte[] fieldData = new byte[length];
            int fieldDataSize = dis.read(fieldData);
            Preconditions.checkState(length == fieldDataSize, "%s != %s", length, fieldDataSize);
            record.set(i, serializers[i].deserialize(fieldData));
          }
        }

        return record;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static class ListSerializer<T> implements Serializer<List<T>> {
    private final Serializer<T> elementSerializer;

    private ListSerializer(Types.ListType list) {
      this.elementSerializer = internal(list.elementType());
    }

    @Override
    public byte[] serialize(List<T> object) {
      if (object == null) {
        return null;
      }

      try (ByteArrayOutputStream out = new ByteArrayOutputStream();
           DataOutputStream dos = new DataOutputStream(out)) {

        dos.writeInt(object.size());
        for (T elem : object) {
          byte[] data = elementSerializer.serialize(elem);

          if (data == null) {
            dos.writeInt(NULL_MARK);
          } else {
            dos.writeInt(data.length);
            dos.write(data);
          }
        }

        return out.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<T> deserialize(byte[] data) {
      if (data == null) {
        return null;
      }

      try (ByteArrayInputStream in = new ByteArrayInputStream(data);
           DataInputStream dis = new DataInputStream(in)) {

        int size = dis.readInt();
        List<T> result = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
          int length = dis.readInt();

          if (length == NULL_MARK) {
            result.add(null);
          } else {
            byte[] fieldData = new byte[length];
            int fieldDataSize = dis.read(fieldData);
            Preconditions.checkState(length == fieldDataSize, "%s != %s", length, fieldDataSize);
            result.add(elementSerializer.deserialize(fieldData));
          }
        }

        return result;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class PrimitiveSerializer<T> implements Serializer<T> {

    private final Type.PrimitiveType type;

    private PrimitiveSerializer(Type.PrimitiveType type) {
      this.type = type;
    }

    @Override
    public byte[] serialize(Object object) {
      return ByteBuffers.toByteArray(Conversions.toByteBuffer(type, object));
    }

    @Override
    public T deserialize(byte[] data) {
      return Conversions.fromByteBuffer(type, data == null ? null : ByteBuffer.wrap(data));
    }
  }
}
