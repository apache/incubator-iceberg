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

package org.apache.iceberg.data;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.function.Function;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public class InternalRecordWrapper implements StructLike {
  private final Function<Object, Object>[] transforms;
  private StructLike wrapped = null;

  @SuppressWarnings("unchecked")
  public InternalRecordWrapper(Types.StructType struct) {
    this.transforms = struct.fields().stream()
        .map(field -> converter(field.type()))
        .toArray(length -> (Function<Object, Object>[]) Array.newInstance(Function.class, length));
  }

  private static Function<Object, Object> converter(Type type) {
    switch (type.typeId()) {
      case DATE:
        return date -> DateTimeUtil.daysFromDate((LocalDate) date);
      case TIME:
        return time -> DateTimeUtil.microsFromTime((LocalTime) time);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return timestamp -> DateTimeUtil.microsFromTimestamptz((OffsetDateTime) timestamp);
        } else {
          return timestamp -> DateTimeUtil.microsFromTimestamp((LocalDateTime) timestamp);
        }
      case FIXED:
        return bytes -> ByteBuffer.wrap((byte[]) bytes);
      case STRUCT:
        InternalRecordWrapper wrapper = new InternalRecordWrapper(type.asStructType());
        return struct -> wrapper.wrap((StructLike) struct);
      default:
    }
    return null;
  }

  public StructLike get() {
    return wrapped;
  }

  public InternalRecordWrapper wrap(StructLike record) {
    this.wrapped = record;
    return this;
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (transforms[pos] != null) {
      return javaClass.cast(transforms[pos].apply(wrapped.get(pos, Object.class)));
    }
    return wrapped.get(pos, javaClass);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot update InternalRecordWrapper");
  }
}
