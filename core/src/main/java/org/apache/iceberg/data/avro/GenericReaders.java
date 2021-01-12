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

package org.apache.iceberg.data.avro;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.DateTimeUtil;

class GenericReaders {
  private GenericReaders() {
  }

  static ValueReader<LocalDate> dates() {
    return DateReader.INSTANCE;
  }

  static ValueReader<LocalTime> times() {
    return TimeReader.INSTANCE;
  }

  static ValueReader<LocalDateTime> timestamps() {
    return TimestampReader.INSTANCE;
  }

  static ValueReader<OffsetDateTime> timestamptz() {
    return TimestamptzReader.INSTANCE;
  }

  static ValueReader<Record> struct(StructType struct, List<ValueReader<?>> readers, Map<Integer, ?> idToConstant) {
    return new GenericRecordReader(readers, struct, idToConstant);
  }

  static <R extends Record> ValueReader<R> struct(Schema schema,
                                                  Class<R> recordClass,
                                                  List<ValueReader<?>> readers,
                                                  Map<Integer, ?> idToConstant) {
    return new InternalRecordReader(readers, recordClass, schema, idToConstant);
  }

  private static class DateReader implements ValueReader<LocalDate> {
    private static final DateReader INSTANCE = new DateReader();

    private DateReader() {
    }

    @Override
    public LocalDate read(Decoder decoder, Object reuse) throws IOException {
      return DateTimeUtil.dateFromDays(decoder.readInt());
    }
  }

  private static class TimeReader implements ValueReader<LocalTime> {
    private static final TimeReader INSTANCE = new TimeReader();

    private TimeReader() {
    }

    @Override
    public LocalTime read(Decoder decoder, Object reuse) throws IOException {
      return DateTimeUtil.timeFromMicros(decoder.readLong());
    }
  }

  private static class TimestampReader implements ValueReader<LocalDateTime> {
    private static final TimestampReader INSTANCE = new TimestampReader();

    private TimestampReader() {
    }

    @Override
    public LocalDateTime read(Decoder decoder, Object reuse) throws IOException {
      return DateTimeUtil.timestampFromMicros(decoder.readLong());
    }
  }

  private static class TimestamptzReader implements ValueReader<OffsetDateTime> {
    private static final TimestamptzReader INSTANCE = new TimestamptzReader();

    private TimestamptzReader() {
    }

    @Override
    public OffsetDateTime read(Decoder decoder, Object reuse) throws IOException {
      return DateTimeUtil.timestamptzFromMicros(decoder.readLong());
    }
  }

  private static class GenericRecordReader extends ValueReaders.StructReader<Record> {
    private final StructType structType;

    private GenericRecordReader(List<ValueReader<?>> readers, StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.structType = struct;
    }

    @Override
    protected Record reuseOrCreate(Object reuse) {
      if (reuse instanceof Record) {
        return (Record) reuse;
      } else {
        return GenericRecord.create(structType);
      }
    }

    @Override
    protected Object get(Record struct, int pos) {
      return struct.get(pos);
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  private static class InternalRecordReader<R extends Record> extends ValueReaders.StructReader<R> {
    private final Class<R> recordClass;
    private final DynConstructors.Ctor<R> ctor;
    private final Schema schema;

    InternalRecordReader(List<ValueReader<?>> readers, Class<R> recordClass, Schema schema,
                         Map<Integer, ?> idToConstant) {
      super(readers, AvroSchemaUtil.toIceberg(schema).asStruct(), idToConstant);
      this.recordClass = recordClass;
      this.ctor = DynConstructors.builder(Record.class)
          .hiddenImpl(recordClass, Schema.class)
          .hiddenImpl(recordClass)
          .build();
      this.schema = schema;
    }

    @Override
    protected R reuseOrCreate(Object reuse) {
      if (recordClass.isInstance(reuse)) {
        return recordClass.cast(reuse);
      } else {
        return ctor.newInstance(schema);
      }
    }

    @Override
    protected Object get(R struct, int pos) {
      return struct.get(pos);
    }

    @Override
    protected void set(R struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }
}
