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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.data.Record;

class GenericWriters {
  private GenericWriters() {
  }

  static ValueWriter<LocalDate> dates(int id) {
    return new DateWriter(id);
  }

  static ValueWriter<LocalTime> times(int id) {
    return new TimeWriter(id);
  }

  static ValueWriter<LocalDateTime> timestamps(int id) {
    return new TimestampWriter(id);
  }

  static ValueWriter<OffsetDateTime> timestamptz(int id) {
    return new TimestamptzWriter(id);
  }

  static ValueWriter<Record> struct(List<ValueWriter<?>> writers) {
    return new GenericRecordWriter(writers);
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateWriter extends ValueWriters.StoredAsIntWriter<LocalDate> {
    private DateWriter(int id) {
      super(id);
    }

    @Override
    protected int convert(LocalDate from) {
      return (int) ChronoUnit.DAYS.between(EPOCH_DAY, from);
    }
  }

  private static class TimeWriter extends ValueWriters.StoredAsLongWriter<LocalTime> {
    private TimeWriter(int id) {
      super(id);
    }

    @Override
    protected long convert(LocalTime from) {
      return from.toNanoOfDay() / 1000;
    }
  }

  private static class TimestampWriter extends ValueWriters.StoredAsLongWriter<LocalDateTime> {
    private TimestampWriter(int id) {
      super(id);
    }

    @Override
    protected long convert(LocalDateTime from) {
      return ChronoUnit.MICROS.between(EPOCH, from.atOffset(ZoneOffset.UTC));
    }
  }

  private static class TimestamptzWriter extends ValueWriters.StoredAsLongWriter<OffsetDateTime> {
    private TimestamptzWriter(int id) {
      super(id);
    }

    @Override
    protected long convert(OffsetDateTime from) {
      return ChronoUnit.MICROS.between(EPOCH, from);
    }
  }

  private static class GenericRecordWriter extends ValueWriters.StructWriter<Record> {
    private GenericRecordWriter(List<ValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int pos) {
      return struct.get(pos);
    }
  }
}
