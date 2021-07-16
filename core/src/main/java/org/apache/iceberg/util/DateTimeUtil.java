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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class DateTimeUtil {
  public static final long UNIX_EPOCH_JULIAN = 2_440_588L;

  private DateTimeUtil() {
  }

  public static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  public static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();
  public static final long MICROS_PER_MILLIS = 1000L;

  public static LocalDate dateFromDays(int daysFromEpoch) {
    return ChronoUnit.DAYS.addTo(EPOCH_DAY, daysFromEpoch);
  }

  public static int daysFromDate(LocalDate date) {
    return (int) ChronoUnit.DAYS.between(EPOCH_DAY, date);
  }

  public static int daysFromInstant(Instant instant) {
    return (int) ChronoUnit.DAYS.between(EPOCH, instant.atOffset(ZoneOffset.UTC));
  }

  public static LocalTime timeFromMicros(long microFromMidnight) {
    return LocalTime.ofNanoOfDay(microFromMidnight * 1000);
  }

  public static long microsFromTime(LocalTime time) {
    return time.toNanoOfDay() / 1000;
  }

  public static LocalDateTime timestampFromMicros(long microsFromEpoch) {
    return ChronoUnit.MICROS.addTo(EPOCH, microsFromEpoch).toLocalDateTime();
  }

  public static long microsFromInstant(Instant instant) {
    return ChronoUnit.MICROS.between(EPOCH, instant.atOffset(ZoneOffset.UTC));
  }

  public static long microsFromTimestamp(LocalDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime.atOffset(ZoneOffset.UTC));
  }

  public static long microsToMillis(long micros) {
    // When the timestamp is negative, i.e before 1970, we need to adjust the milliseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    return Math.floorDiv(micros, MICROS_PER_MILLIS);
  }

  public static OffsetDateTime timestamptzFromMicros(long microsFromEpoch) {
    return ChronoUnit.MICROS.addTo(EPOCH, microsFromEpoch);
  }

  public static long microsFromTimestamptz(OffsetDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime);
  }

  public static Instant instantFromInt96(ByteBuffer bytes) {
    final ByteBuffer byteBuffer = bytes.order(ByteOrder.LITTLE_ENDIAN);

    final long timeOfDayNanos = byteBuffer.getLong();
    final int julianDay = byteBuffer.getInt();

    return Instant
        .ofEpochMilli(TimeUnit.DAYS.toMillis(julianDay - UNIX_EPOCH_JULIAN))
        .plusNanos(timeOfDayNanos);
  }
}
