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

package org.apache.iceberg.nessie;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalQuery;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableReference {
  private static final Logger LOG = LoggerFactory.getLogger(TableReference.class);
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final Splitter REF = Splitter.on("@");
  private static final Splitter TIMESTAMP = Splitter.on("#");

  private static DynMethods.StaticMethod sparkSessionMethod;
  private static DynMethods.UnboundMethod sparkContextMethod;
  private static DynMethods.UnboundMethod sparkConfMethod;
  private static DynMethods.UnboundMethod sparkConfGetMethod;
  private static boolean sparkAvailable = false;
  private static boolean sparkAvailableChecked = false;
  private final TableIdentifier tableIdentifier;
  private final Instant timestamp;
  private final String reference;

  /**
   * Container class to specify a TableIdentifier on a specific Reference or at an Instant in time.
   */
  public TableReference(TableIdentifier tableIdentifier, Instant timestamp, String reference) {
    this.tableIdentifier = tableIdentifier;
    this.timestamp = timestamp;
    this.reference = reference;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public Instant timestamp() {
    return timestamp;
  }

  public String reference() {
    return reference;
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(TableIdentifier path) {
    TableReference pti = parse(path.name());
    return new TableReference(TableIdentifier.of(path.namespace(), pti.tableIdentifier().name()),
        pti.timestamp(),
        pti.reference());
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(String path) {
    // I am assuming tables can't have @ or # symbols
    if (REF.splitToList(path).size() > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one branch in %s", path));
    }
    if (TIMESTAMP.splitToList(path).size() > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one timestamp in %s", path));
    }

    if (path.contains("@") && path.contains("#")) {
      List<String> tableRef = REF.splitToList(path);
      if (tableRef.get(0).contains("#")) {
        throw new IllegalArgumentException("Invalid table name:" +
            " # is not allowed before @. Correct format is table@ref#timestamp");
      }
      TableIdentifier identifier = TableIdentifier.parse(tableRef.get(0));
      List<String> timestampRef = TIMESTAMP.splitToList(tableRef.get(1));
      return new TableReference(identifier, parseTimestamp(timestampRef.get(1)), timestampRef.get(0));
    }

    if (path.contains("@")) {
      List<String> tableRef = REF.splitToList(path);
      TableIdentifier identifier = TableIdentifier.parse(tableRef.get(0));
      return new TableReference(identifier, null, tableRef.get(1));
    }

    if (path.contains("#")) {
      List<String> tableTimestamp = TIMESTAMP.splitToList(path);
      TableIdentifier identifier = TableIdentifier.parse(tableTimestamp.get(0));
      return new TableReference(identifier, parseTimestamp(tableTimestamp.get(1)), null);
    }

    TableIdentifier identifier = TableIdentifier.parse(path);

    return new TableReference(identifier, null, null);
  }

  private enum DateTimeFormatOptions {
    DATE_TIME(DateTimeFormatter.ISO_DATE_TIME, ZonedDateTime::from),
    LOCAL_DATE_TIME(DateTimeFormatter.ISO_LOCAL_DATE_TIME, t -> LocalDateTime.from(t).atZone(sparkTimezoneOrUTC())),
    LOCAL_DATE(DateTimeFormatter.ISO_LOCAL_DATE, t -> LocalDate.from(t).atStartOfDay(sparkTimezoneOrUTC()));

    private final DateTimeFormatter formatter;
    private final TemporalQuery<ZonedDateTime> converter;

    DateTimeFormatOptions(DateTimeFormatter formatter, TemporalQuery<ZonedDateTime> converter) {
      this.formatter = formatter;
      this.converter = converter;
    }

    public ZonedDateTime convert(String timestampStr) {
      try {
        return formatter.parse(timestampStr, converter);
      } catch (DateTimeParseException e) {
        return null;
      }
    }
  }

  private static Instant parseTimestamp(String timestamp) {
    ZonedDateTime parsed;
    for (DateTimeFormatOptions options : DateTimeFormatOptions.values()) {
      parsed = options.convert(modifyTimestamp(timestamp));
      if (parsed != null) {
        return endOfPeriod(parsed, timestamp).toInstant();
      }
    }
    throw new IllegalArgumentException(
        String.format("Cannot parse timestamp: %s is not a legal format. (Use an ISO 8601 compliant string)", timestamp)
    );
  }

  private static String modifyTimestamp(String timestamp) {
    if (timestamp.length() == 7) {
      // only month. add a day
      return String.format("%s-01", timestamp);
    } else if (timestamp.length() == 4) {
      // only year. add a month and day
      return String.format("%s-01-01", timestamp);
    } else {
      return timestamp;
    }
  }

  private static ZonedDateTime endOfPeriod(ZonedDateTime instant, String timestamp) {
    if (timestamp.length() == 10) {
      // only date. Move to end of day
      return endOfPeriod(instant, ChronoUnit.DAYS);
    } else if (timestamp.length() == 7) {
      // only month. Move to end of month
      return endOfPeriod(instant, ChronoUnit.MONTHS);
    } else if (timestamp.length() == 4) {
      // only month. Move to end of month
      return endOfPeriod(instant, ChronoUnit.YEARS);
    } else {
      return instant;
    }
  }

  private static ZonedDateTime endOfPeriod(ZonedDateTime instant, ChronoUnit unit) {
    return instant.plus(1, unit).minus(1, ChronoUnit.MICROS);
  }

  private static ZoneId sparkTimezoneOrUTC() {
    return sparkTimezone().map(ZoneId::of).orElse(UTC);
  }

  private static Optional<String> sparkTimezone() {
    if (!sparkAvailableChecked) {
      sparkAvailableChecked = true;
      try {
        sparkSessionMethod = DynMethods.builder("active")
            .impl("org.apache.spark.sql.SparkSession").buildStatic();
        sparkContextMethod = DynMethods.builder("sparkContext")
            .impl("org.apache.spark.sql.SparkSession").build();
        sparkConfMethod = DynMethods.builder("conf")
            .impl("org.apache.spark.SparkContext").build();
        sparkConfGetMethod = DynMethods.builder("get")
            .impl("org.apache.spark.SparkConf").build();
        sparkAvailable = true;
      } catch (RuntimeException e) {
        sparkAvailable = false; // spark not on classpath
      }
    }
    if (sparkAvailable) {
      try {
        Object sparkContext = sparkContextMethod.bind(sparkSessionMethod.invoke()).invoke();
        Object sparkConf = sparkConfMethod.bind(sparkContext).invoke();
        return Optional.ofNullable(sparkConfGetMethod.bind(sparkConf).invoke("spark.sql.session.timeZone"));
      } catch (RuntimeException e) {
        // we may fail to get Spark timezone, we don't want to crash over that so just log and continue.
        LOG.warn("Cannot find Spark timezone. Using UTC instead.", e);
      }
    }
    return Optional.empty();
  }
}
