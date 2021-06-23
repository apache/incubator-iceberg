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

package org.apache.iceberg.avro;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;

public class AvroMetrics {

  private AvroMetrics() {
  }

  static Metrics fromWriter(DatumWriter<?> datumWriter, Schema schema, long numRecords,
                            MetricsConfig inputMetricsConfig) {
    if (!(datumWriter instanceof MetricsAwareDatumWriter)) {
      return new Metrics(numRecords, null, null, null, null);
    }

    MetricsAwareDatumWriter<?> metricsAwareDatumWriter = (MetricsAwareDatumWriter<?>) datumWriter;
    MetricsConfig metricsConfig;
    if (inputMetricsConfig == null) {
      metricsConfig = MetricsConfig.getDefault();
    } else {
      metricsConfig = inputMetricsConfig;
    }

    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    Map<Integer, Long> nanValueCounts = new HashMap<>();
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

    metricsAwareDatumWriter.metrics().forEach(metrics -> {
      String columnName = schema.findColumnName(metrics.id());
      MetricsModes.MetricsMode metricsMode = metricsConfig.columnMode(columnName);
      if (metricsMode == MetricsModes.None.get()) {
        return;
      }

      valueCounts.put(metrics.id(), metrics.valueCount());
      nullValueCounts.put(metrics.id(), metrics.nullValueCount());
      Type type = schema.findType(metrics.id());

      if (type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE) {
        nanValueCounts.put(metrics.id(), metrics.nanValueCount());
      }

      if (metricsMode == MetricsModes.Counts.get()) {
        return;
      }

      updateLowerBound(metrics, type, metricsMode).ifPresent(lowerBound -> lowerBounds.put(metrics.id(), lowerBound));
      updateUpperBound(metrics, type, metricsMode).ifPresent(upperBound -> upperBounds.put(metrics.id(), upperBound));
    });

    return new Metrics(numRecords, null,
        valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
  }

  private static Optional<ByteBuffer> updateLowerBound(FieldMetrics metrics, Type type,
                                                       MetricsModes.MetricsMode metricsMode) {
    if (metrics.lowerBound() == null) {
      return Optional.empty();
    }

    Object lowerBound = metrics.lowerBound();
    if (metricsMode instanceof MetricsModes.Truncate) {
      MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
      int truncateLength = truncateMode.length();
      switch (type.typeId()) {
        case STRING:
          lowerBound = UnicodeUtil.truncateStringMin(
              Literal.of((CharSequence) metrics.lowerBound()), truncateLength).value();
          break;
        case FIXED:
        case BINARY:
          lowerBound = BinaryUtil.truncateBinaryMin(
              Literal.of((ByteBuffer) metrics.lowerBound()), truncateLength).value();
          break;
        default:
          break;
      }
    }

    return Optional.ofNullable(Conversions.toByteBuffer(type, lowerBound));
  }

  private static Optional<ByteBuffer> updateUpperBound(FieldMetrics metrics, Type type,
                                                 MetricsModes.MetricsMode metricsMode) {
    if (metrics.upperBound() == null) {
      return Optional.empty();
    }

    Object upperBound = null;
    if (metricsMode instanceof MetricsModes.Truncate) {
      MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
      int truncateLength = truncateMode.length();
      switch (type.typeId()) {
        case STRING:
          upperBound = Optional.ofNullable(
              UnicodeUtil.truncateStringMax(Literal.of((CharSequence) metrics.upperBound()), truncateLength))
              .map(Literal::value)
              .orElse(null);
          break;
        case FIXED:
        case BINARY:
          upperBound = Optional.ofNullable(
              BinaryUtil.truncateBinaryMax(Literal.of((ByteBuffer) metrics.upperBound()), truncateLength))
              .map(Literal::value)
              .orElse(null);
          break;
        default:
          break;
      }
    }

    if (upperBound == null) {
      upperBound = metrics.upperBound();
    }

    return Optional.ofNullable(Conversions.toByteBuffer(type, upperBound));
  }
}
