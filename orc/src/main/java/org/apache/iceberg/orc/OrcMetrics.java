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

package org.apache.iceberg.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class OrcMetrics {

  private enum Bound {
    LOWER, UPPER
  }

  private OrcMetrics() {
  }

  public static Metrics fromInputFile(InputFile file) {
    return fromInputFile(file, MetricsConfig.getDefault());
  }

  public static Metrics fromInputFile(InputFile file, MetricsConfig metricsConfig) {
    return fromInputFile(file, metricsConfig, null);
  }

  public static Metrics fromInputFile(InputFile file, MetricsConfig metricsConfig, NameMapping mapping) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config, metricsConfig, mapping);
  }

  static Metrics fromInputFile(InputFile file, Configuration config, MetricsConfig metricsConfig, NameMapping mapping) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {
      return buildOrcMetrics(orcReader.getNumberOfRows(), orcReader.getSchema(), orcReader.getStatistics(),
          Stream.empty(), metricsConfig, mapping);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file.location());
    }
  }

  static Metrics fromWriter(Writer writer, Stream<FieldMetrics<?>> fieldMetricsStream, MetricsConfig metricsConfig) {
    try {
      return buildOrcMetrics(writer.getNumberOfRows(), writer.getSchema(), writer.getStatistics(),
          fieldMetricsStream, metricsConfig, null);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get statistics from writer");
    }
  }

  private static Metrics buildOrcMetrics(final long numOfRows, final TypeDescription orcSchema,
                                         final ColumnStatistics[] colStats,
                                         final Stream<FieldMetrics<?>> fieldMetricsStream,
                                         final MetricsConfig metricsConfig,
                                         final NameMapping mapping) {
    final TypeDescription orcSchemaWithIds = (!ORCSchemaUtil.hasIds(orcSchema) && mapping != null) ?
        ORCSchemaUtil.applyNameMapping(orcSchema, mapping) : orcSchema;
    final Set<Integer> statsColumns = statsColumns(orcSchemaWithIds);
    final MetricsConfig effectiveMetricsConfig = Optional.ofNullable(metricsConfig)
        .orElseGet(MetricsConfig::getDefault);
    Map<Integer, Long> columnSizes = Maps.newHashMapWithExpectedSize(colStats.length);
    Map<Integer, Long> valueCounts = Maps.newHashMapWithExpectedSize(colStats.length);
    Map<Integer, Long> nullCounts = Maps.newHashMapWithExpectedSize(colStats.length);

    if (!ORCSchemaUtil.hasIds(orcSchemaWithIds)) {
      return new Metrics(numOfRows,
          columnSizes,
          valueCounts,
          nullCounts,
          null,
          null,
          null);
    }

    final Schema schema = ORCSchemaUtil.convert(orcSchemaWithIds);
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

    Map<Integer, FieldMetrics<?>> fieldMetricsMap = Optional.ofNullable(fieldMetricsStream)
        .map(stream -> stream.collect(Collectors.toMap(FieldMetrics::id, Function.identity())))
        .orElseGet(HashMap::new);

    for (int i = 0; i < colStats.length; i++) {
      final ColumnStatistics colStat = colStats[i];
      final TypeDescription orcCol = orcSchemaWithIds.findSubtype(i);
      final Optional<Types.NestedField> icebergColOpt = ORCSchemaUtil.icebergID(orcCol)
          .map(schema::findField);

      if (icebergColOpt.isPresent()) {
        final Types.NestedField icebergCol = icebergColOpt.get();
        final int fieldId = icebergCol.fieldId();
        final String colName = schema.findColumnName(icebergCol.fieldId());
        final MetricsMode metricsMode = effectiveMetricsConfig.columnMode(colName);

        columnSizes.put(fieldId, colStat.getBytesOnDisk());

        if (metricsMode == MetricsModes.None.get()) {
          continue;
        }

        if (statsColumns.contains(fieldId)) {
          // Since ORC does not track null values nor repeated ones, the value count for columns in
          // containers (maps, list) may be larger than what it actually is, however these are not
          // used in experssions right now. For such cases, we use the value number of values
          // directly stored in ORC.
          if (colStat.hasNull()) {
            nullCounts.put(fieldId, numOfRows - colStat.getNumberOfValues());
          } else {
            nullCounts.put(fieldId, 0L);
          }
          valueCounts.put(fieldId, colStat.getNumberOfValues() + nullCounts.get(fieldId));

          if (metricsMode != MetricsModes.Counts.get()) {
            Optional<ByteBuffer> orcMin = (colStat.getNumberOfValues() > 0) ?
                fromOrcMin(icebergCol.type(), colStat, metricsMode, fieldMetricsMap.get(fieldId)) : Optional.empty();
            orcMin.ifPresent(byteBuffer -> lowerBounds.put(icebergCol.fieldId(), byteBuffer));
            Optional<ByteBuffer> orcMax = (colStat.getNumberOfValues() > 0) ?
                fromOrcMax(icebergCol.type(), colStat, metricsMode, fieldMetricsMap.get(fieldId)) : Optional.empty();
            orcMax.ifPresent(byteBuffer -> upperBounds.put(icebergCol.fieldId(), byteBuffer));
          }
        }
      }
    }

    return new Metrics(numOfRows,
        columnSizes,
        valueCounts,
        nullCounts,
        MetricsUtil.createNanValueCounts(fieldMetricsMap.values().stream(), effectiveMetricsConfig, schema),
        lowerBounds,
        upperBounds);
  }

  private static Optional<ByteBuffer> fromOrcMin(Type type, ColumnStatistics columnStats,
                                                 MetricsMode metricsMode, FieldMetrics<?> fieldMetrics) {
    Object min = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      min = ((IntegerColumnStatistics) columnStats).getMinimum();
      if (type.typeId() == Type.TypeID.INTEGER) {
        min = Math.toIntExact((long) min);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      // since Orc includes NaN for upper/lower bounds of floating point columns, and we don't want this behavior,
      // we have tracked metrics for such columns ourselves and thus do not need to rely on Orc's column statistics.
      Preconditions.checkNotNull(fieldMetrics,
          "[BUG] Float or double type columns should have metrics being tracked by Iceberg Orc writers");
      min = fieldMetrics.lowerBound();
    } else if (columnStats instanceof StringColumnStatistics) {
      min = ((StringColumnStatistics) columnStats).getMinimum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      min = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMinimum())
          .map(minStats -> minStats.bigDecimalValue()
              .setScale(((Types.DecimalType) type).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      min = (int) ((DateColumnStatistics) columnStats).getMinimumDayOfEpoch();
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp minValue = tColStats.getMinimumUTC();
      min = Optional.ofNullable(minValue)
          .map(v -> DateTimeUtil.microsFromInstant(v.toInstant()))
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      min = booleanStats.getFalseCount() <= 0;
    }

    return Optional.ofNullable(Conversions.toByteBuffer(type, truncateIfNeeded(Bound.LOWER, type, min, metricsMode)));
  }

  private static Optional<ByteBuffer> fromOrcMax(Type type, ColumnStatistics columnStats,
                                                 MetricsMode metricsMode, FieldMetrics<?> fieldMetrics) {
    Object max = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      max = ((IntegerColumnStatistics) columnStats).getMaximum();
      if (type.typeId() == Type.TypeID.INTEGER) {
        max = Math.toIntExact((long) max);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      // since Orc includes NaN for upper/lower bounds of floating point columns, and we don't want this behavior,
      // we have tracked metrics for such columns ourselves and thus do not need to rely on Orc's column statistics.
      Preconditions.checkNotNull(fieldMetrics,
          "[BUG] Float or double type columns should have metrics being tracked by Iceberg Orc writers");
      max = fieldMetrics.upperBound();
    } else if (columnStats instanceof StringColumnStatistics) {
      max = ((StringColumnStatistics) columnStats).getMaximum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      max = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMaximum())
          .map(maxStats -> maxStats.bigDecimalValue()
              .setScale(((Types.DecimalType) type).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      max = (int) ((DateColumnStatistics) columnStats).getMaximumDayOfEpoch();
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp maxValue = tColStats.getMaximumUTC();
      max = Optional.ofNullable(maxValue)
          .map(v -> DateTimeUtil.microsFromInstant(v.toInstant()))
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      max = booleanStats.getTrueCount() > 0;
    }
    return Optional.ofNullable(Conversions.toByteBuffer(type, truncateIfNeeded(Bound.UPPER, type, max, metricsMode)));
  }

  private static Object truncateIfNeeded(Bound bound, Type type, Object value, MetricsMode metricsMode) {
    // Out of the two types which could be truncated, string or binary, ORC only supports string bounds.
    // Therefore, truncation will be applied if needed only on string type.
    if (!(metricsMode instanceof MetricsModes.Truncate) || type.typeId() != Type.TypeID.STRING || value == null) {
      return value;
    }

    CharSequence charSequence = (CharSequence) value;
    MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
    int truncateLength = truncateMode.length();

    switch (bound) {
      case UPPER:
        return Optional.ofNullable(UnicodeUtil.truncateStringMax(Literal.of(charSequence), truncateLength))
            .map(Literal::value).orElse(charSequence);
      case LOWER:
        return UnicodeUtil.truncateStringMin(Literal.of(charSequence), truncateLength).value();
      default:
        throw new RuntimeException("No other bound is defined.");
    }
  }

  private static Set<Integer> statsColumns(TypeDescription schema) {
    return OrcSchemaVisitor.visit(schema, new StatsColumnsVisitor());
  }

  private static class StatsColumnsVisitor extends OrcSchemaVisitor<Set<Integer>> {
    @Override
    public Set<Integer> record(TypeDescription record, List<String> names, List<Set<Integer>> fields) {
      ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
      fields.stream().filter(Objects::nonNull).forEach(result::addAll);
      record.getChildren().stream().map(ORCSchemaUtil::icebergID).filter(Optional::isPresent)
          .map(Optional::get).forEach(result::add);
      return result.build();
    }
  }
}
