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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsFileFilter {
  private static final Filter[] NO_FILTERS = new Filter[0];

  private final SparkSession spark;
  private final Table table;
  private final CaseInsensitiveStringMap options;
  private final List<String> metaColumns = Lists.newArrayList();

  private Schema schema = null;
  private StructType requestedProjection;
  private boolean caseSensitive;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;
  private boolean ignoreResiduals = false;
  private FilterFiles filter = null;

  // lazy variables
  private JavaSparkContext lazySparkContext = null;

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.options = options;
    this.caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  private JavaSparkContext lazySparkContext() {
    if (lazySparkContext == null) {
      this.lazySparkContext = new JavaSparkContext(spark.sparkContext());
    }
    return lazySparkContext;
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedProjection != null) {
        // the projection should include all columns that will be returned, including those only used in filters
        this.schema = SparkSchemaUtil.prune(table.schema(), requestedProjection, filterExpression(), caseSensitive);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private Expression filterExpression() {
    if (filterExpressions != null) {
      return filterExpressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }
    return Expressions.alwaysTrue();
  }

  public SparkScanBuilder caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(table.schema().asStruct(), expr, caseSensitive);
          expressions.add(expr);
          pushed.add(filter);
        } catch (ValidationException e) {
          // binding to the table schema failed, so this expression cannot be pushed down
        }
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requestedSchema) {
    this.requestedProjection = new StructType(Stream.of(requestedSchema.fields())
        .filter(field -> MetadataColumns.nonMetadataColumn(field.name()))
        .toArray(StructField[]::new));

    Stream.of(requestedSchema.fields())
        .map(StructField::name)
        .filter(MetadataColumns::isMetadataColumn)
        .distinct()
        .forEach(metaColumns::add);
  }

  @Override
  public void filterFiles(FilterFiles files) {
    this.filter = files;
  }

  public SparkScanBuilder ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  private Schema schemaWithMetadataColumns() {
    // metadata columns
    List<Types.NestedField> fields = metaColumns.stream()
        .distinct()
        .map(MetadataColumns::get)
        .collect(Collectors.toList());
    Schema meta = new Schema(fields);

    // schema or rows returned by readers
    return TypeUtil.join(lazySchema(), meta);
  }

  @Override
  public Scan build() {
    Broadcast<FileIO> io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryption = lazySparkContext().broadcast(table.encryption());

    return new SparkBatchQueryScan(
        table, io, encryption, caseSensitive, schemaWithMetadataColumns(), filterExpressions, options);
  }

  public Scan buildMergeScan() {
    Broadcast<FileIO> io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryption = lazySparkContext().broadcast(table.encryption());

    metaColumns.add(MetadataColumns.FILE_PATH.name());
    metaColumns.add(MetadataColumns.ROW_POSITION.name());

    return new SparkMergeScan(
        table, io, encryption, caseSensitive, ignoreResiduals,
        schemaWithMetadataColumns(), filterExpressions, options, filter);
  }
}
