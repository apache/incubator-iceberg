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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.iceberg.catalog.ExtendedSupportsDelete;
import org.apache.spark.sql.connector.iceberg.catalog.SupportsMerge;
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.DELETE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE_DEFAULT;

public class SparkTable implements org.apache.spark.sql.connector.catalog.Table,
    SupportsRead, SupportsWrite, ExtendedSupportsDelete, SupportsMerge {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTable.class);

  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of("provider", "format", "current-snapshot-id", "location", "sort-order");
  private static final Set<TableCapability> CAPABILITIES = ImmutableSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.STREAMING_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC);

  private final Table icebergTable;
  private final StructType requestedSchema;
  private final Long snapshotId;
  private final Long asOfTimestamp;
  private final boolean refreshEagerly;
  private StructType lazyTableSchema = null;
  private SparkSession lazySpark = null;

  public SparkTable(Table icebergTable, boolean refreshEagerly) {
    this(icebergTable, null, null, null, refreshEagerly);
  }

  public SparkTable(Table icebergTable, StructType requestedSchema,
      Long snapshotId, Long asOfTimestamp, boolean refreshEagerly) {
    this.icebergTable = icebergTable;
    this.requestedSchema = requestedSchema;
    this.snapshotId = snapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.refreshEagerly = refreshEagerly;

    if (requestedSchema != null) {
      // convert the requested schema to throw an exception if any requested fields are unknown
      SparkSchemaUtil.convert(snapshotSchema(), requestedSchema);
    }
  }

  private SparkSession sparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  public Table table() {
    return icebergTable;
  }

  @Override
  public String name() {
    return icebergTable.toString();
  }

  private Schema snapshotSchema() {
    if (snapshotId != null && icebergTable instanceof BaseTable) {
      return ((BaseTable) icebergTable).schemaForSnapshot(snapshotId);
    } else if (asOfTimestamp != null && icebergTable instanceof BaseTable) {
      return ((BaseTable) icebergTable).schemaForSnapshotAsOfTime(asOfTimestamp);
    } else {
      return icebergTable.schema();
    }
  }

  @Override
  public StructType schema() {
    if (lazyTableSchema == null) {
      if (requestedSchema != null) {
        this.lazyTableSchema = SparkSchemaUtil.convert(SparkSchemaUtil.prune(snapshotSchema(), requestedSchema));
      } else {
        this.lazyTableSchema = SparkSchemaUtil.convert(snapshotSchema());
      }
    }

    return lazyTableSchema;
  }

  @Override
  public Transform[] partitioning() {
    return Spark3Util.toTransforms(icebergTable.spec());
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    String fileFormat = icebergTable.properties()
        .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    propsBuilder.put("format", "iceberg/" + fileFormat);
    propsBuilder.put("provider", "iceberg");
    String currentSnapshotId = icebergTable.currentSnapshot() != null ?
        String.valueOf(icebergTable.currentSnapshot().snapshotId()) : "none";
    propsBuilder.put("current-snapshot-id", currentSnapshotId);
    propsBuilder.put("location", icebergTable.location());

    if (!icebergTable.sortOrder().isUnsorted()) {
      propsBuilder.put("sort-order", Spark3Util.describe(icebergTable.sortOrder()));
    }

    icebergTable.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (options.containsKey(SparkReadOptions.FILE_SCAN_TASK_SET_ID)) {
      // skip planning the job and fetch already staged file scan tasks
      return new SparkFilesScanBuilder(sparkSession(), icebergTable, options);
    }

    if (refreshEagerly) {
      icebergTable.refresh();
    }

    SparkScanBuilder scanBuilder = new SparkScanBuilder(sparkSession(), icebergTable, options);

    if (requestedSchema != null) {
      scanBuilder.pruneColumns(requestedSchema);
    }

    return scanBuilder;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (info.options().containsKey(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID)) {
      // replace data files in the given file scan task set with new files
      return new SparkRewriteBuilder(sparkSession(), icebergTable, info);
    } else {
      return new SparkWriteBuilder(sparkSession(), icebergTable, info);
    }
  }

  @Override
  public MergeBuilder newMergeBuilder(String operation, LogicalWriteInfo info) {
    String mode = getRowLevelOperationMode(operation);
    ValidationException.check(mode.equals("copy-on-write"), "Unsupported mode for %s: %s", operation, mode);
    return new SparkMergeBuilder(sparkSession(), icebergTable, operation, info);
  }

  private String getRowLevelOperationMode(String operation) {
    Map<String, String> props = icebergTable.properties();
    if (operation.equalsIgnoreCase("delete")) {
      return props.getOrDefault(DELETE_MODE, DELETE_MODE_DEFAULT);
    } else if (operation.equalsIgnoreCase("update")) {
      return props.getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
    } else if (operation.equalsIgnoreCase("merge")) {
      return props.getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    } else {
      throw new IllegalArgumentException("Unsupported operation: " + operation);
    }
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    if (table().specs().size() > 1) {
      // cannot guarantee a metadata delete will be successful if we have multiple specs
      return false;
    }

    Set<Integer> identitySourceIds = table().spec().identitySourceIds();
    Schema schema = table().schema();

    for (Filter filter : filters) {
      // return false if the filter requires rewrite or if we cannot translate the filter
      if (requiresRewrite(filter, schema, identitySourceIds) || SparkFilters.convert(filter) == null) {
        return false;
      }
    }

    return true;
  }

  private boolean requiresRewrite(Filter filter, Schema schema, Set<Integer> identitySourceIds) {
    // TODO: handle dots correctly via v2references
    // TODO: detect more cases that don't require rewrites
    Set<String> filterRefs = Sets.newHashSet(filter.references());
    return filterRefs.stream().anyMatch(ref -> {
      Types.NestedField field = schema.findField(ref);
      ValidationException.check(field != null, "Cannot find field %s in schema", ref);
      return !identitySourceIds.contains(field.fieldId());
    });
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    Expression deleteExpr = SparkFilters.convert(filters);

    if (deleteExpr == Expressions.alwaysFalse()) {
      LOG.info("Skipping the delete operation as the condition is always false");
      return;
    }

    try {
      icebergTable.newDelete()
          .set("spark.app.id", sparkSession().sparkContext().applicationId())
          .deleteFromRowFilter(deleteExpr)
          .commit();
    } catch (ValidationException e) {
      throw new IllegalArgumentException("Failed to cleanly delete data files matching: " + deleteExpr, e);
    }
  }

  @Override
  public String toString() {
    return icebergTable.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    // use only name in order to correctly invalidate Spark cache
    SparkTable that = (SparkTable) other;
    return icebergTable.name().equals(that.icebergTable.name());
  }

  @Override
  public int hashCode() {
    // use only name in order to correctly invalidate Spark cache
    return icebergTable.name().hashCode();
  }
}
