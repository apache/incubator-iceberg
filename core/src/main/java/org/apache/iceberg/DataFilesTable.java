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

package org.apache.iceberg;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

/**
 * A {@link Table} implementation that exposes a table's data files as rows.
 */
public class DataFilesTable extends BaseMetadataTable {

  DataFilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".files");
  }

  DataFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new FilesTableScan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    Schema schema = new Schema(DataFile.getType(table().spec().partitionType()).fields());
    if (table().spec().fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field
      return TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    } else {
      return schema;
    }
  }

  @Override
  public Schema schemaForSnapshot(long snapshotId) {
    return schema();
  }

  @Override
  public Schema schemaForSnapshotAsOfTime(long timestampMillis) {
    return schema();
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.FILES;
  }

  public static class FilesTableScan extends BaseTableScan {
    private final Schema fileSchema;

    FilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
    }

    private FilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                           TableScanContext context) {
      super(ops, table, schema, context);
      this.fileSchema = fileSchema;
    }

    @Override
    public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.FILES.name()));
    }

    @Override
    public TableScan appendsAfter(long fromSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.FILES.name()));
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new FilesTableScan(ops, table, schema, fileSchema, context);
    }

    @Override
    public long targetSplitSize() {
      return tableOps().current().propertyAsLong(
          TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter,
        boolean ignoreResiduals, boolean caseSensitive, boolean colStats) {
      CloseableIterable<ManifestFile> manifests = CloseableIterable.withNoopClose(snapshot.dataManifests());
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      // Data tasks produce the table schema, not the projection schema and projection is done by processing engines.
      // This data task needs to use the table schema, which may not include a partition schema to avoid having an
      // empty struct in the schema for unpartitioned tables. Some engines, like Spark, can't handle empty structs in
      // all cases.
      return CloseableIterable.transform(manifests, manifest ->
          new ManifestReadTask(ops.io(), manifest, fileSchema, schemaString, specString, residuals));
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final FileIO io;
    private final ManifestFile manifest;
    private final Schema schema;

    ManifestReadTask(FileIO io, ManifestFile manifest, Schema schema, String schemaString,
                     String specString, ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.io = io;
      this.manifest = manifest;
      this.schema = schema;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      return CloseableIterable.transform(
          ManifestFiles.read(manifest, io).project(schema),
          file -> (GenericDataFile) file);
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }
}
