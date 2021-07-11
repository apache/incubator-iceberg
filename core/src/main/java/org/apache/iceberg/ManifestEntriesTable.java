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

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

/**
 * A {@link Table} implementation that exposes a table's manifest entries as rows, for both delete and data files.
 * <p>
 * WARNING: this table exposes internal details, like files that have been deleted. For a table of the live data files,
 * use {@link DataFilesTable}.
 */
public class ManifestEntriesTable extends BaseMetadataTable {

  ManifestEntriesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".entries");
  }

  ManifestEntriesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new EntriesTableScan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    Schema schema = ManifestEntry.getSchema(table().spec().partitionType());
    if (table().spec().fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field (id 102)
      return TypeUtil.selectNot(schema, Sets.newHashSet(102));
    } else {
      return schema;
    }
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ENTRIES;
  }

  private static class EntriesTableScan extends BaseTableScan {

    EntriesTableScan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema);
    }

    private EntriesTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      super(ops, table, schema, context);
    }

    @Override
    public TableScan dataBetween(long fromSnapshotId, long toSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.ENTRIES.name()));
    }

    @Override
    public TableScan dataAfter(long fromSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.ENTRIES.name()));
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema,
                                       TableScanContext context) {
      return new EntriesTableScan(ops, table, schema, context);
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
      // return entries from both data and delete manifests
      CloseableIterable<ManifestFile> manifests = CloseableIterable.withNoopClose(snapshot.allManifests());
      Type fileProjection = schema().findType("data_file");
      Schema fileSchema = fileProjection != null ? new Schema(fileProjection.asStructType().fields()) : new Schema();
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      return CloseableIterable.transform(manifests, manifest ->
          new ManifestReadTask(ops.io(), manifest, fileSchema, schemaString, specString, residuals,
              ops.current().specsById()));
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final Schema fileSchema;
    private final FileIO io;
    private final ManifestFile manifest;
    private final Map<Integer, PartitionSpec> specsById;

    ManifestReadTask(FileIO io, ManifestFile manifest, Schema fileSchema, String schemaString,
                     String specString, ResidualEvaluator residuals, Map<Integer, PartitionSpec> specsById) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.fileSchema = fileSchema;
      this.io = io;
      this.manifest = manifest;
      this.specsById = specsById;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      if (manifest.content() == ManifestContent.DATA) {
        return CloseableIterable.transform(ManifestFiles.read(manifest, io).project(fileSchema).entries(),
            file -> (GenericManifestEntry<DataFile>) file);
      } else {
        return CloseableIterable.transform(ManifestFiles.readDeleteManifest(manifest, io, specsById)
                .project(fileSchema).entries(),
            file -> (GenericManifestEntry<DeleteFile>) file);
      }
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }
}
