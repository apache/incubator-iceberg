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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's manifest files as rows.
 */
public class ManifestsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_SCHEMA = new Schema(
      Types.NestedField.required(1, "path", Types.StringType.get()),
      Types.NestedField.required(2, "length", Types.LongType.get()),
      Types.NestedField.required(3, "partition_spec_id", Types.IntegerType.get()),
      Types.NestedField.required(4, "added_snapshot_id", Types.LongType.get()),
      Types.NestedField.required(5, "added_data_files_count", Types.IntegerType.get()),
      Types.NestedField.required(6, "existing_data_files_count", Types.IntegerType.get()),
      Types.NestedField.required(7, "deleted_data_files_count", Types.IntegerType.get()),
      Types.NestedField.required(8, "partition_summaries", Types.ListType.ofRequired(9, Types.StructType.of(
          Types.NestedField.required(10, "contains_null", Types.BooleanType.get()),
          Types.NestedField.optional(11, "lower_bound", Types.StringType.get()),
          Types.NestedField.optional(12, "upper_bound", Types.StringType.get())
      )))
  );

  private final TableOperations ops;
  private final Table table;
  private final PartitionSpec spec;
  private final String name;

  ManifestsTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".manifests");
  }

  ManifestsTable(TableOperations ops, Table table, String name) {
    this.ops = ops;
    this.table = table;
    this.spec = table.spec();
    this.name = name;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public TableScan newScan() {
    return new ManifestsTableScan();
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_SCHEMA;
  }

  protected DataTask task(TableScan scan) {
    String location = scan.snapshot().manifestListLocation();
    return StaticDataTask.of(
        ops.io().newInputFile(location != null ? location : ops.current().metadataFileLocation()),
        scan.snapshot().allManifests(),
        manifest -> ManifestsTable.manifestFileToRow(spec, manifest));
  }

  private class ManifestsTableScan extends StaticTableScan {
    ManifestsTableScan() {
      super(ops, table, SNAPSHOT_SCHEMA, ManifestsTable.this::task);
    }
  }

  static StaticDataTask.Row manifestFileToRow(PartitionSpec spec, ManifestFile manifest) {
    return StaticDataTask.Row.of(
        manifest.path(),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.snapshotId(),
        manifest.addedFilesCount(),
        manifest.existingFilesCount(),
        manifest.deletedFilesCount(),
        partitionSummariesToRows(spec, manifest.partitions())
    );
  }

  static List<StaticDataTask.Row> partitionSummariesToRows(PartitionSpec spec,
                                                           List<ManifestFile.PartitionFieldSummary> summaries) {
    if (summaries == null) {
      return null;
    }

    List<StaticDataTask.Row> rows = Lists.newArrayList();

    for (int i = 0; i < spec.fields().size(); i += 1) {
      ManifestFile.PartitionFieldSummary summary = summaries.get(i);
      rows.add(StaticDataTask.Row.of(
          summary.containsNull(),
          spec.fields().get(i).transform().toHumanString(
              Conversions.fromByteBuffer(spec.partitionType().fields().get(i).type(), summary.lowerBound())),
          spec.fields().get(i).transform().toHumanString(
              Conversions.fromByteBuffer(spec.partitionType().fields().get(i).type(), summary.upperBound()))
      ));
    }

    return rows;
  }
}
