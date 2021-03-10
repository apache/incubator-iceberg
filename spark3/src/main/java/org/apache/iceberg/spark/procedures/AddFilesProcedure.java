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


package org.apache.iceberg.spark.procedures;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class AddFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("source_table", DataTypes.StringType),
      ProcedureParameter.optional("partition_filter", STRING_MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("added_files_count", DataTypes.LongType, false, Metadata.empty())
  });

  private AddFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<AddFilesProcedure>() {
      @Override
      protected AddFilesProcedure doBuild() {
        return new AddFilesProcedure(tableCatalog());
      }
    };
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

    CatalogPlugin sessionCat = spark().sessionState().catalogManager().v2SessionCatalog();
    Identifier sourceIdent = toCatalogAndIdentifier(args.getString(1), PARAMETERS[1].name(), sessionCat).identifier();

    Map<String, String> partitionFilter = Maps.newHashMap();
    if (!args.isNullAt(2)) {
      args.getMap(2).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            partitionFilter.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    long addedFilesCount = importToIceberg(tableIdent, sourceIdent, partitionFilter);
    return new InternalRow[]{newInternalRow(addedFilesCount)};
  }

  private boolean isFileIdentifier(Identifier ident) {
    String[] namespace = ident.namespace();
    return namespace.length == 1 &&
        (namespace[0].equalsIgnoreCase("orc") ||
            namespace[0].equalsIgnoreCase("parquet") ||
            namespace[0].equalsIgnoreCase("avro"));
  }

  private long importToIceberg(Identifier destIdent, Identifier sourceIdent, Map<String, String> partitionFilter) {
    return modifyIcebergTable(destIdent, table -> {

      validatePartitionSpec(table, partitionFilter);
      ensureNameMappingPresent(table);

      if (isFileIdentifier(sourceIdent)) {
        Path sourcePath = new Path(sourceIdent.name());
        String format = sourceIdent.namespace()[0];
        importFileTable(table, sourcePath, format, partitionFilter);
      } else {
        importCatalogTable(table, sourceIdent, partitionFilter);
      }

      Snapshot snapshot = table.currentSnapshot();
      return Long.parseLong(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP));
    });
  }

  private static void ensureNameMappingPresent(Table table) {
    if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
      // Forces Name based resolution instead of position based resolution
      NameMapping mapping = MappingUtil.create(table.schema());
      String mappingJson = NameMappingParser.toJson(mapping);
      table.updateProperties()
          .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
          .commit();
    }
  }

  private void importFileTable(Table table, Path tableLocation, String format, Map<String, String> partitionFilter) {
    // List Partitions via Spark InMemory file search interface
    List<SparkPartition> partitions = Spark3Util.getPartitions(spark(), tableLocation, format);
    List<SparkPartition> filteredPartitions = SparkTableUtil.filterPartitions(partitions, partitionFilter);
    importPartitions(table, filteredPartitions);
  }

  private void importCatalogTable(Table table, Identifier sourceIdent, Map<String, String> partitionFilter) {
    String stagingLocation = table.properties()
        .getOrDefault(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/metadata");
    TableIdentifier sourceTableIdentifier = Spark3Util.toV1TableIdentifier(sourceIdent);
    SparkTableUtil.importSparkTable(spark(), sourceTableIdentifier, table, stagingLocation, partitionFilter);
  }

  private void importPartitions(Table table, List<SparkTableUtil.SparkPartition> partitions) {
    String stagingLocation = table.properties()
        .getOrDefault(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/metadata");
    SparkTableUtil.importSparkPartitions(spark(), partitions, table, table.spec(), stagingLocation);
  }

  @Override
  public String description() {
    return null;
  }

  private void validatePartitionSpec(Table table, Map<String, String> partitionFilter) {
    List<PartitionField> partitionFields = table.spec().fields();
    Set<String> partitionNames = table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toSet());

    boolean tablePartitioned = !partitionFields.isEmpty();
    boolean partitionSpecPassed = !partitionFilter.isEmpty();

    if (tablePartitioned && partitionSpecPassed) {
      // Not enough partition columns
      Preconditions.checkArgument(partitionFields.size() >= partitionFilter.size(),
          "Cannot add data files to target table %s because that table is partitioned, " +
              "but the number of columns in the provided partition filter (%d) " +
              "is greater than the number of partitioned columns in table (%d)",
          table.name(), partitionFilter.size(), partitionFields.size());

      // Check for any non-identity partition columns
      List<PartitionField> nonIdentityFields =
          partitionFields.stream().filter(x -> !x.transform().isIdentity()).collect(Collectors.toList());
      Preconditions.checkArgument(nonIdentityFields.isEmpty(),
          "Cannot add data files to target table %s because that table is partitioned and contains non-identity" +
              "partition transforms which will not be compatible. Found non-identity fields %s",
          table.name(), nonIdentityFields);

      // Check for any filters of non existent columns
      List<String> unMatchedFilters =
          partitionFilter.keySet().stream().filter(filterName -> !partitionNames.contains(filterName))
              .collect(Collectors.toList());
      Preconditions.checkArgument(unMatchedFilters.isEmpty(),
          "Cannot add files to target table %s. %s is partitioned but the specified partition filter " +
              "refers to columns that are not partitioned: '%s'",
          table.name(), table.name(), unMatchedFilters);
    } else {
      Preconditions.checkArgument(!partitionSpecPassed,
          "Cannot use partition filter with an unpartitioned table %s",
          table.name());
    }
  }
}
