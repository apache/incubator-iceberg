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

package org.apache.iceberg.flink.actions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrate exist hive table to iceberg table.
 * <p>
 * It support migrate hive table with parquet and orc format now, we can migrate it to iceberg table with {@link
 * HadoopCatalog} and {@link HiveCatalog}.
 * <p>
 * The migration method is to keep the data file in the original hive table unchanged, and then create a new iceberg
 * table, the new iceberg table use the data file of hive, and generate new metadata for the iceberg table.
 * <p>
 * In order to prevent data from being written to hive during the migration process so that new data cannot be migrated.
 * so when we do the migration, we need to stop the writing hive job first, then migrate, and finally modify the logic
 * to reading and writing iceberg table,If migrate failed, we will clean the iceberg table and metadata.If unfortunately
 * the clean failed, you may need to manually clean the iceberg table and manifests.
 */
public class MigrateAction implements Action<MigrateAction, List<ManifestFile>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MigrateAction.class);

  private static final String PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  private static final String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

  private static final String ICEBERG_METADATA_FOLDER = "metadata";

  private final StreamExecutionEnvironment env;
  private final HiveCatalog flinkHiveCatalog; // the HiveCatalog of flink
  private final String hiveSourceDatabaseName;
  private final String hiveSourceTableName;
  private final Catalog icebergCatalog;
  private final Namespace baseNamespace;
  private final String icebergDatabaseName;
  private final String icebergTableName;
  private int maxParallelism;

  public MigrateAction(StreamExecutionEnvironment env, HiveCatalog flinkHiveCatalog, String hiveSourceDatabaseName,
                       String hiveSourceTableName, Catalog icebergCatalog, Namespace baseNamespace,
                       String icebergDatabaseName,
                       String icebergTableName) {
    this.env = env;
    this.flinkHiveCatalog = flinkHiveCatalog;
    this.hiveSourceDatabaseName = hiveSourceDatabaseName;
    this.hiveSourceTableName = hiveSourceTableName;
    this.icebergCatalog = icebergCatalog;
    this.baseNamespace = baseNamespace;
    this.icebergDatabaseName = icebergDatabaseName;
    this.icebergTableName = icebergTableName;
    this.maxParallelism = env.getParallelism();
  }

  @Override
  public List<ManifestFile> execute() {
    flinkHiveCatalog.open();
    // hive source table
    ObjectPath tableSource = new ObjectPath(hiveSourceDatabaseName, hiveSourceTableName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable;
    try {
      hiveTable = flinkHiveCatalog.getHiveTable(tableSource);
    } catch (TableNotExistException e) {
      throw new RuntimeException(String.format("The source table %s not exists ! ", hiveSourceTableName));
    }

    List<FieldSchema> fieldSchemaList = hiveTable.getSd().getCols();
    List<FieldSchema> partitionList = hiveTable.getPartitionKeys();
    fieldSchemaList.addAll(partitionList);
    Schema icebergSchema = HiveSchemaUtil.convert(fieldSchemaList);
    PartitionSpec spec = HiveSchemaUtil.spec(icebergSchema, partitionList);

    FileFormat fileFormat = getHiveFileFormat(hiveTable);

    Namespace namespace =
        Namespace.of(ObjectArrays.concat(baseNamespace.levels(), new String[] {icebergDatabaseName}, String.class));
    TableIdentifier identifier = TableIdentifier.of(namespace, icebergTableName);
    String hiveLocation = hiveTable.getSd().getLocation();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    if (!baseNamespace.isEmpty()) {
      properties.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
    }

    Table icebergTable = null;
    if (icebergCatalog instanceof HadoopCatalog) {
      icebergTable = icebergCatalog.createTable(identifier, icebergSchema, spec, properties);
    } else if (icebergCatalog instanceof org.apache.iceberg.hive.HiveCatalog) {
      icebergTable = icebergCatalog.createTable(identifier, icebergSchema, spec, hiveLocation, properties);
    }

    ensureNameMappingPresent(icebergTable);

    String metadataLocation = getMetadataLocation(icebergTable);

    String nameMapping =
        PropertyUtil.propertyAsString(icebergTable.properties(), TableProperties.DEFAULT_NAME_MAPPING, null);
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(icebergTable.properties());

    List<ManifestFile> manifestFiles = null;
    try {
      if (spec.isUnpartitioned()) {
        manifestFiles =
            migrateUnpartitionedTable(spec, fileFormat, hiveLocation, nameMapping, metricsConfig, metadataLocation);
      } else {
        manifestFiles =
            migratePartitionedTable(spec, tableSource, nameMapping, fileFormat, metricsConfig, metadataLocation);
      }

      boolean snapshotIdInheritanceEnabled = PropertyUtil.propertyAsBoolean(
          icebergTable.properties(),
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);

      AppendFiles append = icebergTable.newAppend();
      manifestFiles.forEach(append::appendManifest);
      append.commit();

      if (!snapshotIdInheritanceEnabled) {
        // delete original manifests as they were rewritten before the commit
        deleteManifests(icebergTable.io(), manifestFiles);
      }
    } catch (Exception e) {
      LOGGER.error("Migrate hive table to iceberg table failed.", e);
      icebergCatalog.dropTable(identifier);
      deleteManifests(icebergTable.io(), manifestFiles);
    } finally {
      flinkHiveCatalog.close();
    }

    return manifestFiles;
  }

  private void ensureNameMappingPresent(Table table) {
    if (!table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      NameMapping nameMapping = MappingUtil.create(table.schema());
      String nameMappingJson = NameMappingParser.toJson(nameMapping);
      table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, nameMappingJson).commit();
    }
  }

  private static void deleteManifests(FileIO io, List<ManifestFile> manifests) {
    Tasks.foreach(manifests)
        .noRetry()
        .suppressFailureWhenFinished()
        .run(item -> io.deleteFile(item.path()));
  }

  private static FileFormat getHiveFileFormat(org.apache.hadoop.hive.metastore.api.Table hiveTable) {
    String hiveFormat = hiveTable.getSd().getInputFormat();
    FileFormat fileFormat;
    switch (hiveFormat) {
      case PARQUET_INPUT_FORMAT:
        fileFormat = FileFormat.PARQUET;
        break;

      case ORC_INPUT_FORMAT:
        fileFormat = FileFormat.ORC;
        break;

      default:
        throw new UnsupportedOperationException("Unsupported file format");
    }

    return fileFormat;
  }

  private String getMetadataLocation(Table table) {
    return table.properties()
        .getOrDefault(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/" + ICEBERG_METADATA_FOLDER);
  }

  private List<ManifestFile> migrateUnpartitionedTable(PartitionSpec spec, FileFormat fileFormat, String hiveLocation,
                                                       String nameMapping, MetricsConfig metricsConfig,
                                                       String metadataLocation) throws Exception {
    MigrateMapper migrateMapper = new MigrateMapper(spec, nameMapping, fileFormat, metricsConfig, metadataLocation);
    DataStream<PartitionAndLocation> dataStream =
        env.fromElements(new PartitionAndLocation(hiveLocation, Maps.newHashMap()));
    DataStream<ManifestFile> ds = dataStream.flatMap(migrateMapper);
    return Lists.newArrayList(ds.executeAndCollect("migrate table :" + hiveSourceTableName));
  }

  private List<ManifestFile> migratePartitionedTable(PartitionSpec spec, ObjectPath tableSource,
                                                     String nameMapping, FileFormat fileFormat,
                                                     MetricsConfig metricsConfig, String metadataLocation)
      throws Exception {
    List<CatalogPartitionSpec> partitionSpecs = flinkHiveCatalog.listPartitions(tableSource);
    List<PartitionAndLocation> inputs = Lists.newArrayList();
    for (CatalogPartitionSpec partitionSpec : partitionSpecs) {
      Partition partition =
          flinkHiveCatalog.getHivePartition(flinkHiveCatalog.getHiveTable(tableSource), partitionSpec);
      inputs.add(
          new PartitionAndLocation(partition.getSd().getLocation(), Maps.newHashMap(partitionSpec.getPartitionSpec())));
    }

    int size = partitionSpecs.size();
    int parallelism = Math.min(size, maxParallelism);

    DataStream<PartitionAndLocation> dataStream = env.fromCollection(inputs);
    MigrateMapper migrateMapper = new MigrateMapper(spec, nameMapping, fileFormat, metricsConfig, metadataLocation);
    DataStream<ManifestFile> ds = dataStream.flatMap(migrateMapper).setParallelism(parallelism);
    return Lists.newArrayList(ds.executeAndCollect("migrate table :" + hiveSourceTableName));
  }

  private static class MigrateMapper extends RichFlatMapFunction<PartitionAndLocation, ManifestFile> {
    private final PartitionSpec spec;
    private final String nameMappingString;
    private final FileFormat fileFormat;
    private final MetricsConfig metricsConfig;
    private final String metadataLocation;

    MigrateMapper(PartitionSpec spec, String nameMapping, FileFormat fileFormat, MetricsConfig metricsConfig,
                  String metadataLocation) {
      this.spec = spec;
      this.nameMappingString = nameMapping;
      this.fileFormat = fileFormat;
      this.metricsConfig = metricsConfig;
      this.metadataLocation = metadataLocation;
    }

    @Override
    public void flatMap(PartitionAndLocation partitionAndLocation, Collector<ManifestFile> out) {
      Map<String, String> partitions = partitionAndLocation.getPartitionSpec();
      String location = partitionAndLocation.getLocation();
      Configuration conf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
      List<DataFile> files;
      switch (fileFormat) {
        case PARQUET:
          files = listParquetPartition(partitions, location, spec, conf, metricsConfig);
          break;

        case ORC:
          files = listOrcPartition(partitions, location, spec, conf, metricsConfig, nameMapping);
          break;

        default:
          throw new UnsupportedOperationException("Unsupported file format");
      }

      ManifestFile manifestFile = buildManifest(conf, spec, metadataLocation, files);
      if (manifestFile != null) {
        out.collect(manifestFile);
      }
    }

    private ManifestFile buildManifest(Configuration conf, PartitionSpec partitionSpec,
                                       String basePath, List<DataFile> dataFiles) {
      if (dataFiles.size() > 0) {
        FileIO io = new HadoopFileIO(conf);
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        int attemptId = getRuntimeContext().getAttemptNumber();
        String suffix = String.format("manifest-%d-%d-%s", subTaskId, attemptId, UUID.randomUUID());
        Path location = new Path(basePath, suffix);
        String outputPath = FileFormat.AVRO.addExtension(location.toString());
        OutputFile outputFile = io.newOutputFile(outputPath);
        ManifestWriter<DataFile> writer = ManifestFiles.write(partitionSpec, outputFile);
        try (ManifestWriter<DataFile> manifestWriter = writer) {
          dataFiles.forEach(manifestWriter::add);
        } catch (IOException e) {
          throw new UncheckedIOException("Unable to close the manifest writer", e);
        }

        return writer.toManifestFile();
      } else {
        return null;
      }
    }

    private List<DataFile> listOrcPartition(Map<String, String> partitionPath, String partitionUri,
                                            PartitionSpec partitionSpec, Configuration conf,
                                            MetricsConfig metricsSpec, NameMapping mapping) {
      try {
        Path partition = new Path(partitionUri);
        FileSystem fs = partition.getFileSystem(conf);

        return Arrays.stream(fs.listStatus(partition, FileUtils.HIDDEN_FILES_PATH_FILTER))
            .filter(FileStatus::isFile)
            .map(stat -> {
              Metrics metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath(), conf),
                  metricsSpec, mapping);
              String partitionKey = partitionSpec.fields().stream()
                  .map(PartitionField::name)
                  .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                  .collect(Collectors.joining("/"));

              return DataFiles.builder(partitionSpec)
                  .withPath(stat.getPath().toString())
                  .withFormat(FileFormat.ORC)
                  .withFileSizeInBytes(stat.getLen())
                  .withMetrics(metrics)
                  .withPartitionPath(partitionKey)
                  .build();

            }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Unable to list files in partition: %s", partitionUri), e);
      }
    }

    private static List<DataFile> listParquetPartition(Map<String, String> partitionPath, String partitionUri,
                                                       PartitionSpec spec, Configuration conf,
                                                       MetricsConfig metricsSpec) {
      try {
        Path partition = new Path(partitionUri);
        FileSystem fs = partition.getFileSystem(conf);

        return Arrays.stream(fs.listStatus(partition, FileUtils.HIDDEN_FILES_PATH_FILTER))
            .filter(FileStatus::isFile)
            .map(stat -> {
              InputFile inputFile = HadoopInputFile.fromPath(stat.getPath(), conf);
              Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsSpec);

              String partitionKey = spec.fields().stream()
                  .map(PartitionField::name)
                  .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                  .collect(Collectors.joining("/"));

              return DataFiles.builder(spec)
                  .withPath(stat.getPath().toString())
                  .withFormat(FileFormat.PARQUET)
                  .withFileSizeInBytes(stat.getLen())
                  .withMetrics(metrics)
                  .withPartitionPath(partitionKey)
                  .build();

            }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Unable to list files in partition: %s", partitionUri), e);
      }
    }
  }

  public static class PartitionAndLocation implements java.io.Serializable {
    private String location;
    private Map<String, String> partitionSpec;

    public PartitionAndLocation(String location, Map<String, String> partitionSpec) {
      this.location = location;
      this.partitionSpec = partitionSpec;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getPartitionSpec() {
      return partitionSpec;
    }

    public void setPartitionSpec(Map<String, String> partitionSpec) {
      this.partitionSpec = partitionSpec;
    }
  }

  public MigrateAction maxParallelism(int parallelism) {
    Preconditions.checkArgument(parallelism > 0, "Invalid max parallelism %d", parallelism);
    this.maxParallelism = parallelism;
    return this;
  }
}
