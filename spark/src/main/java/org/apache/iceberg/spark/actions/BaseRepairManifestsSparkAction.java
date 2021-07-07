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

package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.BaseRepairManifestsActionResult;
import org.apache.iceberg.actions.RepairManifests;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.Tuple2;

import static org.apache.iceberg.MetadataTableType.ENTRIES;

/**
 * An action that repairs manifests in a distributed manner, reading metadata of the data files to repair
 * manifest entries in each manifest.
 * <p>
 * By default, this action repairs all manifests by checking the data file status and reading the data file headers.
 * This can be changed by passing in repair options like {@link #repairMetrics(boolean)}, and manifest predicates
 * via {@link #repairIf(Predicate)}.
 * <p>
 * In addition, there is a way to configure a custom location for new manifests via {@link #stagingLocation}.
 */
public class BaseRepairManifestsSparkAction extends BaseManifestSparkAction<RepairManifests, RepairManifests.Result>
    implements RepairManifests {

  private final int formatVersion;
  private final SerializableConfiguration hadoopConf;

  private Predicate<ManifestFile> predicate = manifest -> true;
  private String stagingLocation;
  private boolean repairMetrics = true;

  public BaseRepairManifestsSparkAction(SparkSession spark, Table table) {
    super(spark, table);

    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());

    // default the staging location to the metadata location
    TableOperations ops = ((HasTableOperations) getTable()).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    this.stagingLocation = metadataFilePath.getParent().toString();

    // use the current table format version for repaired manifests
    this.formatVersion = ops.current().formatVersion();
  }

  @Override
  public RepairManifests repairIf(Predicate<ManifestFile> newPredicate) {
    this.predicate = newPredicate;
    return this;
  }

  @Override
  public RepairManifests stagingLocation(String newStagingLocation) {
    this.stagingLocation = newStagingLocation;
    return this;
  }

  @Override
  public RepairManifests repairMetrics(boolean repair) {
    this.repairMetrics = repair;
    return this;
  }

  @Override
  protected RepairManifests self() {
    return this;
  }

  @Override
  public Result execute() {
    String desc = String.format("Repairing manifests (staging location=%s) of %s", stagingLocation, getTable().name());
    JobGroupInfo info = newJobGroupInfo("REPAIR-MANIFESTS", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private RepairManifests.Result doExecute() {
    // Find matching manifest entries
    Map<String, ManifestFile> manifestFiles = findMatchingManifests();
    Dataset<Row> manifestEntryDf = buildManifestEntryDF(new ArrayList<>(manifestFiles.values()));
    StructType sparkType = (StructType) manifestEntryDf.schema().apply("data_file").dataType();
    JavaRDD<Row> manifestEntryRdd = manifestEntryDf.toJavaRDD();
    JavaPairRDD<ManifestFileInfo, Iterable<Row>> entriesByManifest = manifestEntryRdd.groupBy(
        r -> new ManifestFileInfo(r.getString(0), r.getInt(1)));

    // Calculate manifest entries for repair
    RepairManifestHelper.RepairOptions options = new RepairManifestHelper.RepairOptions(repairMetrics);
    Broadcast<Table> broadcastTable = sparkContext().broadcast(SerializableTable.copyOf(getTable()));
    Broadcast<SerializableConfiguration> conf = sparkContext().broadcast(hadoopConf);
    JavaRDD<Tuple2<ManifestFileInfo, Iterable<PatchedManifestEntry>>> toRepair =
        entriesByManifest.map(calculateRepairs(broadcastTable, conf, sparkType, options))
            // Process only manifest files with repaired entries
        .filter(m -> StreamSupport.stream(m._2.spliterator(), false)
            .anyMatch(p -> p.repaired));

    // Write out repaired manifests
    Broadcast<FileIO> io = sparkContext().broadcast(getFileIO());
    JavaRDD<ManifestFile> repairedManifests = toRepair.map(
        writeRepairs(io, broadcastTable, formatVersion, stagingLocation));

    // Prepare results
    List<ManifestFile> addedManifests = repairedManifests.collect();
    List<ManifestFile> deletedManifests = toRepair.collect().stream().map(t -> {
      String path = t._1().getPath();
      ManifestFile mf = manifestFiles.get(path);
      // Sanity check deleted file existed in original list
      Preconditions.checkNotNull(mf, "Manifest file cannot be null for " + path);
      return mf;
    }).collect(Collectors.toList());

    Iterable<ManifestFile> newManifests = replaceManifests(deletedManifests, addedManifests);
    return new BaseRepairManifestsActionResult(deletedManifests, Lists.newArrayList(newManifests));
  }


  private Map<String, ManifestFile> findMatchingManifests() {
    Snapshot currentSnapshot = getTable().currentSnapshot();
    if (currentSnapshot == null) {
      return ImmutableMap.of();
    }
    return currentSnapshot.dataManifests().stream()
        .filter(manifest -> predicate.test(manifest))
        .collect(Collectors.toMap(ManifestFile::path, mf -> mf));
  }

  private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF = spark()
        .createDataset(Lists.transform(manifests, m -> new ManifestFileInfo(m.path(), m.partitionSpecId())),
            Encoders.bean(ManifestFileInfo.class))
        .toDF("partition_spec_id", "manifest_path");
    Dataset<Row> manifestEntryDF = loadMetadataTable(getTable(), ENTRIES)
        .filter("status < 2") // select only live entries
        .selectExpr("input_file_name() as manifest_path", "snapshot_id", "sequence_number", "data_file");

    return manifestEntryDF.as("manifest_entry")
        .join(manifestDF.as("manifest"), "manifest_path")
        .select("manifest_entry.manifest_path", "manifest.partition_spec_id",
            "snapshot_id", "sequence_number", "data_file");
  }

  private static Function<Tuple2<ManifestFileInfo, Iterable<Row>>,
      Tuple2<ManifestFileInfo, Iterable<PatchedManifestEntry>>> calculateRepairs(
      Broadcast<Table> broadcastTable, Broadcast<SerializableConfiguration> conf,
      StructType sparkType, RepairManifestHelper.RepairOptions options) {
    return manifestFile -> {
      Iterator<Row> rowIterator = manifestFile._2().iterator();
      ManifestFileInfo manifestInfo = manifestFile._1();
      int specId = manifestInfo.partSpecId;

      PartitionSpec spec = broadcastTable.value().specs().get(specId);
      Types.StructType dataFileType = DataFile.getType(spec.partitionType());
      SparkDataFile wrapper = new SparkDataFile(dataFileType, sparkType).withSpecId(spec.specId());

      Iterable<PatchedManifestEntry> entries =
          Streams.stream(rowIterator).map(r -> {
            long snapshotId = r.getLong(2);
            long sequenceNumber = r.getLong(3);
            Row file = r.getStruct(4);
            SparkDataFile dataFile = wrapper.wrap(file);
            Optional<DataFile> repairedDataFile = RepairManifestHelper.repairDataFile(
                dataFile, broadcastTable.value(), spec, conf.value().value(), options);
            return repairedDataFile.map(
                value -> new PatchedManifestEntry(snapshotId, sequenceNumber, value, true))
                .orElseGet(() -> new PatchedManifestEntry(snapshotId, sequenceNumber, dataFile, false));
          }).collect(Collectors.toList());
      return new Tuple2<>(manifestFile._1(), entries);
    };
  }

  private static Function<Tuple2<ManifestFileInfo, Iterable<PatchedManifestEntry>>, ManifestFile> writeRepairs(
      Broadcast<FileIO> io, Broadcast<Table> broadcastTable,
      int formatVersion, String location) {
    return rows -> {
      Iterator<PatchedManifestEntry> entryIterator = rows._2().iterator();
      ManifestFileInfo manifestInfo = rows._1();
      int specId = manifestInfo.partSpecId;
      PartitionSpec spec = broadcastTable.value().specs().get(specId);

      String manifestName = "repaired-m-" + UUID.randomUUID();
      Path newManifestPath = new Path(location, manifestName);
      OutputFile outputFile = io.value().newOutputFile(FileFormat.AVRO.addExtension(newManifestPath.toString()));
      ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, outputFile, null);
      Streams.stream(entryIterator).forEach(
          e -> writer.existing(e.dataFile, e.snapshotId, e.sequenceNumber)
      );
      writer.close();
      return writer.toManifestFile();
    };
  }

  /**
   * Represents information about a manifest file, used to group the manifest entries dataframe.
   */
  public static class ManifestFileInfo implements Serializable {
    private String path;
    private int partSpecId;

    ManifestFileInfo(String path, int partSpecId) {
      this.path = path;
      this.partSpecId = partSpecId;
    }

    public String getPath() {
      return path;
    }

    public int getPartSpecId() {
      return partSpecId;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public void setPartSpecId(int partSpecId) {
      this.partSpecId = partSpecId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ManifestFileInfo that = (ManifestFileInfo) o;
      return partSpecId == that.partSpecId &&
          Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, partSpecId);
    }
  }

  /**
   * Represents a final manifest entry to write, which may or may not be repaired
   * (ie, fixed from the original).
   */
  static class PatchedManifestEntry implements Serializable {
    private final long snapshotId;
    private final long sequenceNumber;
    private final DataFile dataFile;
    private final boolean repaired;

    PatchedManifestEntry(long snapshotId, long sequenceNumber, DataFile dataFile, boolean repaired) {
      Preconditions.checkNotNull(dataFile, "Data File cannot be null");
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.dataFile = dataFile;
      this.repaired = repaired;
    }
  }
}
