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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BaseRewriteManifestsActionResult;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.MetadataTableType.ENTRIES;

/**
 * An action that rewrites manifests in a distributed manner and co-locates metadata for partitions.
 * <p>
 * By default, this action rewrites all manifests for the current partition spec and writes the result
 * to the metadata folder. The behavior can be modified by passing a custom predicate to {@link #rewriteIf(Predicate)}
 * and a custom spec id to {@link #specId(int)}. In addition, there is a way to configure a custom location
 * for new manifests via {@link #stagingLocation}.
 */
public class BaseRewriteManifestsSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteManifests, RewriteManifests.Result>
    implements RewriteManifests {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteManifestsSparkAction.class);

  private static final String USE_CACHING = "use-caching";
  private static final boolean USE_CACHING_DEFAULT = true;

  private final Encoder<ManifestFile> manifestEncoder;
  private final Table table;
  private final int formatVersion;
  private final FileIO fileIO;
  private final long targetManifestSizeBytes;

  private PartitionSpec spec = null;
  private Predicate<ManifestFile> predicate = manifest -> true;
  private String stagingLocation = null;

  public BaseRewriteManifestsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.manifestEncoder = Encoders.javaSerialization(ManifestFile.class);
    this.table = table;
    this.spec = table.spec();
    this.targetManifestSizeBytes = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.MANIFEST_TARGET_SIZE_BYTES,
        TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.fileIO = SparkUtil.serializableFileIO(table);

    // default the staging location to the metadata location
    TableOperations ops = ((HasTableOperations) table).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    this.stagingLocation = metadataFilePath.getParent().toString();

    // use the current table format version for new manifests
    this.formatVersion = ops.current().formatVersion();
  }

  @Override
  protected RewriteManifests self() {
    return this;
  }

  @Override
  public RewriteManifests specId(int specId) {
    Preconditions.checkArgument(table.specs().containsKey(specId), "Invalid spec id %d", specId);
    this.spec = table.specs().get(specId);
    return this;
  }

  @Override
  public RewriteManifests rewriteIf(Predicate<ManifestFile> newPredicate) {
    this.predicate = newPredicate;
    return this;
  }

  @Override
  public RewriteManifests stagingLocation(String newStagingLocation) {
    this.stagingLocation = newStagingLocation;
    return this;
  }

  @Override
  public RewriteManifests.Result execute() {
    String desc = String.format("Rewriting manifests (staging location=%s) of %s", stagingLocation, table.name());
    JobGroupInfo info = newJobGroupInfo("REWRITE-MANIFESTS", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private RewriteManifests.Result doExecute() {
    List<ManifestFile> matchingManifests = findMatchingManifests();
    if (matchingManifests.isEmpty()) {
      return BaseRewriteManifestsActionResult.empty();
    }

    long totalSizeBytes = 0L;
    int numEntries = 0;

    for (ManifestFile manifest : matchingManifests) {
      ValidationException.check(hasFileCounts(manifest), "No file counts in manifest: %s", manifest.path());

      totalSizeBytes += manifest.length();
      numEntries += manifest.addedFilesCount() + manifest.existingFilesCount() + manifest.deletedFilesCount();
    }

    int targetNumManifests = targetNumManifests(totalSizeBytes);
    int targetNumManifestEntries = targetNumManifestEntries(numEntries, targetNumManifests);

    Dataset<Row> manifestEntryDF = buildManifestEntryDF(matchingManifests);

    List<ManifestFile> newManifests;
    if (spec.fields().size() < 1) {
      newManifests = writeManifestsForUnpartitionedTable(manifestEntryDF, targetNumManifests);
    } else {
      newManifests = writeManifestsForPartitionedTable(manifestEntryDF, targetNumManifests, targetNumManifestEntries);
    }

    replaceManifests(matchingManifests, newManifests);

    return new BaseRewriteManifestsActionResult(matchingManifests, newManifests);
  }

  private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF = spark()
        .createDataset(Lists.transform(manifests, ManifestFile::path), Encoders.STRING())
        .toDF("manifest");

    Dataset<Row> manifestEntryDF = loadMetadataTable(table, ENTRIES)
        .filter("status < 2") // select only live entries
        .selectExpr("input_file_name() as manifest", "snapshot_id", "sequence_number", "data_file");

    Column joinCond = manifestDF.col("manifest").equalTo(manifestEntryDF.col("manifest"));
    return manifestEntryDF
        .join(manifestDF, joinCond, "left_semi")
        .select("snapshot_id", "sequence_number", "data_file");
  }

  private List<ManifestFile> writeManifestsForUnpartitionedTable(Dataset<Row> manifestEntryDF, int numManifests) {
    Broadcast<FileIO> io = sparkContext().broadcast(fileIO);
    StructType sparkType = (StructType) manifestEntryDF.schema().apply("data_file").dataType();

    // we rely only on the target number of manifests for unpartitioned tables
    // as we should not worry about having too much metadata per partition
    long maxNumManifestEntries = Long.MAX_VALUE;

    return manifestEntryDF
        .repartition(numManifests)
        .mapPartitions(
            toManifests(io, maxNumManifestEntries, stagingLocation, formatVersion, spec, sparkType),
            manifestEncoder
        )
        .collectAsList();
  }

  private List<ManifestFile> writeManifestsForPartitionedTable(
      Dataset<Row> manifestEntryDF, int numManifests,
      int targetNumManifestEntries) {

    Broadcast<FileIO> io = sparkContext().broadcast(fileIO);
    StructType sparkType = (StructType) manifestEntryDF.schema().apply("data_file").dataType();

    // we allow the actual size of manifests to be 10% higher if the estimation is not precise enough
    long maxNumManifestEntries = (long) (1.1 * targetNumManifestEntries);

    return withReusableDS(manifestEntryDF, df -> {
      Column partitionColumn = df.col("data_file.partition");
      return df.repartitionByRange(numManifests, partitionColumn)
          .sortWithinPartitions(partitionColumn)
          .mapPartitions(
              toManifests(io, maxNumManifestEntries, stagingLocation, formatVersion, spec, sparkType),
              manifestEncoder
          )
          .collectAsList();
    });
  }

  private <T, U> U withReusableDS(Dataset<T> ds, Function<Dataset<T>, U> func) {
    Dataset<T> reusableDS;
    boolean useCaching = PropertyUtil.propertyAsBoolean(options(), USE_CACHING, USE_CACHING_DEFAULT);
    if (useCaching) {
      reusableDS = ds.cache();
    } else {
      int parallelism = SQLConf.get().numShufflePartitions();
      reusableDS = ds.repartition(parallelism).map((MapFunction<T, T>) value -> value, ds.exprEnc());
    }

    try {
      return func.apply(reusableDS);
    } finally {
      if (useCaching) {
        reusableDS.unpersist(false);
      }
    }
  }

  private List<ManifestFile> findMatchingManifests() {
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null) {
      return ImmutableList.of();
    }

    return currentSnapshot.dataManifests().stream()
        .filter(manifest -> manifest.partitionSpecId() == spec.specId() && predicate.test(manifest))
        .collect(Collectors.toList());
  }

  private int targetNumManifests(long totalSizeBytes) {
    return (int) ((totalSizeBytes + targetManifestSizeBytes - 1) / targetManifestSizeBytes);
  }

  private int targetNumManifestEntries(int numEntries, int numManifests) {
    return (numEntries + numManifests - 1) / numManifests;
  }

  private boolean hasFileCounts(ManifestFile manifest) {
    return manifest.addedFilesCount() != null &&
        manifest.existingFilesCount() != null &&
        manifest.deletedFilesCount() != null;
  }

  private void replaceManifests(Iterable<ManifestFile> deletedManifests, Iterable<ManifestFile> addedManifests) {
    try {
      boolean snapshotIdInheritanceEnabled = PropertyUtil.propertyAsBoolean(
          table.properties(),
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);

      org.apache.iceberg.RewriteManifests rewriteManifests = table.rewriteManifests();
      deletedManifests.forEach(rewriteManifests::deleteManifest);
      addedManifests.forEach(rewriteManifests::addManifest);
      commit(rewriteManifests);

      if (!snapshotIdInheritanceEnabled) {
        // delete new manifests as they were rewritten before the commit
        deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      }
    } catch (Exception e) {
      // delete all new manifests because the rewrite failed
      deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      throw e;
    }
  }

  private void deleteFiles(Iterable<String> locations) {
    Tasks.foreach(locations)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
        .run(fileIO::deleteFile);
  }

  private static ManifestFile writeManifest(
      List<Row> rows, int startIndex, int endIndex, Broadcast<FileIO> io,
      String location, int format, PartitionSpec spec, StructType sparkType) throws IOException {

    String manifestName = "optimized-m-" + UUID.randomUUID();
    Path manifestPath = new Path(location, manifestName);
    OutputFile outputFile = io.value().newOutputFile(FileFormat.AVRO.addExtension(manifestPath.toString()));

    Types.StructType dataFileType = DataFile.getType(spec.partitionType());
    SparkDataFile wrapper = new SparkDataFile(dataFileType, sparkType);

    ManifestWriter<DataFile> writer = ManifestFiles.write(format, spec, outputFile, null);

    try {
      for (int index = startIndex; index < endIndex; index++) {
        Row row = rows.get(index);
        long snapshotId = row.getLong(0);
        long sequenceNumber = row.getLong(1);
        Row file = row.getStruct(2);
        writer.existing(wrapper.wrap(file), snapshotId, sequenceNumber);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static MapPartitionsFunction<Row, ManifestFile> toManifests(
      Broadcast<FileIO> io, long maxNumManifestEntries, String location,
      int format, PartitionSpec spec, StructType sparkType) {

    return rows -> {
      List<Row> rowsAsList = Lists.newArrayList(rows);

      if (rowsAsList.isEmpty()) {
        return Collections.emptyIterator();
      }

      List<ManifestFile> manifests = Lists.newArrayList();
      if (rowsAsList.size() <= maxNumManifestEntries) {
        manifests.add(writeManifest(rowsAsList, 0, rowsAsList.size(), io, location, format, spec, sparkType));
      } else {
        int midIndex = rowsAsList.size() / 2;
        manifests.add(writeManifest(rowsAsList, 0, midIndex, io, location, format, spec, sparkType));
        manifests.add(writeManifest(rowsAsList,  midIndex, rowsAsList.size(), io, location, format, spec, sparkType));
      }

      return manifests.iterator();
    };
  }
}
