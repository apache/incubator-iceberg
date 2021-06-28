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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.ManifestReader.FileType;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class ManifestFiles {
  private ManifestFiles() {
  }

  private static final org.apache.avro.Schema MANIFEST_AVRO_SCHEMA = AvroSchemaUtil.convert(ManifestFile.schema(),
      ImmutableMap.of(
          ManifestFile.schema().asStruct(), GenericManifestFile.class.getName(),
          ManifestFile.PARTITION_SUMMARY_TYPE, GenericPartitionFieldSummary.class.getName()
      ));

  /**
   * Returns a {@link CloseableIterable} of file paths in the {@link ManifestFile}.
   *
   * @param manifest a ManifestFile
   * @param io a FileIO
   * @return a manifest reader
   */
  public static CloseableIterable<String> readPaths(ManifestFile manifest, FileIO io) {
    return CloseableIterable.transform(
        read(manifest, io, null).select(ImmutableList.of("file_path")).liveEntries(),
        entry -> entry.file().path().toString());
  }

  /**
   * Returns a new {@link ManifestReader} for a {@link ManifestFile}.
   * <p>
   * <em>Note:</em> Callers should use
   * {@link ManifestFiles#read(ManifestFile, FileIO, Map)} to ensure
   * the schema used by filters is the latest table schema. This should be used only when reading
   * a manifest without filters.
   *
   * @param manifest a ManifestFile
   * @param io a FileIO
   * @return a manifest reader
   */
  public static ManifestReader<DataFile> read(ManifestFile manifest, FileIO io) {
    return read(manifest, io, null, null, false);
  }

  public static ManifestReader<DataFile> read(ManifestFile manifest, FileIO io, String tableLocation,
      boolean shouldUseRelativePaths) {
    return read(manifest, io, null, tableLocation, shouldUseRelativePaths);
  }

  /**
   * Returns a new {@link ManifestReader} for a {@link ManifestFile}.
   *
   * @param manifest a {@link ManifestFile}
   * @param io a {@link FileIO}
   * @param specsById a Map from spec ID to partition spec
   * @return a {@link ManifestReader}
   */
  public static ManifestReader<DataFile> read(ManifestFile manifest, FileIO io, Map<Integer, PartitionSpec> specsById) {
    return read(manifest, io, specsById, null, false);
  }

  public static ManifestReader<DataFile> read(ManifestFile manifest, FileIO io, Map<Integer, PartitionSpec> specsById,
      String tableLocation, boolean shouldUseRelativePaths) {
    Preconditions.checkArgument(manifest.content() == ManifestContent.DATA,
        "Cannot read a delete manifest with a ManifestReader: %s", manifest);
    InputFile file = io.newInputFile(updateManifestPathIfNecessary(manifest, tableLocation, shouldUseRelativePaths));
    InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest, tableLocation,
        shouldUseRelativePaths);
    return new ManifestReader<>(file, specsById, inheritableMetadata, FileType.DATA_FILES);
  }

  /**
   * If relative paths are enabled on table, update the manifest path to relative
   * @param manifest manifest file
   * @param tableLocation table location
   * @param shouldUseRelativePaths if relative paths is enabled
   * @return new metadata path
   */
  private static String updateManifestPathIfNecessary(ManifestFile manifest, String tableLocation,
      boolean shouldUseRelativePaths) {

    String metadataPath = manifest.path();

    if (shouldUseRelativePaths) {
      if (!metadataPath.startsWith(tableLocation)) {
        metadataPath = tableLocation + metadataPath;
      }
    }
    return metadataPath;
  }

  /**
   * Create a new {@link ManifestWriter}.
   * <p>
   * Manifests created by this writer have all entry snapshot IDs set to null.
   * All entries will inherit the snapshot ID that will be assigned to the manifest on commit.
   *
   * @param spec {@link PartitionSpec} used to produce {@link DataFile} partition tuples
   * @param outputFile the destination file location
   * @return a manifest writer
   */
  public static ManifestWriter<DataFile> write(PartitionSpec spec, OutputFile outputFile) {
    return write(spec, outputFile, null, false);
  }

  public static ManifestWriter<DataFile> write(PartitionSpec spec, OutputFile outputFile, String tableLocation,
      boolean shouldUseRelativePaths) {
    return write(1, spec, outputFile, null, tableLocation, shouldUseRelativePaths);
  }

  /**
   * Create a new {@link ManifestWriter} for the given format version.
   *
   * @param formatVersion a target format version
   * @param spec a {@link PartitionSpec}
   * @param outputFile an {@link OutputFile} where the manifest will be written
   * @param snapshotId a snapshot ID for the manifest entries, or null for an inherited ID
   * @return a manifest writer
   */
  public static ManifestWriter<DataFile> write(int formatVersion, PartitionSpec spec, OutputFile outputFile,
                                               Long snapshotId) {
    return write(formatVersion, spec, outputFile, snapshotId, null, false);
  }

  public static ManifestWriter<DataFile> write(int formatVersion, PartitionSpec spec, OutputFile outputFile,
                                               Long snapshotId, String tableLocation, boolean shouldUseRelativePaths) {
    switch (formatVersion) {
      case 1:
        return new ManifestWriter.V1Writer(spec, outputFile, snapshotId, tableLocation, shouldUseRelativePaths);
      case 2:
        return new ManifestWriter.V2Writer(spec, outputFile, snapshotId, tableLocation, shouldUseRelativePaths);
    }
    throw new UnsupportedOperationException("Cannot write manifest for table version: " + formatVersion);
  }

  /**
   * Returns a new {@link ManifestReader} for a {@link ManifestFile}.
   *
   * @param manifest a {@link ManifestFile}
   * @param io a {@link FileIO}
   * @param specsById a Map from spec ID to partition spec
   * @return a {@link ManifestReader}
   */
  public static ManifestReader<DeleteFile> readDeleteManifest(ManifestFile manifest, FileIO io,
                                                              Map<Integer, PartitionSpec> specsById) {
    return readDeleteManifest(manifest, io, specsById, null, false);
  }

  public static ManifestReader<DeleteFile> readDeleteManifest(ManifestFile manifest, FileIO io,
                                                              Map<Integer, PartitionSpec> specsById,
                                                              String tableLocation,
                                                              boolean shouldUseRelativePaths) {
    Preconditions.checkArgument(manifest.content() == ManifestContent.DELETES,
        "Cannot read a data manifest with a DeleteManifestReader: %s", manifest);
    InputFile file = io.newInputFile(updateManifestPathIfNecessary(manifest, tableLocation, shouldUseRelativePaths));
    InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest, tableLocation,
        shouldUseRelativePaths);
    return new ManifestReader<>(file, specsById, inheritableMetadata, FileType.DELETE_FILES);
  }

  /**
   * Create a new {@link ManifestWriter} for the given format version.
   *
   * @param formatVersion a target format version
   * @param spec a {@link PartitionSpec}
   * @param outputFile an {@link OutputFile} where the manifest will be written
   * @param snapshotId a snapshot ID for the manifest entries, or null for an inherited ID
   * @return a manifest writer
   */
  public static ManifestWriter<DeleteFile> writeDeleteManifest(int formatVersion, PartitionSpec spec,
                                                               OutputFile outputFile, Long snapshotId) {
    return writeDeleteManifest(formatVersion, spec, outputFile, snapshotId, null, false);
  }
  public static ManifestWriter<DeleteFile> writeDeleteManifest(int formatVersion, PartitionSpec spec,
                                                               OutputFile outputFile, Long snapshotId,
                                                               String tableLocation, boolean shouldUseRelativePaths) {
    switch (formatVersion) {
      case 1:
        throw new IllegalArgumentException("Cannot write delete files in a v1 table");
      case 2:
        return new ManifestWriter.V2DeleteWriter(spec, outputFile, snapshotId, tableLocation, shouldUseRelativePaths);
    }
    throw new UnsupportedOperationException("Cannot write manifest for table version: " + formatVersion);
  }

  /**
   * Encode the {@link ManifestFile} to a byte array by using avro encoder.
   *
   * @param manifestFile a {@link ManifestFile}, which should always be a {@link GenericManifestFile}.
   * @return the binary data.
   * @throws IOException if encounter any IO error when encoding.
   */
  public static byte[] encode(ManifestFile manifestFile) throws IOException {
    GenericManifestFile genericManifestFile = (GenericManifestFile) manifestFile;
    return AvroEncoderUtil.encode(genericManifestFile, MANIFEST_AVRO_SCHEMA);
  }

  /**
   * Decode the binary data into a {@link ManifestFile}.
   *
   * @param manifestData the binary data.
   * @return a {@link ManifestFile}. To be precise, it's a {@link GenericManifestFile} which don't expose to public.
   * @throws IOException if encounter any IO error when decoding.
   */
  public static ManifestFile decode(byte[] manifestData) throws IOException {
    return AvroEncoderUtil.decode(manifestData);
  }

  static ManifestReader<?> open(ManifestFile manifest, FileIO io) {
    return open(manifest, io, null);
  }

  static ManifestReader<?> open(ManifestFile manifest, FileIO io,
                                Map<Integer, PartitionSpec> specsById) {
    switch (manifest.content()) {
      case DATA:
        return ManifestFiles.read(manifest, io, specsById);
      case DELETES:
        return ManifestFiles.readDeleteManifest(manifest, io, specsById);
    }
    throw new UnsupportedOperationException("Cannot read unknown manifest type: " + manifest.content());
  }

  static ManifestFile copyAppendManifest(int formatVersion,
      InputFile toCopy, Map<Integer, PartitionSpec> specsById,
      OutputFile outputFile, long snapshotId,
      SnapshotSummary.Builder summaryBuilder) {
    return copyAppendManifest(formatVersion, toCopy, specsById, outputFile, snapshotId, summaryBuilder, null, false);
  }

  static ManifestFile copyAppendManifest(int formatVersion,
                                         InputFile toCopy, Map<Integer, PartitionSpec> specsById,
                                         OutputFile outputFile, long snapshotId,
                                         SnapshotSummary.Builder summaryBuilder,
                                         String tableLocation, boolean shouldUseRelativePaths) {
    // use metadata that will add the current snapshot's ID for the rewrite
    InheritableMetadata inheritableMetadata = InheritableMetadataFactory.forCopy(snapshotId);
    try (ManifestReader<DataFile> reader =
             new ManifestReader<>(toCopy, specsById, inheritableMetadata, FileType.DATA_FILES)) {
      return copyManifestInternal(
          formatVersion, reader, outputFile, snapshotId, summaryBuilder, ManifestEntry.Status.ADDED,
          tableLocation, shouldUseRelativePaths);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", toCopy.location());
    }
  }

  static ManifestFile copyRewriteManifest(int formatVersion,
      InputFile toCopy, Map<Integer, PartitionSpec> specsById,
      OutputFile outputFile, long snapshotId,
      SnapshotSummary.Builder summaryBuilder) {
    return copyRewriteManifest(formatVersion, toCopy, specsById, outputFile, snapshotId, summaryBuilder, null, false);
  }

  static ManifestFile copyRewriteManifest(int formatVersion,
                                          InputFile toCopy, Map<Integer, PartitionSpec> specsById,
                                          OutputFile outputFile, long snapshotId,
                                          SnapshotSummary.Builder summaryBuilder,
                                          String tableLocation, boolean shouldUseRelativePaths) {
    // for a rewritten manifest all snapshot ids should be set. use empty metadata to throw an exception if it is not
    InheritableMetadata inheritableMetadata = InheritableMetadataFactory.empty();
    try (ManifestReader<DataFile> reader =
             new ManifestReader<>(toCopy, specsById, inheritableMetadata, FileType.DATA_FILES)) {
      return copyManifestInternal(
          formatVersion, reader, outputFile, snapshotId, summaryBuilder, ManifestEntry.Status.EXISTING,
          tableLocation, shouldUseRelativePaths);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", toCopy.location());
    }
  }

  private static ManifestFile copyManifestInternal(int formatVersion, ManifestReader<DataFile> reader,
                                                   OutputFile outputFile, long snapshotId,
                                                   SnapshotSummary.Builder summaryBuilder,
                                                   ManifestEntry.Status allowedEntryStatus,
                                                   String tableLocation, boolean shouldUseRelativePaths) {
    ManifestWriter<DataFile> writer = write(formatVersion, reader.spec(), outputFile, snapshotId,
        tableLocation, shouldUseRelativePaths);
    boolean threw = true;
    try {
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        Preconditions.checkArgument(
            allowedEntryStatus == entry.status(),
            "Invalid manifest entry status: %s (allowed status: %s)",
            entry.status(), allowedEntryStatus);
        switch (entry.status()) {
          case ADDED:
            summaryBuilder.addedFile(reader.spec(), entry.file());
            writer.add(entry);
            break;
          case EXISTING:
            writer.existing(entry);
            break;
          case DELETED:
            summaryBuilder.deletedFile(reader.spec(), entry.file());
            writer.delete(entry);
            break;
        }
      }

      threw = false;

    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        if (!threw) {
          throw new RuntimeIOException(e, "Failed to close manifest: %s", outputFile);
        }
      }
    }

    return writer.toManifestFile();
  }
}
