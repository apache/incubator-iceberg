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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Writer for manifest files.
 *
 * @param <F> Java class of files written to the manifest, either {@link DataFile} or {@link DeleteFile}.
 */
public abstract class ManifestWriter<F extends ContentFile<F>> implements FileAppender<F> {
  // stand-in for the current sequence number that will be assigned when the commit is successful
  // this is replaced when writing a manifest list by the ManifestFile wrapper
  static final long UNASSIGNED_SEQ = -1L;

  private final OutputFile file;
  private final int specId;
  private final FileAppender<ManifestEntry<F>> writer;
  private final Long snapshotId;
  private final GenericManifestEntry<F> reused;
  private final PartitionSummary stats;
  private final String tableLocation;
  private final boolean shouldUseRelativePaths;
  private boolean closed = false;
  private int addedFiles = 0;
  private long addedRows = 0L;
  private int existingFiles = 0;
  private long existingRows = 0L;
  private int deletedFiles = 0;
  private long deletedRows = 0L;
  private Long minSequenceNumber = null;

  private ManifestWriter(PartitionSpec spec, OutputFile file, Long snapshotId,
      String tableLocation, boolean shouldUseRelativePaths) {
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(spec, file);
    this.snapshotId = snapshotId;
    this.reused = new GenericManifestEntry<>(spec.partitionType());
    this.stats = new PartitionSummary(spec);
    this.tableLocation = tableLocation;
    this.shouldUseRelativePaths = shouldUseRelativePaths;
  }

  protected abstract ManifestEntry<F> prepare(ManifestEntry<F> entry);

  protected abstract FileAppender<ManifestEntry<F>> newAppender(PartitionSpec spec, OutputFile outputFile);

  protected ManifestContent content() {
    return ManifestContent.DATA;
  }

  void addEntry(ManifestEntry<F> entry) {
    switch (entry.status()) {
      case ADDED:
        addedFiles += 1;
        addedRows += entry.file().recordCount();
        break;
      case EXISTING:
        existingFiles += 1;
        existingRows += entry.file().recordCount();
        break;
      case DELETED:
        deletedFiles += 1;
        deletedRows += entry.file().recordCount();
        break;
    }
    stats.update(entry.file().partition());
    if (entry.sequenceNumber() != null && (minSequenceNumber == null || entry.sequenceNumber() < minSequenceNumber)) {
      this.minSequenceNumber = entry.sequenceNumber();
    }
    if (entry.file() instanceof BaseFile) {
      BaseFile<?> entryFile = (BaseFile<?>) entry.file();
      // Update path to use relative location if needed.
      entryFile.setFilePath(MetadataPathUtils.toRelativePath(entryFile.path().toString(),
          tableLocation, shouldUseRelativePaths));
    }
    writer.add(prepare(entry));
  }

  /**
   * Add an added entry for a file.
   * <p>
   * The entry's snapshot ID will be this manifest's snapshot ID.
   *
   * @param addedFile a data file
   */
  @Override
  public void add(F addedFile) {
    addEntry(reused.wrapAppend(snapshotId, addedFile));
  }

  void add(ManifestEntry<F> entry) {
    addEntry(reused.wrapAppend(snapshotId, entry.file()));
  }

  /**
   * Add an existing entry for a file.
   *
   * @param existingFile a file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   * @param sequenceNumber sequence number for the data file
   */
  public void existing(F existingFile, long fileSnapshotId, long sequenceNumber) {
    addEntry(reused.wrapExisting(fileSnapshotId, sequenceNumber, existingFile));
  }

  void existing(ManifestEntry<F> entry) {
    addEntry(reused.wrapExisting(entry.snapshotId(), entry.sequenceNumber(), entry.file()));
  }

  /**
   * Add a delete entry for a file.
   * <p>
   * The entry's snapshot ID will be this manifest's snapshot ID.
   *
   * @param deletedFile a file
   */
  public void delete(F deletedFile) {
    addEntry(reused.wrapDelete(snapshotId, deletedFile));
  }

  void delete(ManifestEntry<F> entry) {
    // Use the current Snapshot ID for the delete. It is safe to delete the data file from disk
    // when this Snapshot has been removed or when there are no Snapshots older than this one.
    addEntry(reused.wrapDelete(snapshotId, entry.file()));
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public long length() {
    return writer.length();
  }

  public ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    // if the minSequenceNumber is null, then no manifests with a sequence number have been written, so the min
    // sequence number is the one that will be assigned when this is committed. pass UNASSIGNED_SEQ to inherit it.
    long minSeqNumber = minSequenceNumber != null ? minSequenceNumber : UNASSIGNED_SEQ;
    return new GenericManifestFile(file.location(), writer.length(), specId, content(),
        UNASSIGNED_SEQ, minSeqNumber, snapshotId,
        addedFiles, addedRows, existingFiles, existingRows, deletedFiles, deletedRows, stats.summaries(), null);
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  static class V2Writer extends ManifestWriter<DataFile> {
    private final V2Metadata.IndexedManifestEntry<DataFile> entryWrapper;

    V2Writer(PartitionSpec spec, OutputFile file, Long snapshotId,
        String tableLocation, boolean shouldUseRelativePaths) {
      super(spec, file, snapshotId, tableLocation, shouldUseRelativePaths);
      this.entryWrapper = new V2Metadata.IndexedManifestEntry<>(snapshotId, spec.partitionType());
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return Avro.write(file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "data")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: %s", file);
      }
    }
  }

  static class V2DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V2Metadata.IndexedManifestEntry<DeleteFile> entryWrapper;

    V2DeleteWriter(PartitionSpec spec, OutputFile file, Long snapshotId,
        String tableLocation, boolean shouldUseRelativePaths) {
      super(spec, file, snapshotId, tableLocation, shouldUseRelativePaths);
      this.entryWrapper = new V2Metadata.IndexedManifestEntry<>(snapshotId, spec.partitionType());
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return Avro.write(file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "deletes")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: %s", file);
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V1Writer extends ManifestWriter<DataFile> {
    private final V1Metadata.IndexedManifestEntry entryWrapper;

    V1Writer(PartitionSpec spec, OutputFile file, Long snapshotId,
        String tableLocation, boolean shouldUseRelativePaths) {
      super(spec, file, snapshotId, tableLocation, shouldUseRelativePaths);
      this.entryWrapper = new V1Metadata.IndexedManifestEntry(spec.partitionType());
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V1Metadata.entrySchema(spec.partitionType());
      try {
        return Avro.write(file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "1")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: %s", file);
      }
    }
  }
}
