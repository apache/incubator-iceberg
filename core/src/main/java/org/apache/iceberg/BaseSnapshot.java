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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseSnapshot implements Snapshot {
  private static final long INITIAL_SEQUENCE_NUMBER = 0;

  private final FileIO io;
  private final long snapshotId;
  private final Long parentId;
  private final long sequenceNumber;
  private final long timestampMillis;
  private final String manifestListLocation;
  private final String operation;
  private final Map<String, String> summary;
  private final String partitionStatsLocation;

  // lazily initialized
  private transient List<ManifestFile> allManifests = null;
  private transient List<ManifestFile> dataManifests = null;
  private transient List<ManifestFile> deleteManifests = null;
  private transient List<DataFile> cachedAdds = null;
  private transient List<DataFile> cachedDeletes = null;

  /**
   * For testing only.
   */
  BaseSnapshot(FileIO io,
               long snapshotId,
               String... manifestFiles) {
    this(io, snapshotId, null, System.currentTimeMillis(), null, null,
        Lists.transform(Arrays.asList(manifestFiles),
            path -> new GenericManifestFile(io.newInputFile(path), 0)));
  }

  BaseSnapshot(FileIO io,
               long sequenceNumber,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               String manifestList, String partitionStatsLocation) {
    this.io = io;
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.manifestListLocation = manifestList;
    this.partitionStatsLocation = partitionStatsLocation;
  }

  BaseSnapshot(FileIO io,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               List<ManifestFile> dataManifests) {
    this(io, INITIAL_SEQUENCE_NUMBER, snapshotId, parentId, timestampMillis, operation, summary, null, null);
    this.allManifests = dataManifests;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public Map<String, String> summary() {
    return summary;
  }

  private void cacheManifests() {
    if (allManifests == null) {
      // if manifests isn't set, then the snapshotFile is set and should be read to get the list
      this.allManifests = ManifestLists.read(io.newInputFile(manifestListLocation));
    }

    if (dataManifests == null || deleteManifests == null) {
      this.dataManifests = ImmutableList.copyOf(Iterables.filter(allManifests,
          manifest -> manifest.content() == ManifestContent.DATA));
      this.deleteManifests = ImmutableList.copyOf(Iterables.filter(allManifests,
          manifest -> manifest.content() == ManifestContent.DELETES));
    }
  }

  @Override
  public List<ManifestFile> allManifests() {
    if (allManifests == null) {
      cacheManifests();
    }
    return allManifests;
  }

  @Override
  public List<ManifestFile> dataManifests() {
    if (dataManifests == null) {
      cacheManifests();
    }
    return dataManifests;
  }

  @Override
  public List<ManifestFile> deleteManifests() {
    if (deleteManifests == null) {
      cacheManifests();
    }
    return deleteManifests;
  }

  @Override
  public List<DataFile> addedFiles() {
    if (cachedAdds == null) {
      cacheChanges();
    }
    return cachedAdds;
  }

  @Override
  public List<DataFile> deletedFiles() {
    if (cachedDeletes == null) {
      cacheChanges();
    }
    return cachedDeletes;
  }

  @Override
  public String manifestListLocation() {
    return manifestListLocation;
  }

  @Override
  public String partitionStatsLocation() {
    return partitionStatsLocation;
  }

  private void cacheChanges() {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedManifests = Iterables.filter(dataManifests(),
        manifest -> Objects.equal(manifest.snapshotId(), snapshotId));
    try (CloseableIterable<ManifestEntry<DataFile>> entries = new ManifestGroup(io, changedManifests)
        .ignoreExisting()
        .entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.file().copy());
            break;
          case DELETED:
            deletes.add(entry.file().copyWithoutStats());
            break;
          default:
            throw new IllegalStateException(
                "Unexpected entry status, not added or deleted: " + entry);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close entries while caching changes");
    }

    this.cachedAdds = adds.build();
    this.cachedDeletes = deletes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof BaseSnapshot) {
      BaseSnapshot other = (BaseSnapshot) o;
      return this.snapshotId == other.snapshotId() &&
          Objects.equal(this.parentId, other.parentId()) &&
          this.sequenceNumber == other.sequenceNumber() &&
          this.timestampMillis == other.timestampMillis();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
      this.snapshotId,
      this.parentId,
      this.sequenceNumber,
      this.timestampMillis
    );
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", snapshotId)
        .add("timestamp_ms", timestampMillis)
        .add("operation", operation)
        .add("summary", summary)
        .add("manifest-list", manifestListLocation)
        .add("partition-stats", partitionStatsLocation)
        .toString();
  }
}
