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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class InheritableMetadataFactory {

  private static final InheritableMetadata EMPTY = new EmptyInheritableMetadata();

  private InheritableMetadataFactory() {
  }

  static InheritableMetadata empty() {
    return EMPTY;
  }

  static InheritableMetadata fromManifest(ManifestFile manifest, String tableLocation,
      Map<String, String> tableProperties) {
    Preconditions.checkArgument(manifest.snapshotId() != null,
        "Cannot read from ManifestFile with null (unassigned) snapshot ID");
    return new BaseInheritableMetadata(manifest.partitionSpecId(), manifest.snapshotId(), manifest.sequenceNumber(),
        tableLocation, tableProperties);
  }

  static InheritableMetadata forCopy(long snapshotId) {
    return new CopyMetadata(snapshotId);
  }

  static class BaseInheritableMetadata implements InheritableMetadata {
    private final int specId;
    private final long snapshotId;
    private final long sequenceNumber;
    private final String tableLocation;
    private final Map<String, String> tableProperties;

    private BaseInheritableMetadata(int specId, long snapshotId, long sequenceNumber, String tableLocation,
        Map<String, String> tableProperties) {
      this.specId = specId;
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.tableLocation = tableLocation;
      this.tableProperties = tableProperties;
    }

    @Override
    public <F extends ContentFile<F>> ManifestEntry<F> apply(ManifestEntry<F> manifestEntry) {
      if (manifestEntry.file() instanceof BaseFile) {
        BaseFile<?> file = (BaseFile<?>) manifestEntry.file();
        file.setSpecId(specId);
        if (MetadataPaths.useRelativePath(tableProperties)) {
          if (!file.path().toString().startsWith(tableLocation)) {
            file.setFilePath(MetadataPaths.toAbsolutePath(file.path().toString(), tableLocation, tableProperties));
          }
        }
      }
      if (manifestEntry.snapshotId() == null) {
        manifestEntry.setSnapshotId(snapshotId);
      }
      if (manifestEntry.sequenceNumber() == null) {
        manifestEntry.setSequenceNumber(sequenceNumber);
      }
      return manifestEntry;
    }
  }

  static class CopyMetadata implements InheritableMetadata {
    private final long snapshotId;

    private CopyMetadata(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    @Override
    public <F extends ContentFile<F>> ManifestEntry<F> apply(ManifestEntry<F> manifestEntry) {
      manifestEntry.setSnapshotId(snapshotId);
      return manifestEntry;
    }
  }

  static class EmptyInheritableMetadata implements InheritableMetadata {

    private EmptyInheritableMetadata() {
    }

    @Override
    public <F extends ContentFile<F>> ManifestEntry<F> apply(ManifestEntry<F> manifestEntry) {
      if (manifestEntry.snapshotId() == null) {
        throw new IllegalArgumentException("Entries must have explicit snapshot ids if inherited metadata is empty");
      }
      return manifestEntry;
    }
  }
}
