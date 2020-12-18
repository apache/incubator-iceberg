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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class ManifestLists {
  private ManifestLists() {
  }

  static List<ManifestFile> read(InputFile manifestList) {
    return read(manifestList, null, null);
  }

  static List<ManifestFile> read(InputFile manifestList, String tableLocation, Map<String, String> tableProperties) {
    try (CloseableIterable<ManifestFile> files = Avro.read(manifestList)
        .rename("manifest_file", GenericManifestFile.class.getName())
        .rename("partitions", GenericPartitionFieldSummary.class.getName())
        .rename("r508", GenericPartitionFieldSummary.class.getName())
        .classLoader(GenericManifestFile.class.getClassLoader())
        .project(ManifestFile.schema())
        .reuseContainers(false)
        .build()) {

      List<ManifestFile> manifestFiles = Lists.newLinkedList(files);
      List<ManifestFile> updatedManifestFiles = Lists.newArrayListWithCapacity(manifestFiles.size());
      if (MetadataPaths.useRelativePath(tableProperties)) {
        for (ManifestFile manifestFile : manifestFiles) {
          updatedManifestFiles.add(GenericManifestFile.copyOf(manifestFile)
              .withManifestPath(MetadataPaths.toAbsolutePath(manifestFile.path(),
              tableLocation, tableProperties)).build());
        }
      } else {
        updatedManifestFiles.addAll(manifestFiles);
      }
      return updatedManifestFiles;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Cannot read manifest list file: %s", manifestList.location());
    }
  }

  static ManifestListWriter write(int formatVersion, OutputFile manifestListFile,
                                  long snapshotId, Long parentSnapshotId, long sequenceNumber, String tableLocation,
      Map<String, String> tableProperties) {
    switch (formatVersion) {
      case 1:
        Preconditions.checkArgument(sequenceNumber == TableMetadata.INITIAL_SEQUENCE_NUMBER,
            "Invalid sequence number for v1 manifest list: %s", sequenceNumber);
        return new ManifestListWriter.V1Writer(manifestListFile, snapshotId, parentSnapshotId, tableLocation,
            tableProperties);
      case 2:
        return new ManifestListWriter.V2Writer(manifestListFile, snapshotId, parentSnapshotId, sequenceNumber,
            tableLocation, tableProperties);
    }
    throw new UnsupportedOperationException("Cannot write manifest list for table version: " + formatVersion);
  }
}
