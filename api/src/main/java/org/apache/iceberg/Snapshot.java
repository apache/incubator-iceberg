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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A snapshot of the data in a table at a point in time.
 * <p>
 * A snapshot consist of one or more file manifests, and the complete table contents is the union
 * of all the data files in those manifests.
 * <p>
 * Snapshots are created by table operations, like {@link AppendFiles} and {@link RewriteFiles}.
 */
public interface Snapshot extends Serializable {
  /**
   * Return this snapshot's sequence number.
   * <p>
   * Sequence numbers are assigned when a snapshot is committed.
   *
   * @return a long sequence number
   */
  long sequenceNumber();

  /**
   * Return this snapshot's ID.
   *
   * @return a long ID
   */
  long snapshotId();

  /**
   * Return this snapshot's parent ID or null.
   *
   * @return a long ID for this snapshot's parent, or null if it has no parent
   */
  Long parentId();

  /**
   * Return this snapshot's timestamp.
   * <p>
   * This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   *
   * @return a long timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Return all {@link ManifestFile} instances for either data or delete manifests in this snapshot.
   *
   * @return a list of ManifestFile
   */
  List<ManifestFile> allManifests();

  /**
   * Return a {@link ManifestFile} for each data manifest in this snapshot.
   *
   * @return a list of ManifestFile
   */
  List<ManifestFile> dataManifests();

  /**
   * Return a {@link ManifestFile} for each delete manifest in this snapshot.
   *
   * @return a list of ManifestFile
   */
  List<ManifestFile> deleteManifests();

  /**
   * Return the name of the {@link DataOperations data operation} that produced this snapshot.
   *
   * @return the operation that produced this snapshot, or null if the operation is unknown
   * @see DataOperations
   */
  String operation();

  /**
   * Return a string map of summary data for the operation that produced this snapshot.
   *
   * @return a string map of summary data.
   */
  Map<String, String> summary();

  /**
   * Return all files added to the table in this snapshot.
   * <p>
   * The files returned include the following columns: file_path, file_format, partition,
   * record_count, and file_size_in_bytes. Other columns will be null.
   *
   * @return all files added to the table in this snapshot.
   */
  Iterable<DataFile> addedFiles();

  /**
   * Return all files deleted from the table in this snapshot.
   * <p>
   * The files returned include the following columns: file_path, file_format, partition,
   * record_count, and file_size_in_bytes. Other columns will be null.
   *
   * @return all files deleted from the table in this snapshot.
   */
  Iterable<DataFile> deletedFiles();

  /**
   * Return the location of this snapshot's manifest list, or null if it is not separate.
   *
   * @return the location of the manifest list for this Snapshot
   */
  String manifestListLocation();

  /**
   * Return the location of partition stats file, or null if doesn't exist.
   *
   * @return the location of Partition stats file for this Snapshot
   */
  String partitionStatsLocation();
}
