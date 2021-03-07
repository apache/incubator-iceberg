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

package org.apache.iceberg.nessie;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableIcebergTable;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/**
 * Nessie implementation of Iceberg TableOperations.
 */
public class NessieTableOperations extends BaseMetastoreTableOperations {

  private final NessieClient client;
  private final ContentsKey key;
  private RefreshableReference reference;
  private IcebergTable table;
  private FileIO fileIO;

  /**
   * Create a nessie table operations given a table identifier.
   */
  public NessieTableOperations(
      ContentsKey key,
      RefreshableReference reference,
      NessieClient client,
      FileIO fileIO) {
    this.key = key;
    this.reference = reference;
    this.client = client;
    this.fileIO = fileIO;
  }

  @Override
  protected void doRefresh() {
    try {
      reference.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to refresh as ref is no longer valid.", e);
    }
    String metadataLocation = null;
    try {
      Contents contents = client.getContentsApi().getContents(key, reference.getHash());
      this.table = contents.unwrap(IcebergTable.class)
          .orElseThrow(() ->
              new IllegalStateException("Cannot refresh iceberg table: " +
                  String.format("Nessie points to a non-Iceberg object for path: %s.", key)));
      metadataLocation = table.getMetadataLocation();
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such table %s", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    reference.checkMutable();

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean threw = true;
    try {
      IcebergTable newTable = ImmutableIcebergTable.builder().metadataLocation(newMetadataLocation).build();
      client.getContentsApi().setContents(key,
                                          reference.getAsBranch().getName(),
                                          reference.getHash(),
                                          String.format("iceberg commit%s", applicationId()),
                                          newTable);
      threw = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(ex, "Commit failed: Reference hash is out of date. " +
          "Update the reference %s and try again", reference.getName());
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(String.format("Commit failed: Reference %s no longer exist", reference.getName()), ex);
    } finally {
      if (threw) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  /**
   * try and get a Spark application id if one exists.
   *
   * <p>
   *   We haven't figured out a general way to pass commit messages through to the Nessie committer yet.
   *   This is hacky but gets the job done until we can have a more complete commit/audit log.
   * </p>
   */
  private String applicationId() {
    String appId = null;
    TableMetadata current = current();
    if (current != null) {
      Snapshot snapshot = current.currentSnapshot();
      if (snapshot != null) {
        Map<String, String> summary = snapshot.summary();
        appId = summary.get("spark.app.id");
      }

    }
    return appId == null ? "" : ("\nspark.app.id= " + appId);
  }

}
