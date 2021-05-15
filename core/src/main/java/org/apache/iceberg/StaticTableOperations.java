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
 *
 */

package org.apache.iceberg;

import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionManagers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * TableOperations implementation that provides access to metadata for a Table at some point in time, using a
 * table metadata location. It will never refer to a different Metadata object than the one it was created with
 * and cannot be used to create or delete files.
 */
public class StaticTableOperations implements TableOperations {
  private TableMetadata staticMetadata;
  private final String metadataFileLocation;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final LocationProvider locationProvider;

  /**
   * @deprecated please use {@link #StaticTableOperations(String, FileIO, EncryptionManager)}
   */
  @Deprecated
  public StaticTableOperations(String metadataFileLocation, FileIO io) {
    this(metadataFileLocation, io, EncryptionManagers.plainText());
  }

  /**
   * Creates a StaticTableOperations tied to a specific static version of the TableMetadata
   */
  public StaticTableOperations(String metadataFileLocation, FileIO io, EncryptionManager encryption) {
    this(metadataFileLocation, io, encryption, null);
  }

  public StaticTableOperations(String metadataFileLocation, FileIO io, EncryptionManager encryption,
                               LocationProvider locationProvider) {
    this.io = io;
    this.encryption = encryption;
    this.metadataFileLocation = metadataFileLocation;
    this.locationProvider = locationProvider;
  }

  @Override
  public TableMetadata current() {
    if (staticMetadata == null) {
      staticMetadata = TableMetadataParser.read(io, encryption, metadataFileLocation);
    }
    return staticMetadata;
  }

  /**
   * StaticTableOperations works on the same version of TableMetadata, and it will never refer a different TableMetadata
   * object than the one it was created with.
   * @return always {@link #current()}.
   */
  @Override
  public TableMetadata refresh() {
    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Cannot modify a static table");
  }

  @Override
  public FileIO io() {
    return this.io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    throw new UnsupportedOperationException("Cannot modify a static table");
  }

  @Override
  public LocationProvider locationProvider() {
    return locationProvider;
  }
}
