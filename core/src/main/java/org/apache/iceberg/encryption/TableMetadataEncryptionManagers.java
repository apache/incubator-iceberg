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

package org.apache.iceberg.encryption;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to get {@link TableMetadataEncryptionManager} instance.
 */
public class TableMetadataEncryptionManagers {

  private static final Logger LOG = LoggerFactory.getLogger(TableMetadataEncryptionManagers.class);

  private static final TableMetadataEncryptionManager PLAINTEXT_TABLE_METADATA_ENCRYPTION_MANAGER =
      new PlaintextTableMetadataEncryptionManager();

  private TableMetadataEncryptionManagers() {
  }

  /**
   * Returns a default plaintext encryption manager.
   */
  public static TableMetadataEncryptionManager plainText() {
    return PLAINTEXT_TABLE_METADATA_ENCRYPTION_MANAGER;
  }

  /**
   * Load a {@link TableMetadataEncryptionManager} implementation.
   * <p>
   * A custom implementation can be loaded through {@link CatalogProperties#ENCRYPTION_TABLE_METADATA_MANAGER_IMPL}.
   * If custom implementation is not set, default plaintext manager is used.
   * A custom implementation must have a no-arg constructor, and implement method
   * {@link TableMetadataEncryptionManager#initialize(Map)} to initialize from catalog properties.
   *
   * @param properties catalog properties
   * @return TableMetadataEncryptionManager implementation
   * @throws IllegalArgumentException if class path not found or
   *  right constructor not found or
   *  the loaded class cannot be casted to the given interface type
   */
  public static TableMetadataEncryptionManager load(Map<String, String> properties) {
    String impl = properties.get(CatalogProperties.ENCRYPTION_TABLE_METADATA_MANAGER_IMPL);
    if (impl == null) {
      return plainText();
    }

    LOG.info("Loading custom TableMetadataEncryptionManager implementation: {}", impl);
    DynConstructors.Ctor<TableMetadataEncryptionManager> ctor;
    try {
      ctor = DynConstructors.builder(TableMetadataEncryptionManager.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize TableMetadataEncryptionManager, missing no-arg constructor: %s", impl), e);
    }

    TableMetadataEncryptionManager tableMetadataEncryptionManager;
    try {
      tableMetadataEncryptionManager = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize TableMetadataEncryptionManager, " +
              "%s does not implement TableMetadataEncryptionManager.", impl), e);
    }

    tableMetadataEncryptionManager.initialize(properties);
    return tableMetadataEncryptionManager;
  }

  static class PlaintextTableMetadataEncryptionManager implements TableMetadataEncryptionManager {

    @Override
    public InputFile decrypt(InputFile encrypted) {
      return encrypted;
    }

    @Override
    public OutputFile encrypt(OutputFile rawOutput) {
      return rawOutput;
    }
  }
}
