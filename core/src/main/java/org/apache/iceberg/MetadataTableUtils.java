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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class MetadataTableUtils {
  private MetadataTableUtils() {
  }

  public static boolean hasMetadataTableName(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null;
  }

  public static Table createMetadataTableInstance(TableOperations ops,
                                                  String baseTableName,
                                                  String metadataTableName,
                                                  MetadataTableType type) {
    Table baseTable = new BaseTable(ops, baseTableName);
    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(ops, baseTable, metadataTableName);
      case FILES:
        return new DataFilesTable(ops, baseTable, metadataTableName);
      case HISTORY:
        return new HistoryTable(ops, baseTable, metadataTableName);
      case SNAPSHOTS:
        return new SnapshotsTable(ops, baseTable, metadataTableName);
      case MANIFESTS:
        return new ManifestsTable(ops, baseTable, metadataTableName);
      case PARTITIONS:
        return new PartitionsTable(ops, baseTable, metadataTableName);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(ops, baseTable, metadataTableName);
      case ALL_MANIFESTS:
        return new AllManifestsTable(ops, baseTable, metadataTableName);
      case ALL_ENTRIES:
        return new AllEntriesTable(ops, baseTable, metadataTableName);
      default:
        throw new NoSuchTableException("Unknown metadata table type: %s for %s", type, metadataTableName);
    }
  }

  public static Table createMetadataTableInstance(TableOperations ops,
                                                  String catalogName,
                                                  TableIdentifier baseTableIdentifier,
                                                  TableIdentifier metadataTableIdentifier,
                                                  MetadataTableType type) {
    String baseTableName = BaseMetastoreCatalog.fullTableName(catalogName, baseTableIdentifier);
    String metadataTableName = BaseMetastoreCatalog.fullTableName(catalogName, metadataTableIdentifier);
    return createMetadataTableInstance(ops, baseTableName, metadataTableName, type);
  }
}
