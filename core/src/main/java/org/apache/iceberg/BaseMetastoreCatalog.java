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

import com.google.common.collect.Maps;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;

public abstract class BaseMetastoreCatalog implements Catalog {
  enum TableType {
    ENTRIES,
    HISTORY,
    SNAPSHOTS,
    MANIFESTS,
    PARTITIONS;

    static TableType from(String name) {
      try {
        return TableType.valueOf(name.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException ignored) {
        return null;
      }
    }
  }

  private final Configuration conf;

  protected BaseMetastoreCatalog(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    TableOperations ops = newTableOps(conf, identifier);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + identifier);
    }

    String baseLocation;
    if (location != null) {
      baseLocation = location;
    } else {
      baseLocation = defaultWarehouseLocation(conf, identifier);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(
        ops, schema, spec, baseLocation, properties == null ? Maps.newHashMap() : properties);

    ops.commit(null, metadata);

    try {
      return new BaseTable(ops, identifier.toString());
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException("Table was created concurrently: " + identifier);
    }
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    TableOperations ops = newTableOps(conf, identifier);
    if (ops.current() == null) {
      String name = identifier.name();
      TableType type = TableType.from(name);
      if (type != null) {
        return loadMetadataTable(TableIdentifier.of(identifier.namespace().levels()), type);
      } else {
        throw new NoSuchTableException("Table does not exist: " + identifier);
      }
    }

    return new BaseTable(ops, identifier.toString());
  }

  private Table loadMetadataTable(TableIdentifier identifier, TableType type) {
    TableOperations ops = newTableOps(conf, identifier);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: " + identifier);
    }

    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(ops, new BaseTable(ops, identifier.toString()));
      case HISTORY:
      case SNAPSHOTS:
      case MANIFESTS:
      case PARTITIONS:
      default:
        throw new NoSuchTableException(String.format("Unknown metadata table type: %s for %s", type, identifier));
    }
  }

  protected abstract TableOperations newTableOps(Configuration newConf, TableIdentifier tableIdentifier);

  protected abstract String defaultWarehouseLocation(Configuration hadoopConf, TableIdentifier tableIdentifier);
}
