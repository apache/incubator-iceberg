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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableMap;

/**
 * A read-only serializable table that can be sent to other nodes in a cluster.
 * <p>
 * An instance of this class represents an immutable serializable copy of a table state and
 * will not reflect any subsequent changed made to the original table.
 * <p>
 * While this class captures the metadata file location that can be used to load the complete
 * table metadata, it directly persists the current schema, spec, sort order, table properties
 * to avoid reading the metadata file from other nodes for frequently needed metadata.
 * <p>
 * The implementation assumes the passed instances of {@link FileIO}, {@link EncryptionManager},
 * {@link LocationProvider} are serializable. If you are serializing the table using a custom
 * serialization framework like Kryo, those instances of {@link FileIO}, {@link EncryptionManager},
 * {@link LocationProvider} must be supported by that particular serialization framework.
 * <p>
 * <em>Note:</em> loading the complete metadata from a large number of nodes can overwhelm the storage.
 */
public class SerializableTable implements Table, Serializable {

  private final String name;
  private final String location;
  private final String metadataFileLocation;
  private final Map<String, String> properties;
  private final String schemaAsJson;
  private final String specAsJson;
  private final String sortOrderAsJson;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final LocationProvider locationProvider;

  private transient volatile Table lazyTable = null;
  private transient volatile Schema lazySchema = null;
  private transient volatile PartitionSpec lazySpec = null;
  private transient volatile SortOrder lazySortOrder = null;

  private SerializableTable(Table table) {
    this.name = table.name();
    this.location = table.location();
    this.metadataFileLocation = metadataFileLocation(table);
    this.properties = SerializableMap.copyOf(table.properties());
    this.schemaAsJson = SchemaParser.toJson(table.schema());
    this.specAsJson = PartitionSpecParser.toJson(table.spec());
    this.sortOrderAsJson = SortOrderParser.toJson(table.sortOrder());
    this.io = fileIO(table);
    this.encryption = table.encryption();
    this.locationProvider = table.locationProvider();
  }

  /**
   * Creates a read-only serializable table that can be sent to other nodes in a cluster.
   *
   * @param table the original table to copy the state from
   * @return a read-only serializable table reflecting the current state of the original table
   */
  public static Table copyOf(Table table) {
    if (table instanceof BaseMetadataTable) {
      return new SerializableMetadataTable((BaseMetadataTable) table);
    } else {
      return new SerializableTable(table);
    }
  }

  private String metadataFileLocation(Table table) {
    if (table instanceof HasTableOperations) {
      TableOperations ops = ((HasTableOperations) table).operations();
      return ops.current().metadataFileLocation();
    } else {
      return null;
    }
  }

  private FileIO fileIO(Table table) {
    if (table.io() instanceof HadoopFileIO) {
      HadoopFileIO hadoopFileIO = (HadoopFileIO) table.io();
      SerializableConfiguration serializedConf = new SerializableConfiguration(hadoopFileIO.getConf());
      return new HadoopFileIO(serializedConf::get);
    } else {
      return table.io();
    }
  }

  private Table lazyTable() {
    if (lazyTable == null) {
      synchronized (this) {
        if (lazyTable == null) {
          if (metadataFileLocation == null) {
            throw new UnsupportedOperationException("Cannot load metadata: metadata file location is null");
          }

          TableOperations ops = new StaticTableOperations(metadataFileLocation, io, encryption, locationProvider);
          this.lazyTable = newTable(ops, name);
        }
      }
    }

    return lazyTable;
  }

  protected Table newTable(TableOperations ops, String tableName) {
    return new BaseTable(ops, tableName);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Schema schema() {
    if (lazySchema == null) {
      synchronized (this) {
        if (lazySchema == null && lazyTable == null) {
          // prefer parsing JSON as opposed to loading the metadata
          this.lazySchema = SchemaParser.fromJson(schemaAsJson);
        } else if (lazySchema == null) {
          this.lazySchema = lazyTable.schema();
        }
      }
    }

    return lazySchema;
  }

  @Override
  public PartitionSpec spec() {
    if (lazySpec == null) {
      synchronized (this) {
        if (lazySpec == null && lazyTable == null) {
          // prefer parsing JSON as opposed to loading the metadata
          this.lazySpec = PartitionSpecParser.fromJson(schema(), specAsJson);
        } else if (lazySpec == null) {
          this.lazySpec = lazyTable.spec();
        }
      }
    }

    return lazySpec;
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return lazyTable().specs();
  }

  @Override
  public SortOrder sortOrder() {
    if (lazySortOrder == null) {
      synchronized (this) {
        if (lazySortOrder == null && lazyTable == null) {
          // prefer parsing JSON as opposed to loading the metadata
          this.lazySortOrder = SortOrderParser.fromJson(schema(), sortOrderAsJson);
        } else if (lazySortOrder == null) {
          this.lazySortOrder = lazyTable.sortOrder();
        }
      }
    }

    return lazySortOrder;
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return lazyTable().sortOrders();
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  public EncryptionManager encryption() {
    return encryption;
  }

  @Override
  public LocationProvider locationProvider() {
    return locationProvider;
  }

  @Override
  public void refresh() {
    throw new UnsupportedOperationException(errorMsg("refresh"));
  }

  @Override
  public TableScan newScan() {
    return lazyTable().newScan();
  }

  @Override
  public Snapshot currentSnapshot() {
    return lazyTable().currentSnapshot();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return lazyTable().snapshot(snapshotId);
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return lazyTable().snapshots();
  }

  @Override
  public List<HistoryEntry> history() {
    return lazyTable().history();
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException(errorMsg("updateSchema"));
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    throw new UnsupportedOperationException(errorMsg("updateSpec"));
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException(errorMsg("updateProperties"));
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    throw new UnsupportedOperationException(errorMsg("replaceSortOrder"));
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException(errorMsg("updateLocation"));
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException(errorMsg("newAppend"));
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException(errorMsg("newRewrite"));
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException(errorMsg("rewriteManifests"));
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException(errorMsg("newOverwrite"));
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException(errorMsg("newRowDelta"));
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException(errorMsg("newReplacePartitions"));
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException(errorMsg("newDelete"));
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException(errorMsg("expireSnapshots"));
  }

  @Override
  public Rollback rollback() {
    throw new UnsupportedOperationException(errorMsg("rollback"));
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException(errorMsg("manageSnapshots"));
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException(errorMsg("newTransaction"));
  }

  private String errorMsg(String operation) {
    return String.format("Operation %s is not supported after the table is serialized", operation);
  }

  private static class SerializableMetadataTable extends SerializableTable {
    private final MetadataTableType type;
    private final String baseTableName;

    SerializableMetadataTable(BaseMetadataTable metadataTable) {
      super(metadataTable);
      this.type = metadataTable.metadataTableType();
      this.baseTableName = metadataTable.table().name();
    }

    @Override
    protected Table newTable(TableOperations ops, String tableName) {
      return MetadataTableUtils.createMetadataTableInstance(ops, baseTableName, tableName, type);
    }
  }

  // captures the current state of a Hadoop configuration in a serializable manner
  private static class SerializableConfiguration implements Serializable {

    private final Map<String, String> confAsMap;
    private transient volatile Configuration conf = null;

    SerializableConfiguration(Configuration conf) {
      this.confAsMap = Maps.newHashMapWithExpectedSize(conf.size());
      conf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
    }

    public Configuration get() {
      if (conf == null) {
        synchronized (this) {
          if (conf == null) {
            Configuration newConf = new Configuration(false);
            confAsMap.forEach(newConf::set);
            this.conf = newConf;
          }
        }
      }

      return conf;
    }
  }
}
