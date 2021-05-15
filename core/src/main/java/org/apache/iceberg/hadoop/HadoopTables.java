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

package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Tables;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.encryption.TableMetadataEncryptionManagers;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Iceberg tables that uses the Hadoop FileSystem
 * to store metadata and manifests.
 */
public class HadoopTables implements Tables, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTables.class);
  private static final String METADATA_JSON = "metadata.json";
  private Configuration conf;

  public HadoopTables() {
    this(new Configuration());
  }

  public HadoopTables(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Loads the table location from a FileSystem path location.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_table/)
   * @return table implementation
   */
  @Override
  public Table load(String location) {
    Table result;
    Pair<String, MetadataTableType> parsedMetadataType = parseMetadataType(location);

    if (parsedMetadataType != null) {
      // Load a metadata table
      result = loadMetadataTable(parsedMetadataType.first(), location, parsedMetadataType.second());
    } else {
      // Load a normal table
      TableOperations ops = newTableOps(location);
      if (ops.current() != null) {
        result = new BaseTable(ops, location);
      } else {
        throw new NoSuchTableException("Table does not exist at location: %s", location);
      }
    }

    LOG.info("Table location loaded: {}", result.location());
    return result;
  }

  @Override
  public boolean exists(String location) {
    return newTableOps(location).current() != null;
  }

  /**
   * Try to resolve a metadata table, which we encode as URI fragments
   * e.g. hdfs:///warehouse/my_table#snapshots
   * @param location Path to parse
   * @return A base table name and MetadataTableType if a type is found, null if not
   */
  private Pair<String, MetadataTableType> parseMetadataType(String location) {
    int hashIndex = location.lastIndexOf('#');
    if (hashIndex != -1 && !location.endsWith("#")) {
      String baseTable = location.substring(0, hashIndex);
      String metaTable = location.substring(hashIndex + 1);
      MetadataTableType type = MetadataTableType.from(metaTable);
      return (type == null) ? null : Pair.of(baseTable, type);
    } else {
      return null;
    }
  }

  private Table loadMetadataTable(String location, String metadataTableName, MetadataTableType type) {
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: %s", location);
    }

    return MetadataTableUtils.createMetadataTableInstance(ops, location, metadataTableName, type);
  }

  /**
   * Create a table using the FileSystem implementation resolve from
   * location.
   *
   * @param schema iceberg schema used to create the table
   * @param spec partitioning spec, if null the table will be unpartitioned
   * @param properties a string map of table properties, initialized to empty if null
   * @param location a path URI (e.g. hdfs:///warehouse/my_table)
   * @return newly created table implementation
   */
  @Override
  public Table create(Schema schema, PartitionSpec spec, SortOrder order,
                      Map<String, String> properties, String location) {
    return buildTable(location, schema).withPartitionSpec(spec)
        .withSortOrder(order)
        .withProperties(properties)
        .create();
  }

  /**
   * Drop a table and delete all data and metadata files.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_table)
   * @return true if the table was dropped, false if it did not exist
   */
  public boolean dropTable(String location) {
    return dropTable(location, true);
  }

  /**
   * Drop a table; optionally delete data and metadata files.
   * <p>
   * If purge is set to true the implementation should delete all data and metadata files.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_table)
   * @param purge if true, delete all data and metadata files in the table
   * @return true if the table was dropped, false if it did not exist
   */
  public boolean dropTable(String location, boolean purge) {
    TableOperations ops = newTableOps(location);
    TableMetadata lastMetadata = null;
    if (ops.current() != null) {
      if (purge) {
        lastMetadata = ops.current();
      }
    } else {
      return false;
    }

    try {
      if (purge && lastMetadata != null) {
        // Since the data files and the metadata files may store in different locations,
        // so it has to call dropTableData to force delete the data file.
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }
      Path tablePath = new Path(location);
      Util.getFs(tablePath, conf).delete(tablePath, true /* recursive */);
      return true;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete file: " + location, e);
    }
  }

  @VisibleForTesting
  TableOperations newTableOps(String location) {
    if (location.contains(METADATA_JSON)) {
      return new StaticTableOperations(location, new HadoopFileIO(conf), TableMetadataEncryptionManagers.plainText());
    } else {
      return new HadoopTableOperations(new Path(location), new HadoopFileIO(conf), conf);
    }
  }

  private TableMetadata tableMetadata(Schema schema, PartitionSpec spec, SortOrder order,
                                      Map<String, String> properties, String location) {
    Preconditions.checkNotNull(schema, "A table schema is required");

    Map<String, String> tableProps = properties == null ? ImmutableMap.of() : properties;
    PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
    SortOrder sortOrder = order == null ? SortOrder.unsorted() : order;
    return TableMetadata.newTableMetadata(schema, partitionSpec, sortOrder, location, tableProps);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param location a location for the table
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  public Transaction newCreateTableTransaction(
      String location,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return buildTable(location, schema).withPartitionSpec(spec).withProperties(properties).createTransaction();
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param location a location for the table
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  public Transaction newReplaceTableTransaction(
      String location,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      boolean orCreate) {


    Catalog.TableBuilder builder = buildTable(location, schema).withPartitionSpec(spec).withProperties(properties);
    return orCreate ? builder.createOrReplaceTransaction() : builder.replaceTransaction();
  }

  public Catalog.TableBuilder buildTable(String location, Schema schema) {
    return new HadoopTableBuilder(location, schema);
  }

  private class HadoopTableBuilder implements Catalog.TableBuilder {
    private final String location;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();


    HadoopTableBuilder(String location, Schema schema) {
      this.location = location;
      this.schema = schema;
    }

    @Override
    public Catalog.TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public Catalog.TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public Catalog.TableBuilder withLocation(String newLocation) {
      Preconditions.checkArgument(newLocation == null || location.equals(newLocation),
          String.format("Table location %s differs from the table location (%s) from the PathIdentifier",
              newLocation, location));
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        propertiesBuilder.putAll(properties);
      }
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      TableOperations ops = newTableOps(location);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists at location: %s", location);
      }

      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = tableMetadata(schema, spec, sortOrder, properties, location);
      ops.commit(null, metadata);
      return new BaseTable(ops, location);
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(location);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", location);
      }

      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = tableMetadata(schema, spec, null, properties, location);
      return Transactions.createTableTransaction(location, ops, metadata);
    }

    @Override
    public Transaction replaceTransaction() {
      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(location);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("No such table: %s", location);
      }

      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata;
      if (ops.current() != null) {
        metadata = ops.current().buildReplacement(schema, spec, sortOrder, location, properties);
      } else {
        metadata = tableMetadata(schema, spec, sortOrder, properties, location);
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(location, ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(location, ops, metadata);
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
