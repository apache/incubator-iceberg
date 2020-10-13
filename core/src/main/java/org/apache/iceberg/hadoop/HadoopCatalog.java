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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * HadoopCatalog provides a way to use table names like db.table to work with path-based tables under a common
 * location. It uses a specified directory under a specified filesystem as the warehouse directory, and organizes
 * multiple levels directories that mapped to the database, namespace and the table respectively. The HadoopCatalog
 * takes a location as the warehouse directory. When creating a table such as $db.$tbl, it creates $db/$tbl
 * directory under the warehouse directory, and put the table metadata into that directory.
 *
 * The HadoopCatalog now supports {@link org.apache.iceberg.catalog.Catalog#createTable},
 * {@link org.apache.iceberg.catalog.Catalog#dropTable}, the {@link org.apache.iceberg.catalog.Catalog#renameTable}
 * is not supported yet.
 *
 * Note: The HadoopCatalog requires that the underlying file system supports atomic rename.
 */
public class HadoopCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces {

  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private static final String TABLE_METADATA_FILE_EXTENSION = ".metadata.json";
  private static final Joiner SLASH = Joiner.on("/");
  private static final PathFilter TABLE_FILTER = path -> path.getName().endsWith(TABLE_METADATA_FILE_EXTENSION);

  private final String catalogName;
  private final Configuration conf;
  private final String warehouseLocation;
  private final FileSystem fs;

  /**
   * The constructor of the HadoopCatalog. It uses the passed location as its warehouse directory.
   *
   * @param name The catalog name
   * @param conf The Hadoop configuration
   * @param warehouseLocation The location used as warehouse directory
   */
  public HadoopCatalog(String name, Configuration conf, String warehouseLocation) {
    Preconditions.checkArgument(warehouseLocation != null && !warehouseLocation.equals(""),
        "no location provided for warehouse");

    this.catalogName = name;
    this.conf = conf;
    this.warehouseLocation = warehouseLocation.replaceAll("/*$", "");
    this.fs = Util.getFs(new Path(warehouseLocation), conf);
  }

  /**
   * The constructor of the HadoopCatalog. It uses the passed location as its warehouse directory.
   *
   * @param conf The Hadoop configuration
   * @param warehouseLocation The location used as warehouse directory
   */
  public HadoopCatalog(Configuration conf, String warehouseLocation) {
    this("hadoop", conf, warehouseLocation);
  }

  /**
   * The constructor of the HadoopCatalog. It gets the value of <code>fs.defaultFS</code> property
   * from the passed Hadoop configuration as its default file system, and use the default directory
   * <code>iceberg/warehouse</code> as the warehouse directory.
   *
   * @param conf The Hadoop configuration
   */
  public HadoopCatalog(Configuration conf) {
    this("hadoop", conf, conf.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(namespace.levels().length >= 1,
        "Missing database in table identifier: %s", namespace);

    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));
    Set<TableIdentifier> tblIdents = Sets.newHashSet();

    try {
      if (!fs.exists(nsPath) || !fs.isDirectory(nsPath)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      for (FileStatus s : fs.listStatus(nsPath)) {
        Path path = s.getPath();
        if (!fs.isDirectory(path)) {
          // Ignore the path which is not a directory.
          continue;
        }

        Path metadataPath = new Path(path, "metadata");
        if (fs.exists(metadataPath) && fs.isDirectory(metadataPath) &&
            (fs.listStatus(metadataPath, TABLE_FILTER).length >= 1)) {
          // Only the path which contains metadata is the path for table, otherwise it could be
          // still a namespace.
          TableIdentifier tblIdent = TableIdentifier.of(namespace, path.getName());
          tblIdents.add(tblIdent);
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to list tables under: %s", namespace);
    }

    return Lists.newArrayList(tblIdents);
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier identifier) {
    return true;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new HadoopTableOperations(new Path(defaultWarehouseLocation(identifier)), conf);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String tableName = tableIdentifier.name();
    StringBuilder sb = new StringBuilder();

    sb.append(warehouseLocation).append('/');
    for (String level : tableIdentifier.namespace().levels()) {
      sb.append(level).append('/');
    }
    sb.append(tableName);

    return sb.toString();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    Path tablePath = new Path(defaultWarehouseLocation(identifier));
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    try {
      if (purge && lastMetadata != null) {
        // Since the data files and the metadata files may store in different locations,
        // so it has to call dropTableData to force delete the data file.
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }
      fs.delete(tablePath, true /* recursive */);
      return true;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", tablePath);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Cannot rename Hadoop tables");
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(
        !namespace.isEmpty(),
        "Cannot create namespace with invalid name: %s", namespace);
    if (!meta.isEmpty()) {
      throw new UnsupportedOperationException("Cannot create namespace " + namespace + ": metadata is not supported");
    }

    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

    if (isNamespace(nsPath)) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    try {
      fs.mkdirs(nsPath);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Create namespace failed: %s", namespace);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    Path nsPath = namespace.isEmpty() ? new Path(warehouseLocation)
        : new Path(warehouseLocation, SLASH.join(namespace.levels()));
    if (!isNamespace(nsPath)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      return Stream.of(fs.listStatus(nsPath))
        .map(FileStatus::getPath)
        .filter(this::isNamespace)
        .map(path -> append(namespace, path.getName()))
        .collect(Collectors.toList());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to list namespace under: %s", namespace);
    }
  }

  private Namespace append(Namespace ns, String name) {
    String[] levels = Arrays.copyOfRange(ns.levels(), 0, ns.levels().length + 1);
    levels[ns.levels().length] = name;
    return Namespace.of(levels);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

    if (!isNamespace(nsPath) || namespace.isEmpty()) {
      return false;
    }

    try {
      if (fs.listStatusIterator(nsPath).hasNext()) {
        throw new NamespaceNotEmptyException("Namespace %s is not empty.", namespace);
      }

      return fs.delete(nsPath, false /* recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Namespace delete failed: %s", namespace);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace,  Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Cannot set namespace properties " + namespace + " : setProperties is not supported");
  }

  @Override
  public boolean removeProperties(Namespace namespace,  Set<String> properties) {
    throw new UnsupportedOperationException(
        "Cannot remove properties " + namespace + " : removeProperties is not supported");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

    if (!isNamespace(nsPath) || namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.of("location", nsPath.toString());
  }

  private boolean isNamespace(Path path) {
    Path metadataPath = new Path(path, "metadata");
    try {
      return fs.isDirectory(path) && !(fs.exists(metadataPath) && fs.isDirectory(metadataPath) &&
          (fs.listStatus(metadataPath, TABLE_FILTER).length >= 1));

    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to list namespace info: %s ", path);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", catalogName)
        .add("location", warehouseLocation)
        .toString();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new HadoopCatalogTableBuilder(identifier, schema);
  }

  private class HadoopCatalogTableBuilder extends BaseMetastoreCatalogTableBuilder {
    private HadoopCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
    }

    @Override
    public TableBuilder withLocation(String location) {
      Preconditions.checkArgument(location == null, "Cannot set a custom location for a path-based table");
      return this;
    }
  }
}
