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

import com.google.common.base.Preconditions;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class HadoopCatalog extends BaseMetastoreCatalog implements Closeable {
  private final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final org.apache.hadoop.conf.Configuration conf;
  private String hdfsRoot;

  public HadoopCatalog(Configuration conf) {
    this.conf = conf;
    String hdfsRoot = conf.get("fs.defaultFS");
    Path warehousePath = new Path(hdfsRoot + ICEBERG_HADOOP_WAREHOUSE_BASE);
    try {
      FileSystem fs = Util.getFs(warehousePath, conf);
      if (!fs.isDirectory(warehousePath)) {
        if (!fs.mkdirs(warehousePath)) {
          throw new IOException("failed to create warehouse for hadoop catalog");
        }
      }
      this.hdfsRoot = hdfsRoot + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
    } catch (IOException e) {
        throw new RuntimeIOException("failed to create directory for warehouse", e);
    }
  }

  @Override
  public org.apache.iceberg.Table createTable(
    TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
            "Missing database in table identifier: %s", identifier);
    Path tablePath = new Path(defaultWarehouseLocation(identifier));
    try {
      FileSystem fs = Util.getFs(tablePath, conf);
      if (!fs.isDirectory(tablePath)) {
        fs.mkdirs(tablePath);
      } else {
        throw new AlreadyExistsException("the table already exists: " + identifier);
      }
    } catch (IOException e) {
        throw new RuntimeIOException("failed to create directory", e);
    }
    return super.createTable(identifier, schema, spec, null, properties);
  }

  public org.apache.iceberg.Table createTable(
          TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
            "Missing database in table identifier: %s", identifier);
    return createTable(identifier, schema, spec, null, properties);
  }

  public org.apache.iceberg.Table createTable(
          TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
            "Missing database in table identifier: %s", identifier);
    return createTable(identifier, schema, spec, null, null);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
                "Missing database in table identifier: %s", identifier);
    return new HadoopTableOperations(new Path(defaultWarehouseLocation(identifier)), conf);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return this.hdfsRoot + "/" + dbName+ ".db" + "/"+ tableName;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
                "Missing database in table identifier: %s", identifier);

    Path tablePath = new Path(defaultWarehouseLocation(identifier));
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    FileSystem fs = Util.getFs(tablePath, conf);
    try {
      if (purge && lastMetadata != null) {
        dropTableData(ops.io(), lastMetadata);
      }
      fs.delete(tablePath, true /* recursive */);
      return true;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", tablePath);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new RuntimeException("rename a hadoop table is not supported");
  }

  @Override
  public void close() throws IOException { }

}
