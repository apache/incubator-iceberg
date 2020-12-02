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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Files;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

// TODO: Use the copy of this from core.
class TestTables {
  private TestTables() {
  }

  static TestTable create(File temp, String name, Schema schema, PartitionSpec spec) {
    TestTableOperations ops = new TestTableOperations(name);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }
    ops.commit(null, TableMetadata.newTableMetadata(schema, spec, temp.toString(), ImmutableMap.of()));
    return new TestTable(ops, name);
  }

  static TestTable load(String name) {
    TestTableOperations ops = new TestTableOperations(name);
    if (ops.current() == null) {
      return null;
    }
    return new TestTable(ops, name);
  }

  static boolean drop(String name) {
    synchronized (METADATA) {
      return METADATA.remove(name) != null;
    }
  }

  static class TestTable extends BaseTable {
    private final TestTableOperations ops;

    private TestTable(TestTableOperations ops, String name) {
      super(ops, name);
      this.ops = ops;
    }

    @Override
    public TestTableOperations operations() {
      return ops;
    }
  }

  private static final Map<String, TableMetadata> METADATA = Maps.newHashMap();

  static void clearTables() {
    synchronized (METADATA) {
      METADATA.clear();
    }
  }

  static TableMetadata readMetadata(String tableName) {
    synchronized (METADATA) {
      return METADATA.get(tableName);
    }
  }

  static void replaceMetadata(String tableName, TableMetadata metadata) {
    synchronized (METADATA) {
      METADATA.put(tableName, metadata);
    }
  }

  static class TestTableOperations implements TableOperations {

    private final String tableName;
    private TableMetadata current = null;
    private long lastSnapshotId = 0;
    private int failCommits = 0;

    TestTableOperations(String tableName) {
      this.tableName = tableName;
      refresh();
      if (current != null) {
        for (Snapshot snap : current.snapshots()) {
          this.lastSnapshotId = Math.max(lastSnapshotId, snap.snapshotId());
        }
      } else {
        this.lastSnapshotId = 0;
      }
    }

    void failCommits(int numFailures) {
      this.failCommits = numFailures;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      synchronized (METADATA) {
        this.current = METADATA.get(tableName);
      }
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (base != current) {
        throw new CommitFailedException("Cannot commit changes based on stale metadata");
      }
      synchronized (METADATA) {
        refresh();
        if (base == current) {
          if (failCommits > 0) {
            this.failCommits -= 1;
            throw new CommitFailedException("Injected failure");
          }
          METADATA.put(tableName, metadata);
          this.current = metadata;
        } else {
          throw new CommitFailedException(
              "Commit failed: table was updated at %d", base.lastUpdatedMillis());
        }
      }
    }

    @Override
    public FileIO io() {
      return new LocalFileIO();
    }

    @Override
    public LocationProvider locationProvider() {
      Preconditions.checkNotNull(current,
          "Current metadata should not be null when locatinProvider is called");
      return LocationProviders.locationsFor(current.location(), current.properties());
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return new File(new File(current.location(), "metadata"), fileName).getAbsolutePath();
    }

    @Override
    public long newSnapshotId() {
      long nextSnapshotId = lastSnapshotId + 1;
      this.lastSnapshotId = nextSnapshotId;
      return nextSnapshotId;
    }
  }

  static class LocalFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(new File(path));
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }

    @Override
    public void deleteDirectory(String directory) {
      try {
        FileUtils.deleteDirectory(new File(directory));
      } catch (Exception e) {
        throw new RuntimeIOException("Failed to delete directory: " + directory);
      }
    }
  }
}
