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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

public class CachingCatalog implements Catalog {
  public static Catalog wrap(Catalog catalog) {
    return wrap(catalog, true);
  }

  public static Catalog wrap(Catalog catalog, boolean caseSensitive) {
    return new CachingCatalog(catalog, caseSensitive);
  }

  private final Cache<TableIdentifier, Table> tableCache = Caffeine.newBuilder().softValues().build();
  private final Catalog catalog;
  private final boolean caseSensitive;

  private CachingCatalog(Catalog catalog, boolean caseSensitive) {
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
  }

  private TableIdentifier canonicalizeIdentifier(TableIdentifier tableIdentifier) {
    if (caseSensitive) {
      return tableIdentifier;
    } else {
      return tableIdentifier.toLowerCase();
    }
  }

  @Override
  public String name() {
    return catalog.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return catalog.listTables(namespace);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    TableIdentifier canonicalized = canonicalizeIdentifier(ident);
    Table cached = tableCache.getIfPresent(canonicalized);
    if (cached != null) {
      return cached;
    }

    if (MetadataTableUtils.hasMetadataTableName(canonicalized)) {
      TableIdentifier originTableIdentifier = TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, catalog::loadTable);

      // share TableOperations instance of origin table for all metadata tables, so that metadata table instances are
      // also refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());

        Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
            ops, catalog.name(), originTableIdentifier,
            canonicalized, type);
        tableCache.put(canonicalized, metadataTable);
        return metadataTable;
      }
    }

    return tableCache.get(canonicalized, catalog::loadTable);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec, String location,
                           Map<String, String> properties) {
    AtomicBoolean created = new AtomicBoolean(false);
    Table table = tableCache.get(canonicalizeIdentifier(ident), identifier -> {
      created.set(true);
      return catalog.createTable(identifier, schema, spec, location, properties);
    });

    if (!created.get()) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    return table;
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier ident, Schema schema, PartitionSpec spec,
                                               String location, Map<String, String> properties) {
    // create a new transaction without altering the cache. the table doesn't exist until the transaction is committed.
    // if the table is created before the transaction commits, any cached version is correct and the transaction create
    // will fail. if the transaction commits before another create, then the cache will be empty.
    return catalog.newCreateTableTransaction(ident, schema, spec, location, properties);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier ident, Schema schema, PartitionSpec spec,
                                                String location, Map<String, String> properties, boolean orCreate) {
    // create a new transaction without altering the cache. the table doesn't change until the transaction is committed.
    // when the transaction commits, invalidate the table in the cache if it is present.
    return CommitCallbackTransaction.addCallback(
        catalog.newReplaceTableTransaction(ident, schema, spec, location, properties, orCreate),
        () -> tableCache.invalidate(canonicalizeIdentifier(ident)));
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    tableCache.invalidate(canonicalizeIdentifier(ident));
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    catalog.renameTable(from, to);
    tableCache.invalidate(canonicalizeIdentifier(from));
  }

}
