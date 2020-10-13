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

package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;

/**
 * A Catalog API for table create, drop, and load operations.
 */
public interface Catalog {

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  default String name() {
    return toString();
  }

  /**
   * Return all the identifiers under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for tables
   * @throws  NotFoundException if the namespace is not found
   */
  List<TableIdentifier> listTables(Namespace namespace);

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties);

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return createTable(identifier, schema, spec, null, properties);
  }

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec) {
    return createTable(identifier, schema, spec, null, null);
  }

  /**
   * Create an unpartitioned table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema) {
    return createTable(identifier, schema, PartitionSpec.unpartitioned(), null, null);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties);

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return newCreateTableTransaction(identifier, schema, spec, null, properties);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec) {
    return newCreateTableTransaction(identifier, schema, spec, null, null);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema) {
    return newCreateTableTransaction(identifier, schema, PartitionSpec.unpartitioned(), null, null);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties,
      boolean orCreate);

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, spec, null, properties, orCreate);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, spec, null, null, orCreate);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, PartitionSpec.unpartitioned(), null, null, orCreate);
  }

  /**
   * Check whether table exists.
   *
   * @param identifier a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(TableIdentifier identifier) {
    try {
      loadTable(identifier);
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Drop a table and delete all data and metadata files.
   *
   * @param identifier a table identifier
   * @return true if the table was dropped, false if the table did not exist
   */
  default boolean dropTable(TableIdentifier identifier) {
    return dropTable(identifier, true /* drop data and metadata files */);
  }

  /**
   * Drop a table; optionally delete data and metadata files.
   * <p>
   * If purge is set to true the implementation should delete all data and metadata files.
   *
   * @param identifier a table identifier
   * @param purge if true, delete all data and metadata files in the table
   * @return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(TableIdentifier identifier, boolean purge);

  /**
   * Rename a table.
   *
   * @param from identifier of the table to rename
   * @param to new table name
   * @throws NoSuchTableException if the from table does not exist
   * @throws AlreadyExistsException if the to table already exists
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * Load a table.
   *
   * @param identifier a table identifier
   * @return instance of {@link Table} implementation referred by {@code tableIdentifier}
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(TableIdentifier identifier);

  /**
   * Instantiate a builder to either create a table or start a create/replace transaction.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return the builder to create a table or start a create/replace transaction
   */
  default TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement buildTable");
  }

  /**
   * A builder used to create valid {@link Table tables} or start create/replace {@link Transaction transactions}.
   * <p>
   * Call {@link #buildTable(TableIdentifier, Schema)} to create a new builder.
   */
  interface TableBuilder {
    /**
     * Sets a partition spec for the table.
     *
     * @param spec a partition spec
     * @return this for method chaining
     */
    TableBuilder withPartitionSpec(PartitionSpec spec);

    /**
     * Sets a sort order for the table.
     *
     * @param sortOrder a sort order
     * @return this for method chaining
     */
    TableBuilder withSortOrder(SortOrder sortOrder);

    /**
     * Sets a location for the table.
     *
     * @param location a location
     * @return this for method chaining
     */
    TableBuilder withLocation(String location);

    /**
     * Adds key/value properties to the table.
     *
     * @param properties key/value properties
     * @return this for method chaining
     */
    TableBuilder withProperties(Map<String, String> properties);

    /**
     * Adds a key/value property to the table.
     *
     * @param key   a key
     * @param value a value
     * @return this for method chaining
     */
    TableBuilder withProperty(String key, String value);

    /**
     * Creates the table.
     *
     * @return the created table
     */
    Table create();

    /**
     * Starts a transaction to create the table.
     *
     * @return the {@link Transaction} to create the table
     */
    Transaction createTransaction();

    /**
     * Starts a transaction to replace the table.
     *
     * @return the {@link Transaction} to replace the table
     */
    Transaction replaceTransaction();

    /**
     * Starts a transaction to create or replace the table.
     *
     * @return the {@link Transaction} to create or replace the table
     */
    Transaction createOrReplaceTransaction();
  }
}
