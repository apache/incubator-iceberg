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

import org.apache.iceberg.catalog.SupportsCatalogTransactions.IsolationLevel;
import org.apache.iceberg.catalog.SupportsCatalogTransactions.LockingMode;
import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * A {@link Catalog} that applies all mutations within a single transaction.
 *
 * <p>A TransactionalCatalog can spawn child transactions for multiple operations on different
 * tables. All operations will be done within the context of a single Catalog-level transaction
 * and they will either all be successful or all fail.
 *
 * <p>A TransactionalCatalog is initially active upon creation and will remain so until one of
 * the following terminal actions occurs:
 * <ul>
 * <li>{@link rollback} is called.
 * <li>{@link commit} is called.
 * <li>The transaction expires while using Pessimistic {@link LockingMode}.
 * <li>The transaction is terminated externally (for example, when a locking arbitrator
 *     determines a deadlock between two transactions has occurred).
 * <li>The underlying implementation determines that the transaction can no longer complete
 *     successfully.
 * </ul>
 *
 * <p>When one of the items above occurs, the transaction is no longer valid. Further use
 * of the transaction will result in a {@link IllegalStateException} being thrown.
 *
 * <p>Nested transactions such as creating a new table may fail. Those failures alone do
 * not necessarily result in a failure of the catalog-level transaction.
 *
 * <p>Implementations of {@code TransactionalCatalog} are responsible for monitoring all
 * table level operations that are spawned from this catalog and ensure that all nested
 * transactions that are completed successfully are either exposed atomically or not.
 *
 */
public interface TransactionalCatalog extends Catalog, AutoCloseable {

  /**
   * An internal identifier associated with this transaction.
   * @return An internal identifier.
   */
  String transactionId();

  /**
   * Return the current {@code IsolationLevel} for this transaction.
   * @return The IsolationLevel for this transaction.
   */
  IsolationLevel isolationLevel();

  /**
   * Return the {@link LockingMode} for this transaction.
   * @return The LockingMode for this transaction.
   */
  LockingMode lockingMode();

  /**
   * Whether the current transaction is still active/open.
   * @return True until a terminal action occurs.
   */
  boolean active();

  /**
   * Aborts the set of operations here and makes this TransactionalCatalog inoperable.
   *
   * <p>Once called, no further operations can be done against this catalog. If any
   * operations are attempted, {@link IllegalStateException} will be thrown.
   */
  void rollback();

  /**
   * Commit the pending changes from all nested transactions against the Catalog.
   *
   * <p>Once called, no further operations can be done against this catalog. If any
   * operations are attempted, {@link IllegalStateException} will be thrown.
   *
   * @throws CommitFailedException If the updates cannot be committed due to conflicts.
   */
  void commit();

  /**
   * Close out all resources associated with a transaction.
   *
   * <p>This will do a conditional rollback if neither {@code commit} nor {@code rollback}
   * were called. Standard usage looks like:
   * <pre>
   * try(TransactionalCatalog tx = catalog.createTransaction(IsolationLevel.READ_COMMITTED)) {
   *  doOp1(tx);
   *  doOp2(tx);
   *  tx.commit();
   * }
   * </pre>
   * This pattern is designed such that if {@code doOp1()} or {@code doOp2()} throw an exception,
   * the transaction will be automatically rolled back. If both operations complete successfully,
   * the close will only close any remaining open resources associated with the transaction.
   */
  @Override
  void close();

}
