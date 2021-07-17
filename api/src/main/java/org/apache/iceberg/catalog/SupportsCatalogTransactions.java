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

import java.util.Set;

/**
 * Catalog methods for working with catalog-level transactions.
 *
 * <p>Catalog implementations are not required to support catalog-level transactional state.
 * If they do, they may support one or more {@code IsolationLevel}s and one or more
 * {@code LockingMode}s.
 */
public interface SupportsCatalogTransactions {

  /**
   * The level of isolation for a catalog-level transaction.
   *
   * <p>Isolation covers both what data is read and what data can be written.
   *
   * <p>At all levels, data is only visible if it is either committed by another transaction or
   * committed by a nested transaction within this catalog-level transaction.
   *
   * <p>Individual nested Table transactions may be "rebased" to expose updated versions of a
   * table if the isolation level allows that behavior.
   *
   * <p>In the definitions of each isolation level, the concept of conflicting writes is
   * referenced. Conflicting writes are two mutations to the same object that happen concurrently.
   * Depending on the particular implementation, the coarseness of this conflict may vary. The
   * most coarse conflict is any two mutations to the same table. However, some implementations
   * may consider some of these "absolute" conflicts as allowable by using finer-grained conflict
   * resolution. For example, two different operations that both append new files to a table may
   * be in "absolute" conflict but could be resolved automatically as a "safe conflict" by using
   * a set of automatic implementation-defined conflict resolution rules.
   */
  enum IsolationLevel {

    /**
     * Reading the same table multiple times may result in different versions read of the same
     * table. A commit can be completed as long as any tables changed externally do not conflict
     * with any writes within this transaction.
     */
    READ_COMMITTED,

    /**
     * Reading the same table multiple times within the same transaction will result in
     * the same version of that table. Different tables may come from different snapshots.
     * A commit can be completed as long as any tables changed externally do not conflict
     * with any writes within this transaction.
     */
    REPEATABLE_READ,

    /**
     * A commit will only succeed if there have been no meaningful changes to data read during
     * the course of this transaction prior to commit. This imposes stricter read guarantees than
     * {@code REPEATED_READ} (consistent reads per table) as it requires that the reads are
     * consistent for all tables to a single point in time (or single snapshot of the database).
     * Additionally, it implies additional requirements around the successful completion of a
     * write. In order for a write to complete, any entities read during this transaction are also
     * disallowed from changing (via another transaction) post-read in ways that would influence the
     * writes of this operation. This isolation level encompasses all the standard guarantees of
     * snapshot isolation.
     */
    SERIALIZABLE;

  }

  /**
   * The type of locking mode used by the transaction.
   */
  enum LockingMode {

    /**
     * Pessimistically lock tables. In this situation, each table being worked on will grab a lock.
     * This mode will typically result in a better chance for transactions to complete at the cost
     * of throughput and resource consumption.
     */
    PESSIMISTIC,

    /**
     * Run the transaction with an optimistic behavior. This assumes that most operations will not
     * conflict. It typically increases concurrency and throughput while reducing resource
     * consumption.
     */
    OPTIMISTIC;
  }

  /**
   * Get the list of {@link LockingMode}s this Catalog supports.
   * @return A set of locking modes supported.
   */
  Set<LockingMode> lockingModes();

  /**
   * Get the list of {@link IsolationLevel}s this Catalog supports.
   * @return A set of locking modes supported.
   */
  Set<IsolationLevel> isolationLevels();

  /**
   * Start a new transaction and return a {@link TransactionalCatalog} that supports
   * cross table operations. If a {@link Catalog} supports {@link OPTIMISTIC}
   * locking mode, this will use that mode. If it only supports {@link PESSIMISTIC},
   * it will use that mode with default values for lock times.
   *
   * @param isolationLevel The {@link IsolationLevel} to use for the transaction.
   * @return A Catalog with an open transaction.
   * @throws IllegalArgumentException If the IsolationLevel is not supported.
   */
  TransactionalCatalog createTransaction(IsolationLevel isolationLevel);

  /**
   * Start a new transaction that holds locks for the life of the transaction.
   *
   * <p>If supported, this type of transaction allows a much higher likelihood of
   * transaction completion but can also result in more failures, lower concurrency
   * and deadlocks.
   *
   * <p>Pessimistic transactions will automatically acquire a lock for each table
   * that is used within that transaction if no lock is already held.
   *
   * @param isolationLevel The {@link IsolationLevel} to use for the transaction.
   * @param lockWaitInMillis How long to wait in milliseconds when grabbing a pessimistic lock.
   * @param maxTimeMillis The maximum amount of time the transaction is allowed to be open.
   * @param tablesHint An optional list of tables that should be locked aggressively for this
   *        transaction. Note that this can include tables that exist and tables that do not
   *        exist as this transaction may be adding new tables and want to grab pessimistic locks
   *        for those operations. This hint is useful if one wants to ensure the acquisition of any
   *        critical locks before doing potentially expensive work.
   *
   * @return A Catalog with an open transaction.
   *
   * @throws IllegalArgumentException If the IsolationLevel is not supported. Also throws if
   *         {@code lockWaitInMillis} or {@code maxTimeMillis} are not within supported limits
   *         (check specific {@link Catalog} documentation for any limits).
   * @throws CommitFailedException If the list of {@code tables} cannot be locked.
   * @throws ValidationException If the set of tables names includes tables names that are not
   *         valid.
   */
  TransactionalCatalog createLockingTransaction(
      IsolationLevel isolationLevel,
      long lockWaitInMillis,
      long maxTimeMillis,
      TableIdentifier...tablesHint);

}
