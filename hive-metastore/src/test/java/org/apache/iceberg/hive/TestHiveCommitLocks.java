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

package org.apache.iceberg.hive;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHiveCommitLocks extends HiveTableBaseTest {
  private static HiveTableOperations spyOps = null;
  private static HiveClientPool spyClientPool = null;
  private static CachedClientPool spyCachedClientPool = null;
  private static Configuration overriddenHiveConf = new Configuration(hiveConf);
  private static AtomicReference<HiveMetaStoreClient> spyClientRef = new AtomicReference<>();
  private static HiveMetaStoreClient spyClient = null;
  HiveTableOperations ops = null;
  TableMetadata metadataV1 = null;
  TableMetadata metadataV2 = null;

  long dummyLockId = 500L;
  LockResponse waitLockResponse = new LockResponse(dummyLockId, LockState.WAITING);
  LockResponse acquiredLockResponse = new LockResponse(dummyLockId, LockState.ACQUIRED);
  LockResponse notAcquiredLockResponse = new LockResponse(dummyLockId, LockState.NOT_ACQUIRED);

  @BeforeClass
  public static void initializeSpies() throws Exception {
    overriddenHiveConf.setLong("iceberg.hive.lock-timeout-ms", 6 * 1000);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-min-wait-ms", 50);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-max-wait-ms", 5 * 1000);

    // Set up the spy clients as static variables instead of before every test.
    // The spy clients are reused between methods and closed at the end of all tests in this class.
    spyClientPool = spy(new HiveClientPool(1, overriddenHiveConf));
    when(spyClientPool.newClient()).thenAnswer(invocation -> {
      HiveMetaStoreClient client = (HiveMetaStoreClient) invocation.callRealMethod();
      spyClientRef.set(spy(client));
      return spyClientRef.get();
    });

    spyClientPool.run(IMetaStoreClient::isLocalMetaStore); // To ensure new client is created.

    spyCachedClientPool = spy(new CachedClientPool(hiveConf, Collections.emptyMap()));
    when(spyCachedClientPool.clientPool()).thenAnswer(invocation -> spyClientPool);

    Assert.assertNotNull(spyClientRef.get());

    spyClient = spyClientRef.get();
  }

  @Before
  public void before() throws Exception {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    ops = (HiveTableOperations) ((HasTableOperations) table).operations();
    String dbName = TABLE_IDENTIFIER.namespace().level(0);
    String tableName = TABLE_IDENTIFIER.name();

    metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    spyOps = spy(new HiveTableOperations(overriddenHiveConf, spyCachedClientPool, ops.io(), catalog.name(),
            dbName, tableName));
  }

  @AfterClass
  public static void cleanup() {
    try {
      spyClientPool.close();
    } catch (Throwable t) {
      // Ignore any exception
    }
  }

  @Test
  public void testLockAcquisitionAtFirstTime() throws TException, InterruptedException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doNothing().when(spyOps).doUnlock(eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    Assert.assertEquals(1, spyOps.current().schema().columns().size()); // should be 1 again
  }

  @Test
  public void testLockAcquisitionAfterRetries() throws TException, InterruptedException {

    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(acquiredLockResponse)
        .when(spyClient)
        .checkLock(eq(dummyLockId));
    doNothing().when(spyOps).doUnlock(eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    Assert.assertEquals(1, spyOps.current().schema().columns().size()); // should be 1 again
  }

  @Test
  public void testLockFailureAtFirstTime() throws TException {
    doReturn(notAcquiredLockResponse).when(spyClient).lock(any());

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Could not acquire the lock on",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testLockFailureAfterRetries() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(notAcquiredLockResponse)
        .when(spyClient)
        .checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Could not acquire the lock on",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testLockTimeoutAfterRetries() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).when(spyClient).checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Timed out after",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testPassThroughThriftExceptions() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).doThrow(new TException("Test Thrift Exception"))
        .when(spyClient).checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        RuntimeException.class,
        "Metastore operation failed for",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testPassThroughInterruptions() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      Thread.sleep(10);
      return waitLockResponse;
    }).when(spyClient).checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Could not acquire the lock on",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testTableLevelProcessLockBlocksConcurrentHMSRequestsForSameTable() throws Exception {
    int numConcurrentCommits = 10;
    // resetting the spy client to forget about prior call history
    reset(spyClient);

    // simulate several concurrent commit operations on the same table
    ExecutorService executor = Executors.newFixedThreadPool(numConcurrentCommits);
    IntStream.range(0, numConcurrentCommits).forEach(i ->
        executor.submit(() -> {
          try {
            spyOps.doCommit(metadataV2, metadataV1);
          } catch (CommitFailedException e) {
            // failures are expected here when checking the base version
            // it's no problem, we're not testing the actual commit success here, only the HMS lock acquisition attempts
          }
        }));
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);

    // intra-process commits to the same table should be serialized now
    // i.e. no thread should receive WAITING state from HMS and have to call checkLock periodically
    verify(spyClient, never()).checkLock(any(Long.class));
    // all threads eventually got their turn
    verify(spyClient, times(numConcurrentCommits)).lock(any(LockRequest.class));
  }
}
