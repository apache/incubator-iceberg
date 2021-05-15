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

import java.util.concurrent.TimeUnit;

public class CatalogProperties {

  private CatalogProperties() {
  }

  public static final String CATALOG_IMPL = "catalog-impl";
  public static final String FILE_IO_IMPL = "io-impl";
  public static final String WAREHOUSE_LOCATION = "warehouse";

  public static final String URI = "uri";
  public static final String CLIENT_POOL_SIZE = "clients";
  public static final int CLIENT_POOL_SIZE_DEFAULT = 2;
  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client.pool.cache.eviction-interval-ms";
  public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);

  public static final String LOCK_IMPL = "lock-impl";

  public static final String LOCK_HEARTBEAT_INTERVAL_MS = "lock.heartbeat-interval-ms";
  public static final long LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(3);

  public static final String LOCK_HEARTBEAT_TIMEOUT_MS = "lock.heartbeat-timeout-ms";
  public static final long LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(15);

  public static final String LOCK_HEARTBEAT_THREADS = "lock.heartbeat-threads";
  public static final int LOCK_HEARTBEAT_THREADS_DEFAULT = 4;

  public static final String LOCK_ACQUIRE_INTERVAL_MS = "lock.acquire-interval-ms";
  public static final long LOCK_ACQUIRE_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(5);

  public static final String LOCK_ACQUIRE_TIMEOUT_MS = "lock.acquire-timeout-ms";
  public static final long LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(3);

  public static final String LOCK_TABLE = "lock.table";

  public static final String ENCRYPTION_TABLE_METADATA_MANAGER_IMPL = "encryption.table-metadata.manager-impl";

}
