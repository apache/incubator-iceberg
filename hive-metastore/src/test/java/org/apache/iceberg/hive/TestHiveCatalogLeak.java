/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hive;

import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.concurrent.*;

public class TestHiveCatalogLeak extends HiveMetastoreTest {
    @Test
    public void testHmsClientLeak() {
        connectHMSSimulator();

        // Before cache cleanup, some clients may not be closed.
        Assert.assertNotEquals(String.format("open count %s , close count %s , all clients have been closed.",
                ClientPoolImpl.openCount.get(), ClientPoolImpl.closeCount.get()),
                ClientPoolImpl.openCount.get(), ClientPoolImpl.closeCount.get());

        CachedClientPool.cleanupCache();

        // Cleanup cache is async, so we should wait some time
        long startTime = System.currentTimeMillis();
        while((ClientPoolImpl.openCount.get() != ClientPoolImpl.closeCount.get())
                && (startTime + 18000) > System.currentTimeMillis()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                // do nothing
            }
        }

        // After cache cleanup, all clients are closed.
        Assert.assertEquals(String.format("open count %s , close count %s , some clients may not be closed.",
                ClientPoolImpl.openCount.get(), ClientPoolImpl.closeCount.get()),
                ClientPoolImpl.openCount.get(), ClientPoolImpl.closeCount.get());
    }

    private void connectHMSSimulator() {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<?> future = executorService.submit(() -> {
                TableIdentifier name = TableIdentifier.of("hivedb", "iceberg_test_" + finalI);
                Schema schema = new Schema(Types.NestedField.required(1, "c1", Types.IntegerType.get()));
                catalog.createTable(name, schema);
            });
            futures.add(future);
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // do nothing
            }
        }
        executorService.shutdown();
    }
}
