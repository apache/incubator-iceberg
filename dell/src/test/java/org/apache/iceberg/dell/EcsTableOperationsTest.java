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

package org.apache.iceberg.dell;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EcsTableOperationsTest {

  /**
   * table id
   */
  private final TableIdentifier id = TableIdentifier.of("test");
  /**
   * table schema
   */
  private final Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));
  /**
   * data file
   */
  private final DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath("/test/data-a.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();
  /**
   * ecs catalog
   */
  private EcsCatalog ecsCatalog;
  /**
   * ecs table operations
   */
  private EcsTableOperations ecsTableOperations;

  @Before
  public void init() {
    ecsCatalog = new EcsCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put(EcsCatalogProperties.ECS_CLIENT_FACTORY, "org.apache.iceberg.dell.MemoryEcsClient#create");
    properties.put(EcsCatalogProperties.BASE_KEY, "test");
    ecsCatalog.initialize("test", properties);

    ecsTableOperations = (EcsTableOperations) ecsCatalog.newTableOps(id);
  }

  @Test
  public void testTableOperations() {
    assertNull("start up, null table", ecsTableOperations.refresh());

    // create table -> version 0
    Table table = ecsCatalog.createTable(id, schema);

    // verify
    assertEquals("current table version is -1", -1,
        ((EcsTableOperations) ((HasTableOperations) table).operations()).currentVersion());

    // append data file -> version 1
    table.newFastAppend().appendFile(dataFile).commit();

    // verify
    assertEquals("current table version is 1", 1,
        ((EcsTableOperations) ((HasTableOperations) table).operations()).currentVersion());

    // delete data file -> version 2
    table.newDelete().deleteFile(dataFile).commit();

    assertEquals("current table has 2 history entries", 2, table.history().size());
    assertEquals("current table version is 2", 2,
        ((EcsTableOperations) ((HasTableOperations) table).operations()).currentVersion());
  }
}