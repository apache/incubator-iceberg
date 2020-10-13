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

package org.apache.iceberg.mr.hive;

import org.apache.hadoop.conf.Configuration;
import org.junit.rules.TemporaryFolder;

public class TestHiveIcebergStorageHandlerWithHiveCatalog extends HiveIcebergStorageHandlerBaseTest {

  @Override
  public TestTables testTables(Configuration conf, TemporaryFolder temp) {
    return new TestTables.HiveTestTables(conf, temp);
  }

  @Override
  protected void createHiveTable(String tableName, String location) {
    // The Hive catalog has already created the Hive table so there's no need to issue another
    // 'CREATE TABLE ...' statement.
  }
}
