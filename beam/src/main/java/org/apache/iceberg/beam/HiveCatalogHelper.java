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

package org.apache.iceberg.beam;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;

public class HiveCatalogHelper {

  private HiveCatalogHelper() {
  }

  public static synchronized Table loadOrCreateTable(
      HiveCatalog hiveCatalog,
      TableIdentifier tableIdentifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    try {
      return hiveCatalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException noSuchTableException) {
      try {
        // If it doesn't exist, we just create the table
        return hiveCatalog.createTable(
            tableIdentifier,
            schema,
            spec,
            properties
        );
      } catch (AlreadyExistsException alreadyExistsException) {
        // It can be that there is a race condition, that the table has been
        // created by another worker
        return hiveCatalog.loadTable(tableIdentifier);
      }
    }
  }
}
