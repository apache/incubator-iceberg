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

package org.apache.iceberg.nessie;

import java.time.Instant;
import java.util.Objects;
import org.apache.iceberg.catalog.TableIdentifier;

public class TableReference {

  private final TableIdentifier tableIdentifier;
  private final Instant timestamp;
  private final String reference;

  /**
   * Container class to specify a TableIdentifier on a specific Reference or at an Instant in time.
   */
  public TableReference(TableIdentifier tableIdentifier, Instant timestamp, String reference) {
    this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "tableIdentifier must not be null");
    this.timestamp = timestamp;
    this.reference = reference;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public Instant timestamp() {
    return timestamp;
  }

  public String reference() {
    return reference;
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(TableIdentifier path) {
    TableReference pti = parse(path.name());
    return new TableReference(TableIdentifier.of(path.namespace(), pti.tableIdentifier().name()),
        pti.timestamp(),
        pti.reference());
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(String path) {
    // I am assuming tables can't have @ or # symbols
    if (path.split("@").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one branch in %s", path));
    }
    if (path.split("#").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one timestamp in %s", path));
    }

    if (path.contains("@") && path.contains("#")) {
      throw new IllegalArgumentException("Invalid table name:" +
          " # is not allowed (reference by timestamp is not supported)");
    }

    if (path.contains("@")) {
      String[] tableRef = path.split("@");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      return new TableReference(identifier, null, tableRef[1]);
    }

    if (path.contains("#")) {
      throw new IllegalArgumentException("Invalid table name:" +
          " # is not allowed (reference by timestamp is not supported)");
    }

    TableIdentifier identifier = TableIdentifier.parse(path);

    return new TableReference(identifier, null, null);
  }
}
