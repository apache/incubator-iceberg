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

package org.apache.iceberg.spark;

import org.apache.spark.sql.connector.catalog.Identifier;

public class TableIdentifier implements SnapshotAwareIdentifier {
  private final String[] namespace;
  private final String name;
  private final Long snapshotId;
  private final Long asOfTimestamp;

  public static Identifier of(String[] namespace, String name, Long snapshotId, Long asOfTimestamp) {
    return new TableIdentifier(namespace, name, snapshotId, asOfTimestamp);
  }

  public TableIdentifier(String[] namespace, String name, Long snapshotId, Long asOfTimestamp) {
    this.namespace = namespace;
    this.name = name;
    this.snapshotId = snapshotId;
    this.asOfTimestamp = asOfTimestamp;
  }

  @Override
  public String[] namespace() {
    return namespace;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long asOfTimestamp() {
    return asOfTimestamp;
  }
}
