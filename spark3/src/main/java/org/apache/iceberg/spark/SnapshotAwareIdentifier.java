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

/**
 * An org.apache.spark.sql.connector.catalog.Identifier that allows
 * org.apache.spark.sql.connector.catalog.TableCatalog#loadTable(Identifier)
 * implemented by SparkCatalog to load a SparkTable corresponding to the snapshot
 * given by id or timestamp, were either to be specified in options passed to the
 * DataFrameReader.
 */
public interface SnapshotAwareIdentifier extends Identifier {

  Long snapshotId();

  Long asOfTimestamp();
}
