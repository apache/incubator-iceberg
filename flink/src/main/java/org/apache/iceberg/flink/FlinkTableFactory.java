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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

public class FlinkTableFactory implements TableSinkFactory<RowData>, TableSourceFactory<RowData> {
  private final FlinkCatalog catalog;

  public FlinkTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public TableSource<RowData> createTableSource(TableSourceFactory.Context context) {
    ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
    TableLoader tableLoader = createTableLoader(objectPath);
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
    return new IcebergTableSource(tableLoader, catalog.getHadoopConf(), tableSchema, context.getTable().getOptions());
  }

  @Override
  public TableSink<RowData> createTableSink(TableSinkFactory.Context context) {
    ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
    TableLoader tableLoader = createTableLoader(objectPath);
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
    return new IcebergTableSink(context.isBounded(), tableLoader, catalog.getHadoopConf(), tableSchema);
  }

  @Override
  public Map<String, String> requiredContext() {
    throw new UnsupportedOperationException("Iceberg Table Factory can not be loaded from Java SPI");
  }

  @Override
  public List<String> supportedProperties() {
    throw new UnsupportedOperationException("Iceberg Table Factory can not be loaded from Java SPI");
  }

  private TableLoader createTableLoader(ObjectPath objectPath) {
    return TableLoader.fromCatalog(catalog.getCatalogLoader(), catalog.toIdentifier(objectPath));
  }
}
