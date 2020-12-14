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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeSet;

public class TestFlinkInputFormatReaderDeletes extends TestFlinkReaderDeletesBase {

  public TestFlinkInputFormatReaderDeletes(FileFormat inputFormat) {
    super(inputFormat);
  }

  @Override
  protected StructLikeSet rowSet(String tableName, Table testTable, String... columns) throws IOException {
    Schema projected = testTable.schema().select(columns);
    RowType rowType = FlinkSchemaUtil.convert(projected);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    properties.put(CatalogProperties.URI, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    properties.put(CatalogProperties.CLIENT_POOL_SIZE,
            Integer.toString(hiveConf.getInt("iceberg.hive.client-pool-size", 5)));
    CatalogLoader hiveCatalogLoader = CatalogLoader.hive(catalog.name(), hiveConf, properties);
    FlinkInputFormat inputFormat = FlinkSource.forRowData()
        .tableLoader(TableLoader.fromCatalog(hiveCatalogLoader, TableIdentifier.of("default", tableName)))
        .project(FlinkSchemaUtil.toSchema(rowType)).buildFormat();

    StructLikeSet set = StructLikeSet.create(projected.asStruct());
    TestHelpers.readRowData(inputFormat, rowType).forEach(rowData -> {
      RowDataWrapper wrapper = new RowDataWrapper(rowType, projected.asStruct());
      set.add(wrapper.wrap(rowData));
    });

    return set;
  }

}
