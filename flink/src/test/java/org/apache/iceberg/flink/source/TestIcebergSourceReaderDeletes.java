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
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
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
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.RowDataIteratorReaderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceReaderDeletes extends TestFlinkReaderDeletesBase {

  private static final int PARALLELISM = 4;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
          .setNumberTaskManagers(1)
          .setNumberSlotsPerTaskManager(PARALLELISM)
          .build());

  public TestIcebergSourceReaderDeletes(FileFormat inputFormat) {
    super(inputFormat);
  }

  @Override
  protected StructLikeSet rowSet(String tableName, Table testTable, String... columns) throws IOException {
    final Schema projected = testTable.schema().select(columns);
    final RowType rowType = FlinkSchemaUtil.convert(projected);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    properties.put(CatalogProperties.URI, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    properties.put(CatalogProperties.CLIENT_POOL_SIZE,
        Integer.toString(hiveConf.getInt("iceberg.hive.client-pool-size", 5)));
    CatalogLoader hiveCatalogLoader = CatalogLoader.hive(catalog.name(), hiveConf, properties);
    TableLoader hiveTableLoader = TableLoader.fromCatalog(hiveCatalogLoader, TableIdentifier.of("default", tableName));
    hiveTableLoader.open();
    try (TableLoader tableLoader = hiveTableLoader) {
      final Configuration config = new Configuration();
      final ScanContext scanContext = ScanContext.builder()
          .project(projected)
          .build();
      final Table table = tableLoader.loadTable();
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      final DataStream<RowData> stream = env.fromSource(
          IcebergSource.<RowData>builder()
              .tableLoader(tableLoader)
              .assignerFactory(new SimpleSplitAssignerFactory())
              .readerFactory(new RowDataIteratorReaderFactory(config, table, scanContext, rowType))
              .scanContext(scanContext)
              .build(),
          WatermarkStrategy.noWatermarks(),
          "testBasicRead",
          TypeInformation.of(RowData.class));

      try (CloseableIterator<RowData> iter = stream.executeAndCollect()) {
        List<RowData> rowDataList = Lists.newArrayList(iter);
        StructLikeSet set = StructLikeSet.create(projected.asStruct());
        rowDataList.forEach(rowData -> {
          RowDataWrapper wrapper = new RowDataWrapper(rowType, projected.asStruct());
          set.add(wrapper.wrap(rowData));
        });
        return set;
      } catch (Exception e) {
        throw new IOException("Failed to collect result", e);
      }
    }
  }

}
