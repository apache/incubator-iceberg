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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergSerDe.class);
  private static final String LIST_COLUMN_COMMENT = "columns.comments";

  private ObjectInspector inspector;
  private Schema tableSchema;
  private Map<ObjectInspector, Deserializer> deserializers = new HashMap<>(1);
  private Container<Record> row = new Container<>();

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    // HiveIcebergSerDe.initialize is called multiple places in Hive code:
    // - When we are trying to create a table - HiveDDL data is stored at the serDeProperties, but no Iceberg table
    // is created yet.
    // - When we are compiling the Hive query on HiveServer2 side - We only have table information (location/name),
    // and we have to read the schema using the table data. This is called multiple times so there is room for
    // optimizing here.
    // - When we are executing the Hive query in the execution engine - We do not want to load the table data on every
    // executor, but serDeProperties are populated by HiveIcebergStorageHandler.configureInputJobProperties() and
    // the resulting properties are serialized and distributed to the executors

    // temporarily disabling vectorization in Tez, since it doesn't work with projection pruning (fix: TEZ-4248)
    // TODO: remove this once TEZ-4248 has been released and the Tez dependencies updated here
    assertNotVectorizedTez(configuration);

    if (serDeProperties.get(InputFormatConfig.TABLE_SCHEMA) != null) {
      this.tableSchema = SchemaParser.fromJson((String) serDeProperties.get(InputFormatConfig.TABLE_SCHEMA));
    } else {
      try {
        // always prefer the original table schema if there is one
        this.tableSchema = Catalogs.loadTable(configuration, serDeProperties).schema();
        LOG.info("Using schema from existing table {}", SchemaParser.toJson(tableSchema));
      } catch (Exception e) {
        boolean autoConversion = configuration.getBoolean(InputFormatConfig.SCHEMA_AUTO_CONVERSION, false);
        // If we can not load the table try the provided hive schema
        this.tableSchema = hiveSchemaOrThrow(serDeProperties, e, autoConversion);
      }
    }

    configuration.setBoolean(InputFormatConfig.CASE_SENSITIVE, false);
    String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(configuration);
    // When same table is joined multiple times, it is possible some selected columns are duplicated,
    // in this case wrong recordStructField position leads wrong value or ArrayIndexOutOfBoundException
    String[] distinctSelectedColumns = Arrays.stream(selectedColumns).distinct().toArray(String[]::new);
    Schema projectedSchema = distinctSelectedColumns.length > 0 ?
            tableSchema.caseInsensitiveSelect(distinctSelectedColumns) : tableSchema;
    // the input split mapper handles does not belong to this table
    // it is necessary to ensure projectedSchema equals to tableSchema,
    // or we cannot find selectOperator's column from inspector
    if (projectedSchema.columns().size() != distinctSelectedColumns.length) {
      projectedSchema = tableSchema;
    }

    String schemaColumnsStr = serDeProperties.getProperty(serdeConstants.LIST_COLUMNS, "");
    String[] schemaColumns = schemaColumnsStr.split(String.valueOf(SerDeUtils.COMMA));
    Set<String> schemaColumnSet = Arrays.stream(schemaColumns).collect(Collectors.toSet());

    String evolutionColumnsStr = configuration.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "");
    String[] evolutionColumns = evolutionColumnsStr.split(String.valueOf(SerDeUtils.COMMA));
    Set<String> evolutionColumnSet = Arrays.stream(evolutionColumns).collect(Collectors.toSet());

    if (schemaColumnSet.size() == evolutionColumnSet.size()) {
      schemaColumnSet.removeAll(evolutionColumnSet);
      projectedSchema = schemaColumnSet.size() == 0 ? projectedSchema : tableSchema;
    } else {
      projectedSchema = tableSchema;
    }

    try {
      this.inspector = IcebergObjectInspector.create(projectedSchema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private void assertNotVectorizedTez(Configuration configuration) {
    if ("tez".equals(configuration.get("hive.execution.engine")) &&
        "true".equals(configuration.get("hive.vectorized.execution.enabled"))) {
      throw new UnsupportedOperationException("Vectorized execution on Tez is currently not supported when using " +
          "Iceberg tables. Please set hive.vectorized.execution.enabled=false and rerun the query.");
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) {
    Deserializer deserializer = deserializers.get(objectInspector);
    if (deserializer == null) {
      deserializer = new Deserializer.Builder()
          .schema(tableSchema)
          .sourceInspector((StructObjectInspector) objectInspector)
          .writerInspector((StructObjectInspector) inspector)
          .build();
      deserializers.put(objectInspector, deserializer);
    }

    row.set(deserializer.deserialize(o));
    return row;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  /**
   * Gets the hive schema from the serDeProperties, and throws an exception if it is not provided. In the later case
   * it adds the previousException as a root cause.
   * @param serDeProperties The source of the hive schema
   * @param previousException If we had an exception previously
   * @param autoConversion When <code>true</code>, convert unsupported types to more permissive ones, like tinyint to
   *                       int
   * @return The hive schema parsed from the serDeProperties
   * @throws SerDeException If there is no schema information in the serDeProperties
   */
  private static Schema hiveSchemaOrThrow(Properties serDeProperties, Exception previousException,
                                          boolean autoConversion)
      throws SerDeException {
    // Read the configuration parameters
    String columnNames = serDeProperties.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypes = serDeProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    // No constant for column comments and column comments delimiter.
    String columnComments = serDeProperties.getProperty(LIST_COLUMN_COMMENT);
    String columnNameDelimiter = serDeProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
        serDeProperties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNames != null && columnTypes != null && columnNameDelimiter != null &&
        !columnNames.isEmpty() && !columnTypes.isEmpty() && !columnNameDelimiter.isEmpty()) {
      // Parse the configuration parameters
      List<String> names = new ArrayList<>();
      Collections.addAll(names, columnNames.split(columnNameDelimiter));
      List<String> comments = new ArrayList<>();
      if (columnComments != null) {
        Collections.addAll(comments, columnComments.split(Character.toString(Character.MIN_VALUE)));
      }
      Schema hiveSchema = HiveSchemaUtil.convert(names, TypeInfoUtils.getTypeInfosFromTypeString(columnTypes),
              comments, autoConversion);
      LOG.info("Using hive schema {}", SchemaParser.toJson(hiveSchema));
      return hiveSchema;
    } else {
      throw new SerDeException("Please provide an existing table or a valid schema", previousException);
    }
  }
}
