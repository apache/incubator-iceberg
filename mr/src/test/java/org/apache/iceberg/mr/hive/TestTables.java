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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestCatalogs;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

// Helper class for setting up and testing various catalog implementations
abstract class TestTables {
  public static final TestTableType[] ALL_TABLE_TYPES = new TestTableType[] {
      TestTableType.HADOOP_TABLE,
      TestTableType.HADOOP_CATALOG,
      TestTableType.CUSTOM_CATALOG,
      TestTableType.HIVE_CATALOG
  };

  private final Tables tables;
  protected final TemporaryFolder temp;
  protected final String catalog;

  protected TestTables(Tables tables, TemporaryFolder temp, String catalogName) {
    this.tables = tables;
    this.temp = temp;
    this.catalog = catalogName;
  }

  protected TestTables(Catalog catalog, TemporaryFolder temp, String catalogName) {
    this(new CatalogToTables(catalog), temp, catalogName);
  }

  public Map<String, String> properties() {
    return Collections.emptyMap();
  }

  // For HadoopTables this method will return a temporary location
  public String identifier(String tableIdentifier) {
    return tableIdentifier;
  }

  public Tables tables() {
    return tables;
  }

  public String catalogName() {
    return catalog;
  }

  /**
   * The location string needed to be provided for CREATE TABLE ... commands,
   * like "LOCATION 'file:///tmp/warehouse/default/tablename'. Empty ("") if LOCATION is not needed.
   * @param identifier The table identifier
   * @return The location string for create table operation
   */
  public abstract String locationForCreateTableSQL(TableIdentifier identifier);

  /**
   * The table properties string needed for the CREATE TABLE ... commands,
   * like "TBLPROPERTIES('iceberg.catalog'='mycatalog')
   * @return The tables properties string, such as "TBLPROPERTIES('iceberg.catalog'='mycatalog')
   */
  public String propertiesForCreateTableSQL(Map<String, String> tableProperties) {
    Map<String, String> properties = new HashMap<>(tableProperties);
    properties.putIfAbsent(InputFormatConfig.CATALOG_NAME, catalog);
    String props = properties.entrySet().stream()
            .map(entry -> String.format("'%s'='%s'", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(","));
    return " TBLPROPERTIES (" + props + ")";
  }

  /**
   * If an independent Hive table creation is needed for the given Catalog then this should return the Hive SQL
   * string which we have to execute. Overridden for HiveCatalog where the Hive table is immediately created
   * during the Iceberg table creation so no extra sql execution is required.
   * @param identifier The table identifier (the namespace should be non-empty and single level)
   * @param tableProps Optional map of table properties
   * @return The SQL string - which should be executed, null - if it is not needed.
   */
  public String createHiveTableSQL(TableIdentifier identifier, Map<String, String> tableProps) {
    Preconditions.checkArgument(!identifier.namespace().isEmpty(), "Namespace should not be empty");
    Preconditions.checkArgument(identifier.namespace().levels().length == 1, "Namespace should be single level");
    return String.format("CREATE TABLE %s.%s STORED BY '%s' %s %s", identifier.namespace(), identifier.name(),
        HiveIcebergStorageHandler.class.getName(), locationForCreateTableSQL(identifier),
            propertiesForCreateTableSQL(tableProps));
  }

  /**
   * Loads the given table from the actual catalog. Overridden by HadoopTables, since the parameter of the
   * {@link Tables#load(String)} should be the full path of the table metadata directory
   * @param identifier The table we want to load
   * @return The Table loaded from the Catalog
   */
  public Table loadTable(TableIdentifier identifier) {
    return tables.load(identifier.toString());
  }

  /**
   * Creates an non partitioned Hive test table. Creates the Iceberg table/data and creates the corresponding Hive
   * table as well when needed. The table will be in the 'default' database. The table will be populated with the
   * provided List of {@link Record}s.
   * @param shell The HiveShell used for Hive table creation
   * @param tableName The name of the test table
   * @param schema The schema used for the table creation
   * @param fileFormat The file format used for writing the data
   * @param records The records with which the table is populated
   * @return The created table
   * @throws IOException If there is an error writing data
   */
  public Table createTable(TestHiveShell shell, String tableName, Schema schema, FileFormat fileFormat,
      List<Record> records) throws IOException {
    Table table = createIcebergTable(shell.getHiveConf(), tableName, schema, fileFormat, records);
    String createHiveSQL = createHiveTableSQL(TableIdentifier.of("default", tableName), ImmutableMap.of());
    if (createHiveSQL != null) {
      shell.executeStatement(createHiveSQL);
    }

    return table;
  }

  /**
   * Creates a partitioned Hive test table using Hive SQL. The table will be in the 'default' database.
   * The table will be populated with the provided List of {@link Record}s using a Hive insert statement.
   * @param shell The HiveShell used for Hive table creation
   * @param tableName The name of the test table
   * @param schema The schema used for the table creation
   * @param spec The partition specification for the table
   * @param fileFormat The file format used for writing the data
   * @param records The records with which the table is populated
   * @return The created table
   * @throws IOException If there is an error writing data
   */
  public Table createTable(TestHiveShell shell, String tableName, Schema schema, PartitionSpec spec,
      FileFormat fileFormat, List<Record> records)  {
    TableIdentifier identifier = TableIdentifier.of("default", tableName);
    shell.executeStatement("CREATE EXTERNAL TABLE " + identifier +
        " STORED BY '" + HiveIcebergStorageHandler.class.getName() + "' " +
        locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(schema) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" +
        PartitionSpecParser.toJson(spec) + "', " +
        "'" + TableProperties.DEFAULT_FILE_FORMAT + "'='" + fileFormat + "', " +
        "'" + InputFormatConfig.CATALOG_NAME + "'='" + catalogName() + "')");

    if (records != null && !records.isEmpty()) {
      StringBuilder query = new StringBuilder().append("INSERT INTO " + identifier + " VALUES ");

      records.forEach(record -> {
        query.append("(");
        query.append(record.struct().fields().stream()
                .map(field -> getStringValueForInsert(record.getField(field.name()), field.type()))
                .collect(Collectors.joining(",")));
        query.append("),");
      });
      query.setLength(query.length() - 1);

      shell.executeStatement(query.toString());
    }

    return loadTable(identifier);
  }

  /**
   * Creates a Hive test table. Creates the Iceberg table/data and creates the corresponding Hive table as well when
   * needed. The table will be in the 'default' database. The table will be populated with the provided with randomly
   * generated {@link Record}s.
   * @param shell The HiveShell used for Hive table creation
   * @param tableName The name of the test table
   * @param schema The schema used for the table creation
   * @param fileFormat The file format used for writing the data
   * @param numRecords The number of records should be generated and stored in the table
   * @throws IOException If there is an error writing data
   */
  public List<Record> createTableWithGeneratedRecords(TestHiveShell shell, String tableName, Schema schema,
      FileFormat fileFormat, int numRecords) throws IOException {
    List<Record> records = TestHelper.generateRandomRecords(schema, numRecords, 0L);
    createTable(shell, tableName, schema, fileFormat, records);
    return records;
  }

  /**
   * Creates an Iceberg table/data without creating the corresponding Hive table. The table will be in the 'default'
   * namespace.
   * @param configuration The configuration used during the table creation
   * @param tableName The name of the test table
   * @param schema The schema used for the table creation
   * @param fileFormat The file format used for writing the data
   * @param records The records with which the table is populated
   * @return The create table
   * @throws IOException If there is an error writing data
   */
  public Table createIcebergTable(Configuration configuration, String tableName, Schema schema, FileFormat fileFormat,
      List<Record> records) throws IOException {
    String identifier = identifier("default." + tableName);
    TestHelper helper = new TestHelper(new Configuration(configuration), tables(), identifier, schema,
        PartitionSpec.unpartitioned(), fileFormat, temp);
    Table table = helper.createTable();

    if (records != null && !records.isEmpty()) {
      helper.appendToTable(helper.writeFile(null, records));
    }

    return table;
  }

  /**
   * Append more data to the table.
   * @param configuration The configuration used during the table creation
   * @param table The table to append
   * @param format The file format used for writing the data
   * @param partition The partition to write to
   * @param records The records with which should be added to the table
   * @throws IOException If there is an error writing data
   */
  public void appendIcebergTable(Configuration configuration, Table table, FileFormat format, StructLike partition,
      List<Record> records) throws IOException {
    TestHelper helper = new TestHelper(
        configuration, null, null, null, null, format, temp);

    helper.setTable(table);
    if (!records.isEmpty()) {
      helper.appendToTable(helper.writeFile(partition, records));
    }
  }

  /**
   * Truncates an Iceberg table.
   * @param table The iceberg table to truncate
   */
  public void truncateIcebergTable(Table table) {
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
  }

  private static class CatalogToTables implements Tables {

    private final Catalog catalog;

    private CatalogToTables(Catalog catalog) {
      this.catalog = catalog;
    }

    @Override
    public Table create(Schema schema, PartitionSpec spec, SortOrder sortOrder,
                        Map<String, String> properties, String tableIdentifier) {
      TableIdentifier tableIdent = TableIdentifier.parse(tableIdentifier);
      return catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withSortOrder(sortOrder)
          .withProperties(properties)
          .create();
    }

    @Override
    public Table load(String tableIdentifier) {
      return catalog.loadTable(TableIdentifier.parse(tableIdentifier));
    }

    @Override
    public boolean exists(String tableIdentifier) {
      return catalog.tableExists(TableIdentifier.parse(tableIdentifier));
    }
  }

  static class CustomCatalogTestTables extends TestTables {

    private final String warehouseLocation;

    CustomCatalogTestTables(Configuration conf, TemporaryFolder temp, String catalogName) throws IOException {
      this(conf, temp, (MetastoreUtil.hive3PresentOnClasspath() ? "file:" : "") +
          temp.newFolder("custom", "warehouse").toString(), catalogName);
    }

    CustomCatalogTestTables(Configuration conf, TemporaryFolder temp, String warehouseLocation, String catalogName) {
      super(new TestCatalogs.CustomHadoopCatalog(conf, warehouseLocation), temp, catalogName);
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(
              InputFormatConfig.catalogPropertyConfigKey(catalog, CatalogProperties.CATALOG_IMPL),
              TestCatalogs.CustomHadoopCatalog.class.getName(),
              InputFormatConfig.catalogPropertyConfigKey(catalog, CatalogProperties.WAREHOUSE_LOCATION),
              warehouseLocation
      );
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + warehouseLocation + TestTables.tablePath(identifier) + "' ";
    }

  }

  static class HadoopCatalogTestTables extends TestTables {

    private final String warehouseLocation;

    HadoopCatalogTestTables(Configuration conf, TemporaryFolder temp, String catalogName) throws IOException {
      this(conf, temp, (MetastoreUtil.hive3PresentOnClasspath() ? "file:" : "") +
          temp.newFolder("hadoop", "warehouse").toString(), catalogName);
    }

    HadoopCatalogTestTables(Configuration conf, TemporaryFolder temp, String warehouseLocation, String catalogName) {
      super(new HadoopCatalog(conf, warehouseLocation), temp, catalogName);
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(
          InputFormatConfig.catalogPropertyConfigKey(catalog, CatalogUtil.ICEBERG_CATALOG_TYPE),
          CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
          InputFormatConfig.catalogPropertyConfigKey(catalog, CatalogProperties.WAREHOUSE_LOCATION),
          warehouseLocation
      );
    }

    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + warehouseLocation + TestTables.tablePath(identifier) + "' ";
    }
  }

  static class HadoopTestTables extends TestTables {
    HadoopTestTables(Configuration conf, TemporaryFolder temp) {
      super(new HadoopTables(conf), temp, Catalogs.ICEBERG_HADOOP_TABLE_NAME);
    }

    @Override
    public String identifier(String tableIdentifier) {
      final File location;

      try {
        TableIdentifier identifier = TableIdentifier.parse(tableIdentifier);
        location = temp.newFolder(ObjectArrays.concat(identifier.namespace().levels(), identifier.name()));
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }

      Assert.assertTrue(location.delete());
      return location.toString();
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + temp.getRoot().getPath() + tablePath(identifier) + "' ";
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return tables().load(temp.getRoot().getPath() + TestTables.tablePath(identifier));
    }

  }

  static class HiveTestTables extends TestTables {

    HiveTestTables(Configuration conf, TemporaryFolder temp, String catalogName) {
      super(CatalogUtil.loadCatalog(HiveCatalog.class.getName(), CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
              ImmutableMap.of(), conf), temp, catalogName);
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(InputFormatConfig.catalogPropertyConfigKey(catalog, CatalogUtil.ICEBERG_CATALOG_TYPE),
          CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "";
    }

    @Override
    public String createHiveTableSQL(TableIdentifier identifier, Map<String, String> tblProps) {
      return null;
    }
  }

  private static String tablePath(TableIdentifier identifier) {
    return "/" + Joiner.on("/").join(identifier.namespace().levels()) + "/" + identifier.name();
  }

  private String getStringValueForInsert(Object value, Type type) {
    String template = "\'%s\'";
    if (type.equals(Types.TimestampType.withoutZone())) {
      return String.format(template, Timestamp.valueOf((LocalDateTime) value).toString());
    } else if (type.equals(Types.TimestampType.withZone())) {
      return String.format(template, Timestamp.from(((OffsetDateTime) value).toInstant()).toString());
    } else if (type.equals(Types.BooleanType.get())) {
      // in hive2 boolean type values must not be surrounded in apostrophes. Otherwise the value is translated to true.
      return value.toString();
    } else {
      return String.format(template, value.toString());
    }
  }

  enum TestTableType {
    HADOOP_TABLE {
      public TestTables instance(Configuration conf, TemporaryFolder temporaryFolder, String catalogName) {
        return new HadoopTestTables(conf, temporaryFolder);
      }
    },
    HADOOP_CATALOG {
      public TestTables instance(Configuration conf, TemporaryFolder temporaryFolder, String catalogName)
          throws IOException {
        return new HadoopCatalogTestTables(conf, temporaryFolder, catalogName);
      }
    },
    CUSTOM_CATALOG {
      public TestTables instance(Configuration conf, TemporaryFolder temporaryFolder, String catalogName)
          throws IOException {
        return new CustomCatalogTestTables(conf, temporaryFolder, catalogName);
      }
    },
    HIVE_CATALOG {
      public TestTables instance(Configuration conf, TemporaryFolder temporaryFolder, String catalogName) {
        return new HiveTestTables(conf, temporaryFolder, catalogName);
      }
    };

    public abstract TestTables instance(Configuration conf, TemporaryFolder temporaryFolder, String catalogName)
        throws IOException;
  }
}
