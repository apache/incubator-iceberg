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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;
import scala.Option;
import scala.Predef;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

import static org.apache.spark.sql.functions.col;

/**
 * Java version of the original SparkTableUtil.scala
 * https://github.com/apache/iceberg/blob/apache-iceberg-0.8.0-incubating/spark/src/main/scala/org/apache/iceberg/spark/SparkTableUtil.scala
 */
public class SparkTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTableUtil.class);

  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on(",").withKeyValueSeparator("=");

  private static final PathFilter HIDDEN_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  private static final String duplicateFileMessage = "Cannot complete import because data files " +
      "to be imported already exist within the target table: %s.  " +
      "This is disabled by default as Iceberg is not designed for mulitple references to the same file" +
      " within the same table.  If you are sure, you may set 'check_duplicate_files' to false to force the import.";


  private SparkTableUtil() {
  }

  /**
   * Returns a DataFrame with a row for each partition in the table.
   *
   * The DataFrame has 3 columns, partition key (a=1/b=2), partition location, and format
   * (avro or parquet).
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @return a DataFrame of the table's partitions
   */
  public static Dataset<Row> partitionDF(SparkSession spark, String table) {
    List<SparkPartition> partitions = getPartitions(spark, table);
    return spark.createDataFrame(partitions, SparkPartition.class).toDF("partition", "uri", "format");
  }

  /**
   * Returns a DataFrame with a row for each partition that matches the specified 'expression'.
   *
   * @param spark a Spark session.
   * @param table name of the table.
   * @param expression The expression whose matching partitions are returned.
   * @return a DataFrame of the table partitions.
   */
  public static Dataset<Row> partitionDFByFilter(SparkSession spark, String table, String expression) {
    List<SparkPartition> partitions = getPartitionsByFilter(spark, table, expression);
    return spark.createDataFrame(partitions, SparkPartition.class).toDF("partition", "uri", "format");
  }

  /**
   * Returns all partitions in the table.
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @return all table's partitions
   */
  public static List<SparkPartition> getPartitions(SparkSession spark, String table) {
    try {
      TableIdentifier tableIdent = spark.sessionState().sqlParser().parseTableIdentifier(table);
      return getPartitions(spark, tableIdent, null);
    } catch (ParseException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unable to parse table identifier: %s", table);
    }
  }

  /**
   * Returns all partitions in the table.
   *
   * @param spark a Spark session
   * @param tableIdent a table identifier
   * @param partitionFilter partition filter, or null if no filter
   * @return all table's partitions
   */
  public static List<SparkPartition> getPartitions(SparkSession spark, TableIdentifier tableIdent,
                                                   Map<String, String> partitionFilter) {
    try {
      SessionCatalog catalog = spark.sessionState().catalog();
      CatalogTable catalogTable = catalog.getTableMetadata(tableIdent);

      Option<scala.collection.immutable.Map<String, String>> scalaPartitionFilter;
      if (partitionFilter != null && !partitionFilter.isEmpty()) {
        scalaPartitionFilter = Option.apply(JavaConverters.mapAsScalaMapConverter(partitionFilter).asScala()
            .toMap(Predef.conforms()));
      } else {
        scalaPartitionFilter = Option.empty();
      }
      Seq<CatalogTablePartition> partitions = catalog.listPartitions(tableIdent, scalaPartitionFilter);
      return JavaConverters
          .seqAsJavaListConverter(partitions)
          .asJava()
          .stream()
          .map(catalogPartition -> toSparkPartition(catalogPartition, catalogTable))
          .collect(Collectors.toList());
    } catch (NoSuchDatabaseException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unknown table: %s. Database not found in catalog.", tableIdent);
    } catch (NoSuchTableException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unknown table: %s. Table not found in catalog.", tableIdent);
    }
  }

  /**
   * Returns partitions that match the specified 'predicate'.
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @param predicate a predicate on partition columns
   * @return matching table's partitions
   */
  public static List<SparkPartition> getPartitionsByFilter(SparkSession spark, String table, String predicate) {
    TableIdentifier tableIdent;
    try {
      tableIdent = spark.sessionState().sqlParser().parseTableIdentifier(table);
    } catch (ParseException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unable to parse the table identifier: %s", table);
    }

    Expression unresolvedPredicateExpr;
    try {
      unresolvedPredicateExpr = spark.sessionState().sqlParser().parseExpression(predicate);
    } catch (ParseException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unable to parse the predicate expression: %s", predicate);
    }

    Expression resolvedPredicateExpr = resolveAttrs(spark, table, unresolvedPredicateExpr);
    return getPartitionsByFilter(spark, tableIdent, resolvedPredicateExpr);
  }

  /**
   * Returns partitions that match the specified 'predicate'.
   *
   * @param spark a Spark session
   * @param tableIdent a table identifier
   * @param predicateExpr a predicate expression on partition columns
   * @return matching table's partitions
   */
  public static List<SparkPartition> getPartitionsByFilter(SparkSession spark, TableIdentifier tableIdent,
                                                           Expression predicateExpr) {
    try {
      SessionCatalog catalog = spark.sessionState().catalog();
      CatalogTable catalogTable = catalog.getTableMetadata(tableIdent);

      Expression resolvedPredicateExpr;
      if (!predicateExpr.resolved()) {
        resolvedPredicateExpr = resolveAttrs(spark, tableIdent.quotedString(), predicateExpr);
      } else {
        resolvedPredicateExpr = predicateExpr;
      }
      Seq<Expression> predicates = JavaConverters
          .collectionAsScalaIterableConverter(ImmutableList.of(resolvedPredicateExpr))
          .asScala().toSeq();

      Seq<CatalogTablePartition> partitions = catalog.listPartitionsByFilter(tableIdent, predicates);

      return JavaConverters
          .seqAsJavaListConverter(partitions)
          .asJava()
          .stream()
          .map(catalogPartition -> toSparkPartition(catalogPartition, catalogTable))
          .collect(Collectors.toList());
    } catch (NoSuchDatabaseException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unknown table: %s. Database not found in catalog.", tableIdent);
    } catch (NoSuchTableException e) {
      throw SparkExceptionUtil.toUncheckedException(e, "Unknown table: %s. Table not found in catalog.", tableIdent);
    }
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet and ORC partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   * @deprecated use {@link TableMigrationUtil#listPartition(Map, String, String, PartitionSpec, Configuration,
   * MetricsConfig, NameMapping)}
   *
   * @param partition a partition
   * @param conf a serializable Hadoop conf
   * @param metricsConfig a metrics conf
   * @return a List of DataFile
   */
  @Deprecated
  public static List<DataFile> listPartition(SparkPartition partition, PartitionSpec spec,
                                             SerializableConfiguration conf, MetricsConfig metricsConfig) {
    return listPartition(partition, spec, conf, metricsConfig, null);
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet and ORC partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   * @deprecated use {@link TableMigrationUtil#listPartition(Map, String, String, PartitionSpec, Configuration,
   * MetricsConfig, NameMapping)}
   *
   * @param partition a partition
   * @param conf a serializable Hadoop conf
   * @param metricsConfig a metrics conf
   * @param mapping a name mapping
   * @return a List of DataFile
   */
  @Deprecated
  public static List<DataFile> listPartition(SparkPartition partition, PartitionSpec spec,
                                             SerializableConfiguration conf, MetricsConfig metricsConfig,
                                             NameMapping mapping) {
    return TableMigrationUtil.listPartition(partition.values, partition.uri, partition.format, spec, conf.get(),
        metricsConfig, mapping);
  }


  private static SparkPartition toSparkPartition(CatalogTablePartition partition, CatalogTable table) {
    Option<URI> locationUri = partition.storage().locationUri();
    Option<String> serde = partition.storage().serde();

    Preconditions.checkArgument(locationUri.nonEmpty(), "Partition URI should be defined");
    Preconditions.checkArgument(serde.nonEmpty() || table.provider().nonEmpty(),
        "Partition format should be defined");

    String uri = Util.uriToString(locationUri.get());
    String format = serde.nonEmpty() ? serde.get() : table.provider().get();

    Map<String, String> partitionSpec = JavaConverters.mapAsJavaMapConverter(partition.spec()).asJava();
    return new SparkPartition(partitionSpec, uri, format);
  }

  private static Expression resolveAttrs(SparkSession spark, String table, Expression expr) {
    Function2<String, String, Object> resolver = spark.sessionState().analyzer().resolver();
    LogicalPlan plan = spark.table(table).queryExecution().analyzed();
    return expr.transform(new AbstractPartialFunction<Expression, Expression>() {
      @Override
      public Expression apply(Expression attr) {
        UnresolvedAttribute unresolvedAttribute = (UnresolvedAttribute) attr;
        Option<NamedExpression> namedExpressionOption = plan.resolve(unresolvedAttribute.nameParts(), resolver);
        if (namedExpressionOption.isDefined()) {
          return (Expression) namedExpressionOption.get();
        } else {
          throw new IllegalArgumentException(
              String.format("Could not resolve %s using columns: %s", attr, plan.output()));
        }
      }

      @Override
      public boolean isDefinedAt(Expression attr) {
        return attr instanceof UnresolvedAttribute;
      }
    });
  }

  private static Iterator<ManifestFile> buildManifest(SerializableConfiguration conf, PartitionSpec spec,
                                                      String basePath, Iterator<Tuple2<String, DataFile>> fileTuples) {
    if (fileTuples.hasNext()) {
      FileIO io = new HadoopFileIO(conf.get());
      TaskContext ctx = TaskContext.get();
      String suffix = String.format("stage-%d-task-%d-manifest", ctx.stageId(), ctx.taskAttemptId());
      Path location = new Path(basePath, suffix);
      String outputPath = FileFormat.AVRO.addExtension(location.toString());
      OutputFile outputFile = io.newOutputFile(outputPath);
      ManifestWriter<DataFile> writer = ManifestFiles.write(spec, outputFile);

      try (ManifestWriter<DataFile> writerRef = writer) {
        fileTuples.forEachRemaining(fileTuple -> writerRef.add(fileTuple._2));
      } catch (IOException e) {
        throw SparkExceptionUtil.toUncheckedException(e, "Unable to close the manifest writer: %s", outputPath);
      }

      ManifestFile manifestFile = writer.toManifestFile();
      return ImmutableList.of(manifestFile).iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  /**
   * Import files from an existing Spark table to an Iceberg table.
   *
   * The import uses the Spark session to get table metadata. It assumes no
   * operation is going on the original and target table and thus is not
   * thread-safe.
   *
   * @param spark a Spark session
   * @param sourceTableIdent an identifier of the source Spark table
   * @param targetTable an Iceberg table where to import the data
   * @param stagingDir a staging directory to store temporary manifest files
   * @param partitionFilter only import partitions whose values match those in the map, can be partially defined
   * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
   */
  public static void importSparkTable(SparkSession spark, TableIdentifier sourceTableIdent, Table targetTable,
                                      String stagingDir, Map<String, String> partitionFilter,
                                      boolean checkDuplicateFiles) {
    SessionCatalog catalog = spark.sessionState().catalog();

    String db = sourceTableIdent.database().nonEmpty() ?
        sourceTableIdent.database().get() :
        catalog.getCurrentDatabase();
    TableIdentifier sourceTableIdentWithDB = new TableIdentifier(sourceTableIdent.table(), Some.apply(db));

    if (!catalog.tableExists(sourceTableIdentWithDB)) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException("Table %s does not exist", sourceTableIdentWithDB);
    }

    try {
      PartitionSpec spec = SparkSchemaUtil.specForTable(spark, sourceTableIdentWithDB.unquotedString());

      if (Objects.equal(spec, PartitionSpec.unpartitioned())) {
        importUnpartitionedSparkTable(spark, sourceTableIdentWithDB, targetTable, checkDuplicateFiles);
      } else {
        List<SparkPartition> sourceTablePartitions = getPartitions(spark, sourceTableIdent,
            partitionFilter);
        Preconditions.checkArgument(!sourceTablePartitions.isEmpty(),
            "Cannot find any partitions in table %s", sourceTableIdent);
        importSparkPartitions(spark, sourceTablePartitions, targetTable, spec, stagingDir, checkDuplicateFiles);
      }
    } catch (AnalysisException e) {
      throw SparkExceptionUtil.toUncheckedException(
          e, "Unable to get partition spec for table: %s", sourceTableIdentWithDB);
    }
  }

  /**
   * Import files from an existing Spark table to an Iceberg table.
   *
   * The import uses the Spark session to get table metadata. It assumes no
   * operation is going on the original and target table and thus is not
   * thread-safe.
   *
   * @param spark a Spark session
   * @param sourceTableIdent an identifier of the source Spark table
   * @param targetTable an Iceberg table where to import the data
   * @param stagingDir a staging directory to store temporary manifest files
   * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
   */
  public static void importSparkTable(SparkSession spark, TableIdentifier sourceTableIdent, Table targetTable,
                                      String stagingDir, boolean checkDuplicateFiles) {
    importSparkTable(spark, sourceTableIdent, targetTable, stagingDir, Collections.emptyMap(), checkDuplicateFiles);
  }

  /**
   * Import files from an existing Spark table to an Iceberg table.
   *
   * The import uses the Spark session to get table metadata. It assumes no
   * operation is going on the original and target table and thus is not
   * thread-safe.
   * @param spark a Spark session
   * @param sourceTableIdent an identifier of the source Spark table
   * @param targetTable an Iceberg table where to import the data
   * @param stagingDir a staging directory to store temporary manifest files
   */
  public static void importSparkTable(SparkSession spark, TableIdentifier sourceTableIdent, Table targetTable,
                                      String stagingDir) {
    importSparkTable(spark, sourceTableIdent, targetTable, stagingDir, Collections.emptyMap(), false);
  }

  private static void importUnpartitionedSparkTable(SparkSession spark, TableIdentifier sourceTableIdent,
                                                    Table targetTable, boolean checkDuplicateFiles) {
    try {
      CatalogTable sourceTable = spark.sessionState().catalog().getTableMetadata(sourceTableIdent);
      Option<String> format =
          sourceTable.storage().serde().nonEmpty() ? sourceTable.storage().serde() : sourceTable.provider();
      Preconditions.checkArgument(format.nonEmpty(), "Could not determine table format");

      Map<String, String> partition = Collections.emptyMap();
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Configuration conf = spark.sessionState().newHadoopConf();
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(targetTable.properties());
      String nameMappingString = targetTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

      List<DataFile> files = TableMigrationUtil.listPartition(
          partition, Util.uriToString(sourceTable.location()), format.get(), spec, conf, metricsConfig, nameMapping);

      if (checkDuplicateFiles) {
        Dataset<Row> importedFiles = spark.createDataset(
            Lists.transform(files, f -> f.path().toString()), Encoders.STRING()).toDF("file_path");
        Dataset<Row> existingFiles = loadMetadataTable(spark, targetTable, MetadataTableType.ENTRIES);
        Column joinCond = existingFiles.col("data_file.file_path").equalTo(importedFiles.col("file_path"));
        Dataset<Row> duplicates = importedFiles.join(existingFiles, joinCond);
        List<String> duplicateList = duplicates.takeAsList(10)
            .stream().map(p -> p.getString(0)).collect(Collectors.toList());
        Preconditions.checkState(duplicateList.isEmpty(),
            String.format(duplicateFileMessage, Joiner.on(",").join(duplicateList)));
      }

      AppendFiles append = targetTable.newAppend();
      files.forEach(append::appendFile);
      append.commit();
    } catch (NoSuchDatabaseException e) {
      throw SparkExceptionUtil.toUncheckedException(
          e, "Unknown table: %s. Database not found in catalog.", sourceTableIdent);
    } catch (NoSuchTableException e) {
      throw SparkExceptionUtil.toUncheckedException(
          e, "Unknown table: %s. Table not found in catalog.", sourceTableIdent);
    }
  }

  /**
   * Import files from given partitions to an Iceberg table.
   *
   * @param spark a Spark session
   * @param partitions partitions to import
   * @param targetTable an Iceberg table where to import the data
   * @param spec a partition spec
   * @param stagingDir a staging directory to store temporary manifest files
   * @param checkDuplicateFiles if true, throw exception if import results in a duplicate data file
   */
  public static void importSparkPartitions(SparkSession spark, List<SparkPartition> partitions, Table targetTable,
                                           PartitionSpec spec, String stagingDir, boolean checkDuplicateFiles) {
    Configuration conf = spark.sessionState().newHadoopConf();
    SerializableConfiguration serializableConf = new SerializableConfiguration(conf);
    int parallelism = Math.min(partitions.size(), spark.sessionState().conf().parallelPartitionDiscoveryParallelism());
    int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(targetTable.properties());
    String nameMappingString = targetTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    JavaRDD<SparkPartition> partitionRDD = sparkContext.parallelize(partitions, parallelism);

    Dataset<SparkPartition> partitionDS = spark.createDataset(
        partitionRDD.rdd(),
        Encoders.javaSerialization(SparkPartition.class));

    Dataset<DataFile> filesToImport = partitionDS
        .flatMap((FlatMapFunction<SparkPartition, DataFile>) sparkPartition ->
                listPartition(sparkPartition, spec, serializableConf, metricsConfig, nameMapping).iterator(),
            Encoders.javaSerialization(DataFile.class));

    if (checkDuplicateFiles) {
      Dataset<Row> importedFiles = filesToImport.map(f -> f.path().toString(), Encoders.STRING()).toDF("file_path");
      Dataset<Row> existingFiles = loadMetadataTable(spark, targetTable, MetadataTableType.ENTRIES);
      Column joinCond = existingFiles.col("data_file.file_path").equalTo(importedFiles.col("file_path"));
      Dataset<Row> duplicates = importedFiles.join(existingFiles, joinCond);
      List<String> duplicateList = duplicates.takeAsList(10)
          .stream().map(p -> p.getString(0)).collect(Collectors.toList());
      Preconditions.checkState(duplicateList.isEmpty(),
          String.format(duplicateFileMessage, Joiner.on(",").join(duplicateList)));
    }

    List<ManifestFile> manifests = filesToImport
        .repartition(numShufflePartitions)
        .map((MapFunction<DataFile, Tuple2<String, DataFile>>) file ->
                Tuple2.apply(file.path().toString(), file),
            Encoders.tuple(Encoders.STRING(), Encoders.javaSerialization(DataFile.class)))
        .orderBy(col("_1"))
        .mapPartitions(
            (MapPartitionsFunction<Tuple2<String, DataFile>, ManifestFile>) fileTuple ->
                buildManifest(serializableConf, spec, stagingDir, fileTuple),
            Encoders.javaSerialization(ManifestFile.class))
        .collectAsList();

    try {
      boolean snapshotIdInheritanceEnabled = PropertyUtil.propertyAsBoolean(
          targetTable.properties(),
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
          TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);

      AppendFiles append = targetTable.newAppend();
      manifests.forEach(append::appendManifest);
      append.commit();

      if (!snapshotIdInheritanceEnabled) {
        // delete original manifests as they were rewritten before the commit
        deleteManifests(targetTable.io(), manifests);
      }
    } catch (Throwable e) {
      deleteManifests(targetTable.io(), manifests);
      throw e;
    }
  }

  /**
   * Import files from given partitions to an Iceberg table.
   *
   * @param spark a Spark session
   * @param partitions partitions to import
   * @param targetTable an Iceberg table where to import the data
   * @param spec a partition spec
   * @param stagingDir a staging directory to store temporary manifest files
   */
  public static void importSparkPartitions(SparkSession spark, List<SparkPartition> partitions, Table targetTable,
                                           PartitionSpec spec, String stagingDir) {
    importSparkPartitions(spark, partitions, targetTable, spec, stagingDir, false);
  }

  public static List<SparkPartition> filterPartitions(List<SparkPartition> partitions,
                                                      Map<String, String> partitionFilter) {
    if (partitionFilter.isEmpty()) {
      return partitions;
    } else {
      return partitions.stream()
          .filter(p -> p.getValues().entrySet().containsAll(partitionFilter.entrySet()))
          .collect(Collectors.toList());
    }
  }

  private static void deleteManifests(FileIO io, List<ManifestFile> manifests) {
    Tasks.foreach(manifests)
        .noRetry()
        .suppressFailureWhenFinished()
        .run(item -> io.deleteFile(item.path()));
  }

  // Attempt to use Spark3 Catalog resolution if available on the path
  private static final DynMethods.UnboundMethod LOAD_CATALOG = DynMethods.builder("loadCatalogMetadataTable")
      .hiddenImpl("org.apache.iceberg.spark.Spark3Util", SparkSession.class, String.class, MetadataTableType.class)
      .orNoop()
      .build();

  public static Dataset<Row> loadCatalogMetadataTable(SparkSession spark, String tableName, MetadataTableType type) {
    Preconditions.checkArgument(!LOAD_CATALOG.isNoop(), "Cannot find Spark3Util class but Spark3 is in use");
    return LOAD_CATALOG.asStatic().invoke(spark, tableName, type);
  }

  public static Dataset<Row> loadMetadataTable(SparkSession spark, Table table, MetadataTableType type) {
    String tableName = table.name();
    String tableLocation = table.location();

    DataFrameReader dataFrameReader = spark.read().format("iceberg");
    if (tableName.contains("/")) {
      // Hadoop Table or Metadata location passed, load without a catalog
      return dataFrameReader.load(tableName + "#" + type);
    }

    // Try DSV2 catalog based name based resolution
    if (spark.version().startsWith("3")) {
      Dataset<Row> catalogMetadataTable = loadCatalogMetadataTable(spark, tableName, type);
      if (catalogMetadataTable != null) {
        return catalogMetadataTable;
      }
    }

    // Catalog based resolution failed, our catalog may be a non-DatasourceV2 Catalog
    if (tableName.startsWith("hadoop.")) {
      // Try loading by location as Hadoop table without Catalog
      return dataFrameReader.load(tableLocation + "#" + type);
    } else if (tableName.startsWith("hive")) {
      // Try loading by name as a Hive table without Catalog
      return dataFrameReader.load(tableName.replaceFirst("hive\\.", "") + "." + type);
    } else {
      throw new IllegalArgumentException(String.format(
          "Cannot find the metadata table for %s of type %s", tableName, type));
    }
  }

  /**
   * Class representing a table partition.
   */
  public static class SparkPartition implements Serializable {
    private final Map<String, String> values;
    private final String uri;
    private final String format;

    public SparkPartition(Map<String, String> values, String uri, String format) {
      this.values = ImmutableMap.copyOf(values);
      this.uri = uri;
      this.format = format;
    }

    public Map<String, String> getValues() {
      return values;
    }

    public String getUri() {
      return uri;
    }

    public String getFormat() {
      return format;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("values", values)
          .add("uri", uri)
          .add("format", format)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SparkPartition that = (SparkPartition) o;
      return Objects.equal(values, that.values) &&
          Objects.equal(uri, that.uri) &&
          Objects.equal(format, that.format);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(values, uri, format);
    }
  }
}
