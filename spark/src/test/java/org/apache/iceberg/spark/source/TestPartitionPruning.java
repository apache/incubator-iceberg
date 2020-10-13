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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestPartitionPruning {

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet", false },
        new Object[] { "parquet", true },
        new Object[] { "avro", false },
        new Object[] { "orc", false },
        new Object[] { "orc", true },
    };
  }

  private final String format;
  private final boolean vectorized;

  public TestPartitionPruning(String format, boolean vectorized) {
    this.format = format;
    this.vectorized = vectorized;
  }

  private static SparkSession spark = null;
  private static JavaSparkContext sparkContext = null;

  private static Transform<Object, Integer> bucketTransform = Transforms.bucket(Types.IntegerType.get(), 3);
  private static Transform<Object, Object> truncateTransform = Transforms.truncate(Types.StringType.get(), 5);
  private static Transform<Object, Integer> hourTransform = Transforms.hour(Types.TimestampType.withoutZone());

  @BeforeClass
  public static void startSpark() {
    TestPartitionPruning.spark = SparkSession.builder().master("local[2]").getOrCreate();
    TestPartitionPruning.sparkContext = new JavaSparkContext(spark.sparkContext());

    String optionKey = String.format("fs.%s.impl", CountOpenLocalFileSystem.scheme);
    CONF.set(optionKey, CountOpenLocalFileSystem.class.getName());
    spark.conf().set(optionKey, CountOpenLocalFileSystem.class.getName());
    spark.conf().set("spark.sql.session.timeZone", "UTC");
    spark.udf().register("bucket3", (Integer num) -> bucketTransform.apply(num), DataTypes.IntegerType);
    spark.udf().register("truncate5", (String str) -> truncateTransform.apply(str), DataTypes.StringType);
    // NOTE: date transforms take the type long, not Timestamp
    spark.udf().register("hour", (Timestamp ts) -> hourTransform.apply(
        org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp(ts)),
        DataTypes.IntegerType);
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestPartitionPruning.spark;
    TestPartitionPruning.spark = null;
    currentSpark.stop();
  }

  private static final Schema LOG_SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "date", Types.StringType.get()),
      Types.NestedField.optional(3, "level", Types.StringType.get()),
      Types.NestedField.optional(4, "message", Types.StringType.get()),
      Types.NestedField.optional(5, "timestamp", Types.TimestampType.withZone())
  );

  private static final List<LogMessage> LOGS = ImmutableList.of(
      LogMessage.debug("2020-02-02", "debug event 1", getInstant("2020-02-02T00:00:00")),
      LogMessage.info("2020-02-02", "info event 1", getInstant("2020-02-02T01:00:00")),
      LogMessage.debug("2020-02-02", "debug event 2", getInstant("2020-02-02T02:00:00")),
      LogMessage.info("2020-02-03", "info event 2", getInstant("2020-02-03T00:00:00")),
      LogMessage.debug("2020-02-03", "debug event 3", getInstant("2020-02-03T01:00:00")),
      LogMessage.info("2020-02-03", "info event 3", getInstant("2020-02-03T02:00:00")),
      LogMessage.error("2020-02-03", "error event 1", getInstant("2020-02-03T03:00:00")),
      LogMessage.debug("2020-02-04", "debug event 4", getInstant("2020-02-04T01:00:00")),
      LogMessage.warn("2020-02-04", "warn event 1", getInstant("2020-02-04T02:00:00")),
      LogMessage.debug("2020-02-04", "debug event 5", getInstant("2020-02-04T03:00:00"))
  );

  private static Instant getInstant(String timestampWithoutZone) {
    Long epochMicros = (Long) Literal.of(timestampWithoutZone).to(Types.TimestampType.withoutZone()).value();
    return Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(epochMicros));
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private PartitionSpec spec = PartitionSpec.builderFor(LOG_SCHEMA)
      .identity("date")
      .identity("level")
      .bucket("id", 3)
      .truncate("message", 5)
      .hour("timestamp")
      .build();

  @Test
  public void testPartitionPruningIdentityString() {
    String filterCond = "date >= '2020-02-03' AND level = 'DEBUG'";
    Predicate<Row> partCondition = (Row r) -> {
      String date = r.getString(0);
      String level = r.getString(1);
      return date.compareTo("2020-02-03") >= 0 && level.equals("DEBUG");
    };

    runTest(filterCond, partCondition);
  }

  @Test
  public void testPartitionPruningBucketingInteger() {
    final int[] ids = new int[]{
        LOGS.get(3).getId(),
        LOGS.get(7).getId()
    };
    String condForIds = Arrays.stream(ids).mapToObj(String::valueOf)
        .collect(Collectors.joining(",", "(", ")"));
    String filterCond = "id in " + condForIds;
    Predicate<Row> partCondition = (Row r) -> {
      int bucketId = r.getInt(2);
      Set<Integer> buckets = Arrays.stream(ids).map(bucketTransform::apply)
          .boxed().collect(Collectors.toSet());
      return buckets.contains(bucketId);
    };

    runTest(filterCond, partCondition);
  }

  @Test
  public void testPartitionPruningTruncatedString() {
    String filterCond = "message like 'info event%'";
    Predicate<Row> partCondition = (Row r) -> {
      String truncatedMessage = r.getString(3);
      return truncatedMessage.equals("info ");
    };

    runTest(filterCond, partCondition);
  }

  @Test
  public void testPartitionPruningTruncatedStringComparingValueShorterThanPartitionValue() {
    String filterCond = "message like 'inf%'";
    Predicate<Row> partCondition = (Row r) -> {
      String truncatedMessage = r.getString(3);
      return truncatedMessage.startsWith("inf");
    };

    runTest(filterCond, partCondition);
  }

  @Test
  public void testPartitionPruningHourlyPartition() {
    String filterCond;
    if (spark.version().startsWith("2")) {
      // Looks like from Spark 2 we need to compare timestamp with timestamp to push down the filter.
      filterCond = "timestamp >= to_timestamp('2020-02-03T01:00:00')";
    } else {
      filterCond = "timestamp >= '2020-02-03T01:00:00'";
    }
    Predicate<Row> partCondition = (Row r) -> {
      int hourValue = r.getInt(4);
      Instant instant = getInstant("2020-02-03T01:00:00");
      Integer hourValueToFilter = hourTransform.apply(TimeUnit.MILLISECONDS.toMicros(instant.toEpochMilli()));
      return hourValue >= hourValueToFilter;
    };

    runTest(filterCond, partCondition);
  }

  private void runTest(String filterCond, Predicate<Row> partCondition) {
    File originTableLocation = createTempDir();
    Assert.assertTrue("Temp folder should exist", originTableLocation.exists());

    Table table = createTable(originTableLocation);
    Dataset<Row> logs = createTestDataset();
    saveTestDatasetToTable(logs, table);

    List<Row> expected = logs
        .select("id", "date", "level", "message", "timestamp")
        .filter(filterCond)
        .orderBy("id")
        .collectAsList();
    Assert.assertFalse("Expected rows should be not empty", expected.isEmpty());

    // remove records which may be recorded during storing to table
    CountOpenLocalFileSystem.resetRecordsInPathPrefix(originTableLocation.getAbsolutePath());

    List<Row> actual = spark.read()
        .format("iceberg")
        .option("vectorization-enabled", String.valueOf(vectorized))
        .load(table.location())
        .select("id", "date", "level", "message", "timestamp")
        .filter(filterCond)
        .orderBy("id")
        .collectAsList();
    Assert.assertFalse("Actual rows should not be empty", actual.isEmpty());

    Assert.assertEquals("Rows should match", expected, actual);

    assertAccessOnDataFiles(originTableLocation, table, partCondition);
  }

  private File createTempDir() {
    try {
      return temp.newFolder();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Table createTable(File originTableLocation) {
    String trackedTableLocation = CountOpenLocalFileSystem.convertPath(originTableLocation);
    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format);
    return TABLES.create(LOG_SCHEMA, spec, properties, trackedTableLocation);
  }

  private Dataset<Row> createTestDataset() {
    List<InternalRow> rows = LOGS.stream().map(logMessage -> {
      Object[] underlying = new Object[] {
          logMessage.getId(),
          UTF8String.fromString(logMessage.getDate()),
          UTF8String.fromString(logMessage.getLevel()),
          UTF8String.fromString(logMessage.getMessage()),
          // discard the nanoseconds part to simplify
          TimeUnit.MILLISECONDS.toMicros(logMessage.getTimestamp().toEpochMilli())
      };
      return new GenericInternalRow(underlying);
    }).collect(Collectors.toList());

    JavaRDD<InternalRow> rdd = sparkContext.parallelize(rows);
    Dataset<Row> df = spark.internalCreateDataFrame(JavaRDD.toRDD(rdd), SparkSchemaUtil.convert(LOG_SCHEMA), false);

    return df
        .selectExpr("id", "date", "level", "message", "timestamp")
        .selectExpr("id", "date", "level", "message", "timestamp", "bucket3(id) AS bucket_id",
            "truncate5(message) AS truncated_message", "hour(timestamp) AS ts_hour");
  }

  private void saveTestDatasetToTable(Dataset<Row> logs, Table table) {
    logs.orderBy("date", "level", "bucket_id", "truncated_message", "ts_hour")
        .select("id", "date", "level", "message", "timestamp")
        .write().format("iceberg").mode("append").save(table.location());
  }

  private void assertAccessOnDataFiles(File originTableLocation, Table table, Predicate<Row> partCondition) {
    // only use files in current table location to avoid side-effects on concurrent test runs
    Set<String> readFilesInQuery = CountOpenLocalFileSystem.pathToNumOpenCalled.keySet()
        .stream().filter(path -> path.startsWith(originTableLocation.getAbsolutePath()))
        .collect(Collectors.toSet());

    List<Row> files = spark.read().format("iceberg").load(table.location() + "#files").collectAsList();

    Set<String> filesToRead = extractFilePathsMatchingConditionOnPartition(files, partCondition);
    Set<String> filesToNotRead = extractFilePathsNotIn(files, filesToRead);

    // Just to be sure, they should be mutually exclusive.
    Assert.assertTrue(Sets.intersection(filesToRead, filesToNotRead).isEmpty());

    Assert.assertFalse("The query should prune some data files.", filesToNotRead.isEmpty());

    // We don't check "all" data files bound to the condition are being read, as data files can be pruned on
    // other conditions like lower/upper bound of columns.
    Assert.assertFalse("Some of data files in partition range should be read. " +
        "Read files in query: " + readFilesInQuery + " / data files in partition range: " + filesToRead,
        Sets.intersection(filesToRead, readFilesInQuery).isEmpty());

    // Data files which aren't bound to the condition shouldn't be read.
    Assert.assertTrue("Data files outside of partition range should not be read. " +
        "Read files in query: " + readFilesInQuery + " / data files outside of partition range: " + filesToNotRead,
        Sets.intersection(filesToNotRead, readFilesInQuery).isEmpty());
  }

  private Set<String> extractFilePathsMatchingConditionOnPartition(List<Row> files, Predicate<Row> condition) {
    // idx 1: file_path, idx 3: partition
    return files.stream()
        .filter(r -> {
          Row partition = r.getStruct(3);
          return condition.test(partition);
        }).map(r -> CountOpenLocalFileSystem.stripScheme(r.getString(1)))
        .collect(Collectors.toSet());
  }

  private Set<String> extractFilePathsNotIn(List<Row> files, Set<String> filePaths) {
    Set<String> allFilePaths = files.stream().map(r -> CountOpenLocalFileSystem.stripScheme(r.getString(1)))
        .collect(Collectors.toSet());
    return new HashSet<>(Sets.symmetricDifference(allFilePaths, filePaths));
  }

  public static class CountOpenLocalFileSystem extends RawLocalFileSystem {
    public static String scheme = String.format("TestIdentityPartitionData%dfs",
        new Random().nextInt());
    public static ConcurrentHashMap<String, Long> pathToNumOpenCalled = new ConcurrentHashMap<>();

    public static String convertPath(String absPath) {
      return scheme + "://" + absPath;
    }

    public static String convertPath(File file) {
      return convertPath(file.getAbsolutePath());
    }

    public static String stripScheme(String pathWithScheme) {
      if (!pathWithScheme.startsWith(scheme + ":")) {
        throw new IllegalArgumentException("Received unexpected path: " + pathWithScheme);
      }

      int idxToCut = scheme.length() + 1;
      while (pathWithScheme.charAt(idxToCut) == '/') {
        idxToCut++;
      }

      // leave the last '/'
      idxToCut--;

      return pathWithScheme.substring(idxToCut);
    }

    public static void resetRecordsInPathPrefix(String pathPrefix) {
      pathToNumOpenCalled.keySet().stream()
          .filter(p -> p.startsWith(pathPrefix))
          .forEach(key -> pathToNumOpenCalled.remove(key));
    }

    @Override
    public URI getUri() {
      return URI.create(scheme + ":///");
    }

    @Override
    public String getScheme() {
      return scheme;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      String path = f.toUri().getPath();
      pathToNumOpenCalled.compute(path, (ignored, v) -> {
        if (v == null) {
          return 1L;
        } else {
          return v + 1;
        }
      });
      return super.open(f, bufferSize);
    }
  }
}
