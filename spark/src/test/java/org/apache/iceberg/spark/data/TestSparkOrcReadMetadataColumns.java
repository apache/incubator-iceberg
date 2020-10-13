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

package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSparkOrcReadMetadataColumns {
  private static final Schema DATA_SCHEMA = new Schema(
      required(100, "id", Types.LongType.get()),
      required(101, "data", Types.StringType.get())
  );

  private static final Schema PROJECTION_SCHEMA = new Schema(
      required(100, "id", Types.LongType.get()),
      required(101, "data", Types.StringType.get()),
      MetadataColumns.ROW_POSITION
  );

  private static final int NUM_ROWS = 1000;
  private static final List<InternalRow> DATA_ROWS;
  private static final List<InternalRow> EXPECTED_ROWS;

  static {
    DATA_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i++) {
      InternalRow row = new GenericInternalRow(DATA_SCHEMA.columns().size());
      row.update(0, i);
      row.update(1, UTF8String.fromString("str" + i));
      DATA_ROWS.add(row);
    }

    EXPECTED_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i++) {
      InternalRow row = new GenericInternalRow(PROJECTION_SCHEMA.columns().size());
      row.update(0, i);
      row.update(1, UTF8String.fromString("str" + i));
      row.update(2, i);
      EXPECTED_ROWS.add(row);
    }
  }

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[] parameters() {
    return new Object[] { false, true };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private boolean vectorized;
  private File testFile;

  public TestSparkOrcReadMetadataColumns(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Before
  public void writeFile() throws IOException {
    testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(DATA_SCHEMA)
        // write in such a way that the file contains 10 stripes each with 100 rows
        .config("iceberg.orc.vectorbatch.size", "100")
        .config(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), "100")
        .config(OrcConf.STRIPE_SIZE.getAttribute(), "1")
        .build()) {
      writer.addAll(DATA_ROWS);
    }
  }

  @Test
  public void testReadRowNumbers() throws IOException {
    readAndValidate(null, null, null, EXPECTED_ROWS);
  }

  @Test
  public void testReadRowNumbersWithFilter() throws IOException {
    readAndValidate(Expressions.greaterThanOrEqual("id", 500), null, null, EXPECTED_ROWS.subList(500, 1000));
  }

  @Test
  public void testReadRowNumbersWithSplits() throws IOException {
    Reader reader;
    try {
      OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration()).useUTCTimestamp(true);
      reader =  OrcFile.createReader(new Path(testFile.toString()), readerOptions);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", testFile);
    }
    List<Long> splitOffsets = reader.getStripes().stream().map(StripeInformation::getOffset)
        .collect(Collectors.toList());
    List<Long> splitLengths = reader.getStripes().stream().map(StripeInformation::getLength)
        .collect(Collectors.toList());

    for (int i = 0; i < 10; i++) {
      readAndValidate(null, splitOffsets.get(i), splitLengths.get(i), EXPECTED_ROWS.subList(i * 100, (i + 1) * 100));
    }
  }

  private void readAndValidate(Expression filter, Long splitStart, Long splitLength, List<InternalRow> expected)
      throws IOException {
    Schema projectionWithoutMetadataFields = TypeUtil.selectNot(PROJECTION_SCHEMA, MetadataColumns.metadataFieldIds());
    CloseableIterable<InternalRow> reader = null;
    try {
      ORC.ReadBuilder builder = ORC.read(Files.localInput(testFile))
          .project(projectionWithoutMetadataFields);

      if (vectorized) {
        builder = builder.createBatchedReaderFunc(readOrcSchema ->
            VectorizedSparkOrcReaders.buildReader(PROJECTION_SCHEMA, readOrcSchema, ImmutableMap.of()));
      } else {
        builder = builder.createReaderFunc(readOrcSchema -> new SparkOrcReader(PROJECTION_SCHEMA, readOrcSchema));
      }

      if (filter != null) {
        builder = builder.filter(filter);
      }

      if (splitStart != null && splitLength != null) {
        builder = builder.split(splitStart, splitLength);
      }

      if (vectorized) {
        reader = batchesToRows(builder.build());
      } else {
        reader = builder.build();
      }

      final Iterator<InternalRow> actualRows = reader.iterator();
      final Iterator<InternalRow> expectedRows = expected.iterator();
      while (expectedRows.hasNext()) {
        Assert.assertTrue("Should have expected number of rows", actualRows.hasNext());
        TestHelpers.assertEquals(PROJECTION_SCHEMA, expectedRows.next(), actualRows.next());
      }
      Assert.assertFalse("Should not have extra rows", actualRows.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private CloseableIterable<InternalRow> batchesToRows(CloseableIterable<ColumnarBatch> batches) {
    return CloseableIterable.combine(
        Iterables.concat(Iterables.transform(batches, b -> (Iterable<InternalRow>) b::rowIterator)),
        batches);
  }
}
