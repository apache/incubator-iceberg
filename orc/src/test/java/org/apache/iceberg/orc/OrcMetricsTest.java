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

package org.apache.iceberg.orc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrcMetricsTest {
  private static final Random RAND = new Random();

  private final TypeDescription orcSchema;
  private final Schema icebergSchema;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public OrcMetricsTest() {
    orcSchema = TypeDescription.fromString("struct<w:int,x:bigint,y:int,z:double>");
    icebergSchema = ORCSchemaUtil.convert(orcSchema);
  }

  private File writeOrcTestFile(int rows) throws IOException {
    final Configuration conf = new Configuration();

    final File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    final Path testPath = new Path(testFile.toURI().toString());
    final List<Long> optionsLong = ImmutableList.of(10L, 20L, 30L, 100L);
    final List<Double> optionsDouble = ImmutableList.of(1.3, 1.7, 0.21, 2.3, 0.09);

    try (Writer writer = OrcFile.createWriter(testPath,
        OrcFile.writerOptions(conf).setSchema(orcSchema))) {
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      LongColumnVector w = (LongColumnVector) batch.cols[0];
      LongColumnVector x = (LongColumnVector) batch.cols[1];
      LongColumnVector y = (LongColumnVector) batch.cols[2];
      DoubleColumnVector z = (DoubleColumnVector) batch.cols[3];

      for (int r = 0; r < rows; ++r) {
        int row = batch.size++;
        w.vector[row] = r;
        x.vector[row] = optionsLong.get(RAND.nextInt(optionsLong.size()));
        y.vector[row] = r * 3;
        z.vector[row] = optionsDouble.get(RAND.nextInt(optionsDouble.size()));
        // If the batch is full, write it out and start over.
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    return testFile;
  }

  @Test
  public void testOrcMetricsPrimitive() throws IOException {
    final int rows = 10000;
    final File orcTestFile = writeOrcTestFile(rows);

    final Metrics orcMetrics = OrcMetrics.fromInputFile(Files.localInput(orcTestFile));
    assertNotNull(orcMetrics);
    assertEquals(rows, orcMetrics.recordCount().intValue());

    Map<Integer, ?> lowerBounds = fromBufferMap(icebergSchema,
        orcMetrics.lowerBounds());
    assertEquals(0, lowerBounds.get(1));     // w (id = 1)
    assertEquals(10L, lowerBounds.get(2));   // x
    assertEquals(0, lowerBounds.get(3));     // y
    assertEquals(0.09, lowerBounds.get(4));  // z

    Map<Integer, ?> upperBounds = fromBufferMap(icebergSchema,
        orcMetrics.upperBounds());
    assertEquals(rows - 1, upperBounds.get(1));        // w (id = 1)
    assertEquals(100L, upperBounds.get(2));            // x
    assertEquals((rows - 1) * 3, upperBounds.get(3));  // y
    assertEquals(2.3, upperBounds.get(4));             // z
  }

  private Map<Integer, ?> fromBufferMap(Schema schema, Map<Integer, ByteBuffer> map) {
    Map<Integer, ?> values = Maps.newHashMap();
    for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
      values.put(entry.getKey(),
          Conversions.fromByteBuffer(schema.findType(entry.getKey()), entry.getValue()));
    }
    return values;
  }
}
