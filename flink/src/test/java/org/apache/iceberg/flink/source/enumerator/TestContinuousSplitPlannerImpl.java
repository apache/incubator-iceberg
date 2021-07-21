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

package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestContinuousSplitPlannerImpl {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();
  private static final AtomicLong randomSeed = new AtomicLong();

  @Rule
  public final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private GenericAppenderHelper dataAppender;
  private DataFile dataFile1;
  private Snapshot snapshot1;

  @Before
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(tableResource.table(), fileFormat, TEMPORARY_FOLDER);
    final List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    dataFile1 = dataAppender.writeFile(null, batch1);
    dataAppender.appendToTable(dataFile1);
    snapshot1 = tableResource.table().currentSnapshot();
  }

  @Test
  public void testContinuousEnumerator() throws Exception {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        tableResource.table(), config, scanContext);

    ContinuousEnumerationResult result1 = splitPlanner.planSplits(null);
    Assert.assertEquals(snapshot1.snapshotId(), result1.position().endSnapshotId());
    Assert.assertEquals(1, result1.splits().size());
    final IcebergSourceSplit split1 = result1.splits().iterator().next();
    Assert.assertEquals(1, split1.task().files().size());
    Assert.assertEquals(dataFile1.path().toString(), split1.task().files().iterator().next().file().path().toString());

    IcebergEnumeratorPosition lastPosition = result1.position();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  /**
   * @return the last enumerated snapshot id
   */
  private IcebergEnumeratorPosition verifyOneCycle(
      ContinuousSplitPlannerImpl splitPlanner, IcebergEnumeratorPosition lastPosition) throws Exception {
    final List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    final DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    final Snapshot snapshot = tableResource.table().currentSnapshot();

    ContinuousEnumerationResult result = splitPlanner.planSplits(lastPosition);
    Assert.assertEquals(snapshot.snapshotId(), result.position().endSnapshotId());
    Assert.assertEquals(1, result.splits().size());
    final IcebergSourceSplit split = result.splits().iterator().next();
    Assert.assertEquals(1, split.task().files().size());
    Assert.assertEquals(dataFile.path().toString(), split.task().files().iterator().next().file().path().toString());
    return result.position();
  }
}
