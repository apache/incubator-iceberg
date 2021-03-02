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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

public class TestFlinkManifest {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private Table table;
  private FileAppenderFactory<RowData> appenderFactory;
  private final AtomicInteger fileCount = new AtomicInteger(0);

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    String warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table directory correctly.", new File(tablePath).mkdir());

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), false);

    int[] equalityFieldIds = new int[] {
        table.schema().findField("id").fieldId(),
        table.schema().findField("data").fieldId()
    };
    this.appenderFactory = new FlinkAppenderFactory(table.schema(), FlinkSchemaUtil.convert(table.schema()),
        table.properties(), table.spec(), table.sortOrder(), equalityFieldIds, table.schema(), null);
  }


  @Test
  public void testIO() throws IOException {
    String flinkJobId = newFlinkJobId();
    for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
      ManifestOutputFileFactory factory =
          FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, 1, 1);
      final long curCkpId = checkpointId;

      List<DataFile> dataFiles = generateDataFiles(10);
      List<DeleteFile> eqDeleteFiles = generateEqDeleteFiles(5);
      List<DeleteFile> posDeleteFiles = generatePosDeleteFiles(5);
      DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(
          WriteResult.builder()
              .addDataFiles(dataFiles)
              .addDeleteFiles(eqDeleteFiles)
              .addDeleteFiles(posDeleteFiles)
              .build(),
          () -> factory.create(curCkpId), table.spec());

      WriteResult result = FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io());
      Assert.assertEquals("Size of data file list are not equal.", 10, result.deleteFiles().length);
      for (int i = 0; i < dataFiles.size(); i++) {
        TestHelpers.assertEquals(dataFiles.get(i), result.dataFiles()[i]);
      }
      Assert.assertEquals("Size of delete file list are not equal.", 10, result.dataFiles().length);
      for (int i = 0; i < 5; i++) {
        TestHelpers.assertEquals(eqDeleteFiles.get(i), result.deleteFiles()[i]);
      }
      for (int i = 0; i < 5; i++) {
        TestHelpers.assertEquals(posDeleteFiles.get(i), result.deleteFiles()[5 + i]);
      }
    }
  }

  @Test
  public void testUserProvidedManifestLocation() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    File userProvidedFolder = tempFolder.newFolder();
    Map<String, String> props = ImmutableMap.of(FLINK_MANIFEST_LOCATION, userProvidedFolder.getAbsolutePath() + "///");
    ManifestOutputFileFactory factory = new ManifestOutputFileFactory(
        ((HasTableOperations) table).operations(), table.io(), props,
        flinkJobId, 1, 1);

    List<DataFile> dataFiles = generateDataFiles(5);
    DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(
        WriteResult.builder()
            .addDataFiles(dataFiles)
            .build(),
        () -> factory.create(checkpointId),
        table.spec());

    Assert.assertNotNull("Data manifest shouldn't be null", deltaManifests.dataManifest());
    Assert.assertNull("Delete manifest should be null", deltaManifests.deleteManifest());
    Assert.assertEquals("The newly created manifest file should be located under the user provided directory",
        userProvidedFolder.toPath(), Paths.get(deltaManifests.dataManifest().path()).getParent());

    WriteResult result = FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io());

    Assert.assertEquals(0, result.deleteFiles().length);
    Assert.assertEquals(5, result.dataFiles().length);

    Assert.assertEquals("Size of data file list are not equal.", dataFiles.size(), result.dataFiles().length);
    for (int i = 0; i < dataFiles.size(); i++) {
      TestHelpers.assertEquals(dataFiles.get(i), result.dataFiles()[i]);
    }
  }

  @Test
  public void testVersionedSerializer() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    ManifestOutputFileFactory factory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, 1, 1);

    List<DataFile> dataFiles = generateDataFiles(10);
    List<DeleteFile> eqDeleteFiles = generateEqDeleteFiles(10);
    List<DeleteFile> posDeleteFiles = generatePosDeleteFiles(10);
    DeltaManifests expected = FlinkManifestUtil.writeCompletedFiles(
        WriteResult.builder()
            .addDataFiles(dataFiles)
            .addDeleteFiles(eqDeleteFiles)
            .addDeleteFiles(posDeleteFiles)
            .build(),
        () -> factory.create(checkpointId), table.spec());

    byte[] versionedSerializeData =
        SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, expected);
    DeltaManifests actual = SimpleVersionedSerialization
        .readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, versionedSerializeData);
    TestHelpers.assertEquals(expected.dataManifest(), actual.dataManifest());
    TestHelpers.assertEquals(expected.deleteManifest(), actual.deleteManifest());

    byte[] versionedSerializeData2 =
        SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, actual);
    Assert.assertArrayEquals(versionedSerializeData, versionedSerializeData2);
  }

  @Test
  public void testCompatibility() throws IOException {
    // The v2 deserializer should be able to deserialize the v1 binary.
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    ManifestOutputFileFactory factory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, 1, 1);

    List<DataFile> dataFiles = generateDataFiles(10);
    ManifestFile manifest = FlinkManifestUtil.writeDataFiles(factory.create(checkpointId), table.spec(), dataFiles);
    byte[] dataV1 = SimpleVersionedSerialization.writeVersionAndSerialize(new V1Serializer(), manifest);

    DeltaManifests delta =
        SimpleVersionedSerialization.readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, dataV1);
    Assert.assertNull("Serialization v1 don't include delete files.", delta.deleteManifest());
    Assert.assertNotNull("Serialization v1 should not have null data manifest.", delta.dataManifest());
    TestHelpers.assertEquals(manifest, delta.dataManifest());

    List<DataFile> actualFiles = FlinkManifestUtil.readDataFiles(delta.dataManifest(), table.io());
    Assert.assertEquals(10, actualFiles.size());
    for (int i = 0; i < 10; i++) {
      TestHelpers.assertEquals(dataFiles.get(i), actualFiles.get(i));
    }
  }

  private static class V1Serializer implements SimpleVersionedSerializer<ManifestFile> {

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(ManifestFile m) throws IOException {
      return ManifestFiles.encode(m);
    }

    @Override
    public ManifestFile deserialize(int version, byte[] serialized) throws IOException {
      return ManifestFiles.decode(serialized);
    }
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), table.sortOrder(), CONF,
        tablePath, FileFormat.PARQUET.addExtension(filename), rows);
  }

  private DeleteFile writeEqDeleteFile(String filename, List<RowData> deletes) throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, FileFormat.PARQUET, tablePath, filename, appenderFactory, deletes);
  }

  private DeleteFile writePosDeleteFile(String filename, List<Pair<CharSequence, Long>> positions)
      throws IOException {
    return SimpleDataUtil
        .writePosDeleteFile(table, FileFormat.PARQUET, tablePath, filename, appenderFactory, positions);
  }

  private List<DataFile> generateDataFiles(int fileNum) throws IOException {
    List<RowData> rowDataList = Lists.newArrayList();
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      rowDataList.add(SimpleDataUtil.createRowData(i, "a" + i));
      dataFiles.add(writeDataFile("data-file-" + fileCount.incrementAndGet(), rowDataList));
    }
    return dataFiles;
  }

  private List<DeleteFile> generateEqDeleteFiles(int fileNum) throws IOException {
    List<RowData> rowDataList = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      rowDataList.add(SimpleDataUtil.createDelete(i, "a" + i));
      deleteFiles.add(writeEqDeleteFile("eq-delete-file-" + fileCount.incrementAndGet(), rowDataList));
    }
    return deleteFiles;
  }

  private List<DeleteFile> generatePosDeleteFiles(int fileNum) throws IOException {
    List<Pair<CharSequence, Long>> positions = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      positions.add(Pair.of("data-file-1", (long) i));
      deleteFiles.add(writePosDeleteFile("pos-delete-file-" + fileCount.incrementAndGet(), positions));
    }
    return deleteFiles;
  }

  private static String newFlinkJobId() {
    return UUID.randomUUID().toString();
  }
}
