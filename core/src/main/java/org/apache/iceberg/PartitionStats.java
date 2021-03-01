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

package org.apache.iceberg;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Utility class for partition stats file
 */
public class PartitionStats {

  private PartitionStats() {
  }

  static List<PartitionStatsEntry> read(InputFile partitionStats, int specID, Map<Integer, PartitionSpec> specsById) {
    Schema fileSchema;
    try (CloseableIterable<PartitionStatsEntry> files = Avro.read(partitionStats)
        .rename("partitionStats_file", GenericPartitionStatsEntry.class.getName())
        .rename("partition", PartitionData.class.getName())
        .rename("r600", PartitionData.class.getName())
        .classLoader(GenericPartitionStatsEntry.class.getClassLoader())
        .project(PartitionStatsEntry.getSchema(specsById.get(specID).partitionType()))
        .reuseContainers(false)
        .build()) {
      return Lists.newLinkedList(files);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Cannot read manifest list file: %s", partitionStats.location());
    }
  }

  static PartitionStatsWriter write(PartitionSpec spec, OutputFile outputFile) {
    return new PartitionStatsWriter.V2Writer(spec, outputFile);
  }
}
