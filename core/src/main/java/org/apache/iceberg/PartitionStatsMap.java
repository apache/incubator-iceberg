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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * ParitionStatsMap: Instance of this class holds updated delta information, this can be used for both delete and
 * inserts.
 */
public class PartitionStatsMap {
  private final Map<Integer, PartitionSpec> specsById;

  private Map<Integer, PartitionsTable.PartitionMap> partitionSpecMap = Maps.newHashMap();
  private Set<Integer> partitionSpecUpdated = new HashSet<>();

  public PartitionStatsMap(Map<Integer, PartitionSpec> specsById) {
    this.specsById = specsById;
  }

  synchronized void put(DataFile dataFile) {
    int partitionSpecId = dataFile.specId();
    PartitionData pd = (PartitionData) dataFile.partition();
    PartitionsTable.PartitionMap existingPartitionMap = partitionSpecMap.get(partitionSpecId);
    if (Objects.isNull(existingPartitionMap)) {
      partitionSpecUpdated.add(partitionSpecId);
      PartitionsTable.PartitionMap newPartitionMap = new PartitionsTable.PartitionMap(
          specsById.get(partitionSpecId).partitionType()
      );
      partitionSpecMap.put(partitionSpecId, newPartitionMap);
      newPartitionMap.get(pd.copy()).update(dataFile);
    } else {
      existingPartitionMap.get(pd.copy()).update(dataFile);
    }
  }

  Set<Integer> getAllUpdatedPartitionSpec() {
    return partitionSpecUpdated;
  }

  Iterable<PartitionsTable.Partition> getPartitionDataStats(Integer partitionSpecId) {
    return partitionSpecMap.get(partitionSpecId).all();
  }

  PartitionsTable.PartitionMap getPartitionMap(Integer partitionSpecId) {
    return partitionSpecMap.get(partitionSpecId);
  }

  PartitionsTable.PartitionMap getEmptyPartitionMap(Integer partitionSpecId) {
    return new PartitionsTable.PartitionMap(specsById.get(partitionSpecId).partitionType());
  }
}
