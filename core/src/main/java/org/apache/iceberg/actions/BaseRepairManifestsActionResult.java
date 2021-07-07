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

package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseRepairManifestsActionResult implements RepairManifests.Result {

  private final List<ManifestFile> deletedManifests;
  private final List<ManifestFile> addedManifests;

  public BaseRepairManifestsActionResult(List<ManifestFile> deletedManifests, List<ManifestFile> addedManifests) {
    Preconditions.checkNotNull(deletedManifests, "Deleted manifests must not be null");
    Preconditions.checkNotNull(addedManifests, "Added manifests must not be null");
    this.deletedManifests = deletedManifests;
    this.addedManifests = addedManifests;
  }

  public static RepairManifests.Result empty() {
    return new BaseRepairManifestsActionResult(ImmutableList.of(), ImmutableList.of());
  }

  @Override
  public List<ManifestFile> deletedManifests() {
    return deletedManifests;
  }

  @Override
  public List<ManifestFile> addedManifests() {
    return addedManifests;
  }
}
