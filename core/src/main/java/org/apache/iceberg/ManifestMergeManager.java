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
import java.lang.reflect.Array;
import java.security.PrivilegedExceptionAction;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

abstract class ManifestMergeManager<F extends ContentFile<F>> {
  private final long targetSizeBytes;
  private final int minCountToMerge;
  private final boolean mergeEnabled;

  // cache merge results to reuse when retrying
  private final Map<List<ManifestFile>, ManifestFile> mergedManifests = Maps.newConcurrentMap();

  ManifestMergeManager(long targetSizeBytes, int minCountToMerge, boolean mergeEnabled) {
    this.targetSizeBytes = targetSizeBytes;
    this.minCountToMerge = minCountToMerge;
    this.mergeEnabled = mergeEnabled;
  }

  protected abstract long snapshotId();
  protected abstract PartitionSpec spec(int specId);
  protected abstract void deleteFile(String location);
  protected abstract ManifestWriter<F> newManifestWriter(PartitionSpec spec);
  protected abstract ManifestReader<F> newManifestReader(ManifestFile manifest);

  Iterable<ManifestFile> mergeManifests(Iterable<ManifestFile> manifests) {
    Iterator<ManifestFile> manifestIter = manifests.iterator();
    if (!mergeEnabled || !manifestIter.hasNext()) {
      return manifests;
    }

    ManifestFile first = manifestIter.next();

    List<ManifestFile> merged = Lists.newArrayList();
    ListMultimap<Integer, ManifestFile> groups = groupBySpec(first, manifestIter);
    for (Integer specId : groups.keySet()) {
      Iterables.addAll(merged, mergeGroup(first, specId, groups.get(specId)));
    }

    return merged;
  }

  void cleanUncommitted(Set<ManifestFile> committed) {
    // iterate over a copy of entries to avoid concurrent modification
    List<Map.Entry<List<ManifestFile>, ManifestFile>> entries =
        Lists.newArrayList(mergedManifests.entrySet());

    for (Map.Entry<List<ManifestFile>, ManifestFile> entry : entries) {
      // delete any new merged manifests that aren't in the committed list
      ManifestFile merged = entry.getValue();
      if (!committed.contains(merged)) {
        deleteFile(merged.path());
        // remove the deleted file from the cache
        mergedManifests.remove(entry.getKey());
      }
    }
  }

  private ListMultimap<Integer, ManifestFile> groupBySpec(ManifestFile first, Iterator<ManifestFile> remaining) {
    ListMultimap<Integer, ManifestFile> groups = Multimaps.newListMultimap(
        Maps.newTreeMap(Comparator.<Integer>reverseOrder()),
        Lists::newArrayList);
    groups.put(first.partitionSpecId(), first);
    remaining.forEachRemaining(manifest -> groups.put(manifest.partitionSpecId(), manifest));
    return groups;
  }

  @SuppressWarnings("unchecked")
  private Iterable<ManifestFile> mergeGroup(ManifestFile first, int specId, List<ManifestFile> group) {
    // use a lookback of 1 to avoid reordering the manifests. using 1 also means this should pack
    // from the end so that the manifest that gets under-filled is the first one, which will be
    // merged the next time.
    ListPacker<ManifestFile> packer = new ListPacker<>(targetSizeBytes, 1, false);
    List<List<ManifestFile>> bins = packer.packEnd(group, ManifestFile::length);

    // process bins in parallel, but put results in the order of the bins into an array to preserve
    // the order of manifests and contents. preserving the order helps avoid random deletes when
    // data files are eventually aged off.
    List<ManifestFile>[] binResults = (List<ManifestFile>[])
        Array.newInstance(List.class, bins.size());

    final UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user from UserGroupInformation", e);
    }

    Tasks.range(bins.size())
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(index -> {
          try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() {
                List<ManifestFile> bin = bins.get(index);
                List<ManifestFile> outputManifests = Lists.newArrayList();
                binResults[index] = outputManifests;

                if (bin.size() == 1) {
                  // no need to rewrite
                  outputManifests.add(bin.get(0));
                  return null;
                }

                // if the bin has the first manifest (the new data files or an appended manifest file)
                // then only merge it if the number of manifests is above the minimum count. this is applied only to
                // bins with an in-memory manifest so that large manifests don't prevent merging older groups.
                if (bin.contains(first) && bin.size() < minCountToMerge) {
                  // not enough to merge, add all manifest files to the output list
                  outputManifests.addAll(bin);
                } else {
                  // merge the group
                  outputManifests.add(createManifest(specId, bin));
                }
                return null;
              }
            });
          } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error running mergeGroup", e);
          }
        });

    return Iterables.concat(binResults);
  }

  private ManifestFile createManifest(int specId, List<ManifestFile> bin) {
    // if this merge was already rewritten, use the existing file.
    // if the new files are in this merge, then the ManifestFile for the new files has changed and
    // will be a cache miss.
    if (mergedManifests.containsKey(bin)) {
      return mergedManifests.get(bin);
    }

    ManifestWriter<F> writer = newManifestWriter(spec(specId));
    boolean threw = true;
    try {
      for (ManifestFile manifest : bin) {
        try (ManifestReader<F> reader = newManifestReader(manifest)) {
          for (ManifestEntry<F> entry : reader.entries()) {
            if (entry.status() == Status.DELETED) {
              // suppress deletes from previous snapshots. only files deleted by this snapshot
              // should be added to the new manifest
              if (entry.snapshotId() == snapshotId()) {
                writer.delete(entry);
              }
            } else if (entry.status() == Status.ADDED && entry.snapshotId() == snapshotId()) {
              // adds from this snapshot are still adds, otherwise they should be existing
              writer.add(entry);
            } else {
              // add all files from the old manifest as existing files
              writer.existing(entry);
            }
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to close manifest reader");
        }
      }
      threw = false;

    } finally {
      Exceptions.close(writer, threw);
    }

    ManifestFile manifest = writer.toManifestFile();

    // update the cache
    mergedManifests.put(bin, manifest);

    return manifest;

  }
}
