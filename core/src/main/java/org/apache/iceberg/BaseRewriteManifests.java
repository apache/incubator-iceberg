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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT;


public class BaseRewriteManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private static final String KEPT_MANIFESTS_COUNT = "manifests-kept";
  private static final String CREATED_MANIFESTS_COUNT = "manifests-created";
  private static final String REPLACED_MANIFESTS_COUNT = "manifests-replaced";
  private static final String PROCESSED_ENTRY_COUNT = "entries-processed";

  private final TableOperations ops;
  private final Map<Integer, PartitionSpec> specsById;
  private final long manifestTargetSizeBytes;
  private final boolean snapshotIdInheritanceEnabled;

  private final Set<ManifestFile> deletedManifests = Sets.newHashSet();
  private final List<ManifestFile> addedManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAddedManifests = Lists.newArrayList();

  private final Collection<ManifestFile> keptManifests = new ConcurrentLinkedQueue<>();
  private final Collection<ManifestFile> newManifests = new ConcurrentLinkedQueue<>();
  private final Set<ManifestFile> rewrittenManifests = Sets.newConcurrentHashSet();
  private final Map<Object, WriterWrapper> writers = Maps.newConcurrentMap();

  private final AtomicLong entryCount = new AtomicLong(0);

  private Function<DataFile, Object> clusterByFunc;
  private Predicate<ManifestFile> predicate;

  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

  BaseRewriteManifests(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.specsById = ops.current().specsById();
    this.manifestTargetSizeBytes =
      ops.current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.snapshotIdInheritanceEnabled = ops.current()
        .propertyAsBoolean(SNAPSHOT_ID_INHERITANCE_ENABLED, SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
  }

  @Override
  protected RewriteManifests self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteManifests set(String property, String value) {
    summaryBuilder.set(property, value);
    return this;
  }

  @Override
  protected Map<String, String> summary() {
    int createdManifestsCount = newManifests.size() + addedManifests.size() + rewrittenAddedManifests.size();
    summaryBuilder.set(CREATED_MANIFESTS_COUNT, String.valueOf(createdManifestsCount));
    summaryBuilder.set(KEPT_MANIFESTS_COUNT, String.valueOf(keptManifests.size()));
    summaryBuilder.set(REPLACED_MANIFESTS_COUNT, String.valueOf(rewrittenManifests.size() + deletedManifests.size()));
    summaryBuilder.set(PROCESSED_ENTRY_COUNT, String.valueOf(entryCount.get()));
    summaryBuilder.setPartitionSummaryLimit(0); // do not include partition summaries because data did not change
    return summaryBuilder.build();
  }

  @Override
  public RewriteManifests clusterBy(Function<DataFile, Object> func) {
    this.clusterByFunc = func;
    return this;
  }

  @Override
  public RewriteManifests rewriteIf(Predicate<ManifestFile> pred) {
    this.predicate = pred;
    return this;
  }

  @Override
  public RewriteManifests deleteManifest(ManifestFile manifest) {
    deletedManifests.add(manifest);
    return this;
  }

  @Override
  public RewriteManifests addManifest(ManifestFile manifest) {
    Preconditions.checkArgument(!manifest.hasAddedFiles(), "Cannot add manifest with added files");
    Preconditions.checkArgument(!manifest.hasDeletedFiles(), "Cannot add manifest with deleted files");
    Preconditions.checkArgument(
        manifest.snapshotId() == null || manifest.snapshotId() == -1,
        "Snapshot id must be assigned during commit");
    Preconditions.checkArgument(manifest.sequenceNumber() == -1,
        "Sequence must be assigned during commit");

    if (snapshotIdInheritanceEnabled && manifest.snapshotId() == null) {
      addedManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAddedManifests.add(copiedManifest);
    }

    return this;
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    TableMetadata current = ops.current();
    InputFile toCopy = ops.io().newInputFile(manifest.path());
    OutputFile newFile = newManifestOutput();
    return ManifestFiles.copyRewriteManifest(
        current.formatVersion(), toCopy, specsById, newFile, snapshotId(), summaryBuilder,
            current.location(), current.properties());
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    List<ManifestFile> currentManifests = base.currentSnapshot().dataManifests();
    Set<ManifestFile> currentManifestSet = ImmutableSet.copyOf(currentManifests);

    validateDeletedManifests(currentManifestSet);

    if (requiresRewrite(currentManifestSet)) {
      performRewrite(currentManifests);
    } else {
      keepActiveManifests(currentManifests);
    }

    validateFilesCounts();

    Iterable<ManifestFile> newManifestsWithMetadata = Iterables.transform(
        Iterables.concat(newManifests, addedManifests, rewrittenAddedManifests),
        manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());

    // put new manifests at the beginning
    List<ManifestFile> apply = Lists.newArrayList();
    Iterables.addAll(apply, newManifestsWithMetadata);
    apply.addAll(keptManifests);
    apply.addAll(base.currentSnapshot().deleteManifests());

    return apply;
  }

  private boolean requiresRewrite(Set<ManifestFile> currentManifests) {
    if (clusterByFunc == null) {
      // manifests are deleted and added directly so don't perform a rewrite
      return false;
    }

    if (rewrittenManifests.size() == 0) {
      // nothing yet processed so perform a full rewrite
      return true;
    }

    // if any processed manifest is not in the current manifest list, perform a full rewrite
    return rewrittenManifests.stream().anyMatch(manifest -> !currentManifests.contains(manifest));
  }

  private void keepActiveManifests(List<ManifestFile> currentManifests) {
    // keep any existing manifests as-is that were not processed
    keptManifests.clear();
    currentManifests.stream()
        .filter(manifest -> !rewrittenManifests.contains(manifest) && !deletedManifests.contains(manifest))
        .forEach(keptManifests::add);
  }

  private void reset() {
    cleanUncommitted(newManifests, ImmutableSet.of());
    entryCount.set(0);
    keptManifests.clear();
    rewrittenManifests.clear();
    newManifests.clear();
    writers.clear();
  }

  private void performRewrite(List<ManifestFile> currentManifests) {
    reset();

    List<ManifestFile> remainingManifests = currentManifests.stream()
        .filter(manifest -> !deletedManifests.contains(manifest))
        .collect(Collectors.toList());

    try {
      Tasks.foreach(remainingManifests)
          .executeWith(ThreadPools.getWorkerPool())
          .run(manifest -> {
            if (predicate != null && !predicate.test(manifest)) {
              keptManifests.add(manifest);
            } else {
              rewrittenManifests.add(manifest);
              try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, ops.io(), ops.current().specsById(),
                  ops.current().location(), ops.current().properties())
                  .select(Arrays.asList("*"))) {
                reader.liveEntries().forEach(
                    entry -> appendEntry(entry, clusterByFunc.apply(entry.file()), manifest.partitionSpecId())
                );

              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            }
          });
    } finally {
      Tasks.foreach(writers.values()).executeWith(ThreadPools.getWorkerPool()).run(WriterWrapper::close);
    }
  }

  private void validateDeletedManifests(Set<ManifestFile> currentManifests) {
    // directly deleted manifests must be still present in the current snapshot
    deletedManifests.stream()
        .filter(manifest -> !currentManifests.contains(manifest))
        .findAny()
        .ifPresent(manifest -> {
          throw new ValidationException("Manifest is missing: %s", manifest.path());
        });
  }

  private void validateFilesCounts() {
    Iterable<ManifestFile> createdManifests = Iterables.concat(newManifests, addedManifests, rewrittenAddedManifests);
    int createdManifestsFilesCount = activeFilesCount(createdManifests);

    Iterable<ManifestFile> replacedManifests = Iterables.concat(rewrittenManifests, deletedManifests);
    int replacedManifestsFilesCount = activeFilesCount(replacedManifests);

    if (createdManifestsFilesCount != replacedManifestsFilesCount) {
      throw new ValidationException(
          "Replaced and created manifests must have the same number of active files: %d (new), %d (old)",
          createdManifestsFilesCount, replacedManifestsFilesCount);
    }
  }

  private int activeFilesCount(Iterable<ManifestFile> manifests) {
    int activeFilesCount = 0;

    for (ManifestFile manifest : manifests) {
      Preconditions.checkNotNull(manifest.addedFilesCount(), "Missing file counts in %s", manifest.path());
      Preconditions.checkNotNull(manifest.existingFilesCount(), "Missing file counts in %s", manifest.path());
      activeFilesCount += manifest.addedFilesCount();
      activeFilesCount += manifest.existingFilesCount();
    }

    return activeFilesCount;
  }

  private void appendEntry(ManifestEntry<DataFile> entry, Object key, int partitionSpecId) {
    Preconditions.checkNotNull(entry, "Manifest entry cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    WriterWrapper writer = getWriter(key, partitionSpecId);
    writer.addEntry(entry);
    entryCount.incrementAndGet();
  }

  private WriterWrapper getWriter(Object key, int partitionSpecId) {
    return writers.computeIfAbsent(
        Pair.of(key, partitionSpecId), k -> new WriterWrapper(specsById.get(partitionSpecId)));
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    cleanUncommitted(newManifests, committed);
    // clean up only rewrittenAddedManifests as they are always owned by the table
    // don't clean up addedManifests as they are added to the manifest list and are not compacted
    cleanUncommitted(rewrittenAddedManifests, committed);
  }

  private void cleanUncommitted(Iterable<ManifestFile> manifests, Set<ManifestFile> committedManifests) {
    for (ManifestFile manifest : manifests) {
      if (!committedManifests.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }

  long getManifestTargetSizeBytes() {
    return manifestTargetSizeBytes;
  }

  class WriterWrapper {
    private final PartitionSpec spec;
    private ManifestWriter<DataFile> writer;

    WriterWrapper(PartitionSpec spec) {
      this.spec = spec;
    }

    synchronized void addEntry(ManifestEntry<DataFile> entry) {
      if (writer == null) {
        writer = newManifestWriter(spec);
      } else if (writer.length() >= getManifestTargetSizeBytes()) {
        close();
        writer = newManifestWriter(spec);
      }
      writer.existing(entry);
    }

    synchronized void close() {
      if (writer != null) {
        try {
          writer.close();
          newManifests.add(writer.toManifestFile());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      }
    }

  }

}
