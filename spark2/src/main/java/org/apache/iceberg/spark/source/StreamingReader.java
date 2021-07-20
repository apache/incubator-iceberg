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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

/**
 * A micro-batch based Spark Structured Streaming reader for Iceberg table. It will track the added
 * files and generate tasks per batch to process newly added files. By default it will process
 * all the newly added files to the current snapshot in each batch, user could also set this
 * configuration "max-files-per-trigger" to control the number of files processed per batch.
 */
class StreamingReader extends Reader implements MicroBatchReader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingReader.class);
  private static final String MAX_SIZE_PER_BATCH = "max-size-per-batch";
  private static final String START_SNAPSHOT_ID = "start-snapshot-id";
  private static final Joiner SLASH = Joiner.on("/");

  private StreamingOffset startOffset;
  private StreamingOffset endOffset;

  private final Table table;
  private final long maxSizePerBatch;
  private final Long startSnapshotId;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private Boolean readUsingBatch = null;

  /**
   * Used to cache the pending batches for this streaming batch interval.
   */
  private Pair<StreamingOffset, List<MicroBatch>> cachedPendingBatches = null;

  StreamingReader(SparkSession spark, Table table, String checkpointLocation, boolean caseSensitive,
      DataSourceOptions options) {
    super(spark, table, caseSensitive, options);

    this.table = table;
    this.maxSizePerBatch = options.get(MAX_SIZE_PER_BATCH).map(Long::parseLong).orElse(Long.MAX_VALUE);
    Preconditions.checkArgument(maxSizePerBatch > 0L,
        "Option max-size-per-batch '%s' should > 0", maxSizePerBatch);

    this.startSnapshotId = options.get(START_SNAPSHOT_ID).map(Long::parseLong).orElse(null);
    if (startSnapshotId != null) {
      if (!SnapshotUtil.ancestorOf(table, table.currentSnapshot().snapshotId(), startSnapshotId)) {
        throw new IllegalArgumentException("The option start-snapshot-id " + startSnapshotId +
            " is not an ancestor of the current snapshot");
      }
    }

    long tableSplitSize = Optional.ofNullable(splitSize())
        .orElseGet(
            () -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_SIZE, SPLIT_SIZE_DEFAULT));
    int tableSplitLookback = Optional.ofNullable(splitLookback())
        .orElseGet(() -> PropertyUtil
            .propertyAsInt(table.properties(), SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT));
    long tableSplitOpenFileCost = Optional.ofNullable(splitOpenFileCost())
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_OPEN_FILE_COST,
            SPLIT_OPEN_FILE_COST_DEFAULT));

    this.splitSize = options.getLong(SparkReadOptions.SPLIT_SIZE, tableSplitSize);
    this.splitLookback = options.getInt(SparkReadOptions.LOOKBACK, tableSplitLookback);
    this.splitOpenFileCost = options
        .getLong(SparkReadOptions.FILE_OPEN_COST, tableSplitOpenFileCost);

    InitialOffsetStore initialOffsetStore = new InitialOffsetStore(table, checkpointLocation);
    this.startOffset = initialOffsetStore.initialOffset();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
    table.refresh();

    if (start.isPresent() && !StreamingOffset.START_OFFSET.equals(start.get())) {
      this.startOffset = (StreamingOffset) start.get();
      this.endOffset = (StreamingOffset) end.orElseGet(() -> calculateEndOffset(startOffset));
    } else {
      // If starting offset is "START_OFFSET" (there's no snapshot in the last batch), or starting
      // offset is not set, then we need to calculate the starting offset again.
      this.startOffset = calculateStartingOffset();
      this.endOffset = calculateEndOffset(startOffset);
    }
  }

  @Override
  public Offset getStartOffset() {
    if (startOffset == null) {
      throw new IllegalStateException("Start offset is not set");
    }

    return startOffset;
  }

  @Override
  public Offset getEndOffset() {
    if (endOffset == null) {
      throw new IllegalStateException("End offset is not set");
    }

    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {
    // Since all the data and metadata of Iceberg is as it is, nothing needs to commit when
    // offset is processed, so no need to implement this method.
  }

  @Override
  public void stop() {
  }

  @Override
  public boolean enableBatchRead() {
    return readUsingBatch != null && readUsingBatch;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergStreamScan(table=%s, type=%s)", table, table.schema().asStruct());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<CombinedScanTask> tasks() {
    if (startOffset.equals(endOffset)) {
      LOG.info("Start offset {} equals to end offset {}, no data to process", startOffset, endOffset);
      return Collections.emptyList();
    }

    Preconditions.checkState(cachedPendingBatches != null,
        "pendingBatches is null, which is unexpected as it will be set when calculating end offset");

    List<MicroBatch> pendingBatches = cachedPendingBatches.second();
    if (pendingBatches.isEmpty()) {
      LOG.info("Current start offset {} and end offset {}, there's no task to process in this batch",
          startOffset, endOffset);
      return Collections.emptyList();
    }

    MicroBatch lastBatch = pendingBatches.get(pendingBatches.size() - 1);
    Preconditions.checkState(
        lastBatch.snapshotId() == endOffset.snapshotId() && lastBatch.endFileIndex() == endOffset.position(),
        "The cached pendingBatches doesn't match the current end offset " + endOffset);

    LOG.info("Processing data from {} to {}", startOffset, endOffset);
    List<FileScanTask> tasks = pendingBatches.stream()
        .flatMap(batch -> batch.tasks().stream())
        .collect(Collectors.toList());
    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks),
        splitSize);
    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));

    if (readUsingBatch == null) {
      this.readUsingBatch = checkEnableBatchRead(combinedScanTasks);
    }

    return combinedScanTasks;
  }

  /**
   * Used to calculate start offset. If the startSnapshotId has a value, start the construction
   * from the specified snapshot, otherwise, start the construction from the beginning.
   *
   * @return The start offset to scan from.
   */
  private StreamingOffset calculateStartingOffset() {
    StreamingOffset startingOffset;
    if (startSnapshotId != null) {
      startingOffset = new StreamingOffset(startSnapshotId, 0, false);
    } else {
      List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
      if (snapshotIds.isEmpty()) {
        // there's no snapshot currently.
        startingOffset = StreamingOffset.START_OFFSET;
      } else {
        startingOffset = new StreamingOffset(Iterables.getLast(snapshotIds), 0, true);
      }
    }

    return startingOffset;
  }

  /**
   * Used to calculate end offset.
   *
   * @param start The start offset to scan from
   * @return The end offset to scan to
   */
  private StreamingOffset calculateEndOffset(StreamingOffset start) {
    if (start.equals(StreamingOffset.START_OFFSET)) {
      return StreamingOffset.START_OFFSET;
    }

    // Spark will invoke setOffsetRange more than once. If this is already calculated, use the cached one to avoid
    // calculating again.
    if (cachedPendingBatches == null || CollectionUtils.isEmpty(cachedPendingBatches.second()) ||
        !cachedPendingBatches.first().equals(start)) {
      this.cachedPendingBatches = Pair.of(start, getChangesWithRateLimit(start, maxSizePerBatch));
    }

    List<MicroBatch> batches = cachedPendingBatches.second();
    MicroBatch lastBatch = Iterables.getLast(batches, null);

    if (lastBatch == null) {
      return start;
    } else {
      boolean isStarting = lastBatch.snapshotId() == start.snapshotId() && start.shouldScanAllFiles();
      return new StreamingOffset(lastBatch.snapshotId(), lastBatch.endFileIndex(), isStarting);
    }
  }

  /**
   * Streaming Read control is performed by changing the offset and maxSize.
   *
   * @param offset The start offset to scan from
   * @param maxSize     The maximum size of Bytes can calculate how many batches
   * @return MicroBatch of list
   */
  @VisibleForTesting
  List<MicroBatch> getChangesWithRateLimit(StreamingOffset offset, long maxSize) {
    List<MicroBatch> batches = Lists.newArrayList();
    long currentLeftSize = maxSize;
    MicroBatch lastBatch = null;

    assertNoOverwrite(table.snapshot(offset.snapshotId()));
    if (shouldGenerateFromStartOffset(offset)) {
      MicroBatch batch = generateMicroBatch(offset.snapshotId(), offset.position(),
          offset.shouldScanAllFiles(), currentLeftSize);
      if (!batch.tasks().isEmpty()) {
        batches.add(batch);
        currentLeftSize -= batch.sizeInBytes();
        lastBatch = Iterables.getLast(batches);
      }
    }

    // Current snapshot can already satisfy the size needs.
    if (currentLeftSize <= 0L || (lastBatch != null && !lastBatch.lastIndexOfSnapshot())) {
      return batches;
    }

    long currentSnapshotId = table.currentSnapshot().snapshotId();
    if (currentSnapshotId == offset.snapshotId()) {
      // the snapshot of current offset is already the latest snapshot of this table.
      return batches;
    }

    ImmutableList<Long> snapshotIds = ImmutableList.<Long>builder()
        .addAll(SnapshotUtil.snapshotIdsBetween(table, offset.snapshotId(), currentSnapshotId))
        .build()
        .reverse();

    for (Long id : snapshotIds) {
      Snapshot snapshot = table.snapshot(id);
      assertNoOverwrite(snapshot);
      if (!isAppend(snapshot)) {
        continue;
      }

      MicroBatch batch = generateMicroBatch(id, 0, false, currentLeftSize);
      if (!batch.tasks().isEmpty()) {
        batches.add(batch);
        currentLeftSize -= batch.sizeInBytes();
        lastBatch = Iterables.getLast(batches);
      }

      // If the current request size is already satisfied, or none of the DataFile size can satisfy the current left
      // size, break the current loop.
      if (currentLeftSize <= 0L || !Objects.requireNonNull(lastBatch).lastIndexOfSnapshot()) {
        break;
      }
    }

    return batches;
  }

  private MicroBatch generateMicroBatch(long snapshotId, long startIndex, boolean isStart, long currentLeftSize) {
    return MicroBatches.from(table.snapshot(snapshotId), table.io())
        .caseSensitive(caseSensitive())
        .specsById(table.specs())
        .generate(startIndex, currentLeftSize, isStart);
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private boolean shouldGenerateFromStartOffset(StreamingOffset startOffset) {
    boolean isSnapshotFullyProcessed;
    if (cachedPendingBatches != null && !cachedPendingBatches.second().isEmpty()) {
      List<MicroBatch> batches = cachedPendingBatches.second();
      MicroBatch lastBatch = Iterables.getLast(batches, null);
      isSnapshotFullyProcessed = lastBatch.lastIndexOfSnapshot();
    } else {
      isSnapshotFullyProcessed = false;
    }
    return !isSnapshotFullyProcessed &&
        (startOffset.shouldScanAllFiles() || isAppend(table.snapshot(startOffset.snapshotId())));
  }

  private static void assertNoOverwrite(Snapshot snapshot) {
    if (!snapshot.operation().equals(DataOperations.APPEND)) {
      throw new UnsupportedOperationException(String.format("Found %s operation, cannot support incremental data for " +
          "snapshot %d", snapshot.operation(), snapshot.snapshotId()));
    }
  }

  private static boolean isAppend(Snapshot snapshot) {
    return snapshot.operation().equals(DataOperations.APPEND);
  }

  private static class InitialOffsetStore {
    private final Table table;
    private final FileIO io;
    private final String initialOffsetLocation;

    InitialOffsetStore(Table table, String checkpointLocation) {
      this.table = table;
      this.io = table.io();
      this.initialOffsetLocation = SLASH.join(checkpointLocation, "offsets/0");
    }

    public StreamingOffset initialOffset() {
      InputFile inputFile = io.newInputFile(initialOffsetLocation);
      if (inputFile.exists()) {
        return readOffset(inputFile);
      }

      table.refresh();
      StreamingOffset offset = table.currentSnapshot() == null ?
          StreamingOffset.START_OFFSET :
          new StreamingOffset(SnapshotUtil.oldestSnapshot(table).snapshotId(), 0, false);

      OutputFile outputFile = io.newOutputFile(initialOffsetLocation);
      writeOffset(offset, outputFile);

      return offset;
    }

    private void writeOffset(StreamingOffset offset, OutputFile file) {
      try (OutputStream outputStream = file.create()) {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        writer.write(offset.json());
        writer.flush();
      } catch (IOException ioException) {
        throw new UncheckedIOException(
            String.format("Failed writing offset to: %s", initialOffsetLocation), ioException);
      }
    }

    private StreamingOffset readOffset(InputFile file) {
      try (InputStream in = file.newStream()) {
        return StreamingOffset.fromJson(in);
      } catch (IOException ioException) {
        throw new UncheckedIOException(
            String.format("Failed reading offset from: %s", initialOffsetLocation), ioException);
      }
    }
  }
}
