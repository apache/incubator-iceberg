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

package org.apache.iceberg.deletes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FilterIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Filter;
import org.apache.iceberg.util.SortedMerge;
import org.apache.iceberg.util.StructLikeSet;

public class Deletes {
  private static final Schema POSITION_DELETE_SCHEMA = new Schema(
      MetadataColumns.DELETE_FILE_PATH,
      MetadataColumns.DELETE_FILE_POS
  );

  private static final Accessor<StructLike> FILENAME_ACCESSOR = POSITION_DELETE_SCHEMA
      .accessorForField(MetadataColumns.DELETE_FILE_PATH.fieldId());
  private static final Accessor<StructLike> POSITION_ACCESSOR = POSITION_DELETE_SCHEMA
      .accessorForField(MetadataColumns.DELETE_FILE_POS.fieldId());

  private Deletes() {
  }

  public static <T> CloseableIterable<T> filter(CloseableIterable<T> rows, Function<T, StructLike> rowToDeleteKey,
                                                StructLikeSet deleteSet) {
    if (deleteSet.isEmpty()) {
      return rows;
    }

    EqualitySetDeleteFilter<T> equalityFilter = new EqualitySetDeleteFilter<>(rowToDeleteKey, deleteSet);
    return equalityFilter.filter(rows);
  }

  public static <T> CloseableIterable<T> filter(CloseableIterable<T> rows, Function<T, Long> rowToPosition,
                                                Set<Long> deleteSet) {
    if (deleteSet.isEmpty()) {
      return rows;
    }

    PositionSetDeleteFilter<T> filter = new PositionSetDeleteFilter<>(rowToPosition, deleteSet);
    return filter.filter(rows);
  }

  public static StructLikeSet toEqualitySet(CloseableIterable<StructLike> eqDeletes, Types.StructType eqType) {
    try (CloseableIterable<StructLike> deletes = eqDeletes) {
      StructLikeSet deleteSet = StructLikeSet.create(eqType);
      Iterables.addAll(deleteSet, deletes);
      return deleteSet;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delete source", e);
    }
  }

  public static Set<Long> toPositionSet(CharSequence dataLocation, CloseableIterable<? extends StructLike> deleteFile) {
    return toPositionSet(dataLocation, ImmutableList.of(deleteFile));
  }

  public static <T extends StructLike> Set<Long> toPositionSet(CharSequence dataLocation,
                                                               List<CloseableIterable<T>> deleteFiles) {
    DataFileFilter<T> locationFilter = new DataFileFilter<>(dataLocation);
    List<CloseableIterable<Long>> positions = Lists.transform(deleteFiles, deletes ->
        CloseableIterable.transform(locationFilter.filter(deletes), row -> (Long) POSITION_ACCESSOR.get(row)));
    return toPositionSet(CloseableIterable.concat(positions));
  }

  public static Set<Long> toPositionSet(CloseableIterable<Long> posDeletes) {
    try (CloseableIterable<Long> deletes = posDeletes) {
      return Sets.newHashSet(deletes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close position delete source", e);
    }
  }

  public static <T> CloseableIterable<T> streamingFilter(CloseableIterable<T> rows,
                                                         Function<T, Long> rowToPosition,
                                                         CloseableIterable<Long> posDeletes) {
    return new PositionStreamDeleteFilter<>(rows, rowToPosition, posDeletes);
  }

  public static <T> CloseableIterable<T> streamingDeletedRowMarker(CloseableIterable<T> rows,
                                                                   Function<T, Long> rowToPosition,
                                                                   CloseableIterable<Long> posDeletes,
                                                                   Consumer<T> deleteMarker) {
    return new PositionStreamDeletedRowMarker<>(rows, rowToPosition, posDeletes, deleteMarker);
  }

  public static CloseableIterable<Long> deletePositions(CharSequence dataLocation,
                                                        CloseableIterable<StructLike> deleteFile) {
    return deletePositions(dataLocation, ImmutableList.of(deleteFile));
  }

  public static <T extends StructLike> CloseableIterable<Long> deletePositions(CharSequence dataLocation,
                                                                               List<CloseableIterable<T>> deleteFiles) {
    DataFileFilter<T> locationFilter = new DataFileFilter<>(dataLocation);
    List<CloseableIterable<Long>> positions = Lists.transform(deleteFiles, deletes ->
        CloseableIterable.transform(locationFilter.filter(deletes), row -> (Long) POSITION_ACCESSOR.get(row)));

    return new SortedMerge<>(Long::compare, positions);
  }

  private static class EqualitySetDeleteFilter<T> extends Filter<T> {
    private final StructLikeSet deletes;
    private final Function<T, StructLike> extractEqStruct;

    protected EqualitySetDeleteFilter(Function<T, StructLike> extractEq,
                                      StructLikeSet deletes) {
      this.extractEqStruct = extractEq;
      this.deletes = deletes;
    }

    @Override
    protected boolean shouldKeep(T row) {
      return !deletes.contains(extractEqStruct.apply(row));
    }
  }

  private static class PositionSetDeleteFilter<T> extends Filter<T> {
    private final Function<T, Long> rowToPosition;
    private final Set<Long> deleteSet;

    private PositionSetDeleteFilter(Function<T, Long> rowToPosition, Set<Long> deleteSet) {
      this.rowToPosition = rowToPosition;
      this.deleteSet = deleteSet;
    }

    @Override
    protected boolean shouldKeep(T row) {
      return !deleteSet.contains(rowToPosition.apply(row));
    }
  }

  private static class PositionStreamDeleteFilter<T> extends CloseableGroup implements CloseableIterable<T> {
    private final CloseableIterable<T> rows;
    private final Function<T, Long> extractPos;
    private final CloseableIterable<Long> deletePositions;

    private PositionStreamDeleteFilter(CloseableIterable<T> rows, Function<T, Long> extractPos,
                                       CloseableIterable<Long> deletePositions) {
      this.rows = rows;
      this.extractPos = extractPos;
      this.deletePositions = deletePositions;
    }

    @Override
    public CloseableIterator<T> iterator() {
      CloseableIterator<Long> deletePosIterator = deletePositions.iterator();

      CloseableIterator<T> iter;
      if (deletePosIterator.hasNext()) {
        iter = positionIterator(rows.iterator(), deletePosIterator);
      } else {
        iter = rows.iterator();
        try {
          deletePosIterator.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close delete positions iterator", e);
        }
      }

      addCloseable(iter);

      return iter;
    }

    protected FilterIterator<T> positionIterator(CloseableIterator<T> items,
                                                 CloseableIterator<Long> newDeletePositions) {
      return new PositionFilterIterator(items, newDeletePositions);
    }

    protected class PositionFilterIterator extends FilterIterator<T> {
      private final CloseableIterator<Long> deletePosIterator;
      private long nextDeletePos;

      protected PositionFilterIterator(CloseableIterator<T> items, CloseableIterator<Long> deletePositions) {
        super(items);
        this.deletePosIterator = deletePositions;
        this.nextDeletePos = deletePosIterator.next();
      }

      @Override
      protected boolean shouldKeep(T row) {
        long currentPos = extractPos.apply(row);
        if (currentPos < nextDeletePos) {
          return true;
        }

        // consume delete positions until the next is past the current position
        boolean keep = currentPos != nextDeletePos;
        while (deletePosIterator.hasNext() && nextDeletePos <= currentPos) {
          this.nextDeletePos = deletePosIterator.next();
          if (keep && currentPos == nextDeletePos) {
            // if any delete position matches the current position, discard
            keep = false;
          }
        }

        return keep;
      }

      @Override
      public void close() {
        super.close();
        try {
          deletePosIterator.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close delete positions iterator", e);
        }
      }
    }
  }

  static class PositionStreamDeletedRowMarker<T> extends PositionStreamDeleteFilter<T> {
    private final Consumer<T> deleteMarker;

    private PositionStreamDeletedRowMarker(CloseableIterable<T> rows, Function<T, Long> extractPos,
                                           CloseableIterable<Long> deletePositions,
                                           Consumer<T> deleteMarker) {
      super(rows, extractPos, deletePositions);
      this.deleteMarker = deleteMarker;
    }

    @Override
    protected FilterIterator<T> positionIterator(CloseableIterator<T> items,
                                                 CloseableIterator<Long> deletePositions) {
      return new PositionMarkerIterator(items, deletePositions);
    }

    private class PositionMarkerIterator extends PositionFilterIterator {
      private PositionMarkerIterator(CloseableIterator<T> items, CloseableIterator<Long> deletePositions) {
        super(items, deletePositions);
      }

      @Override
      protected boolean shouldKeep(T row) {
        if (!super.shouldKeep(row)) {
          deleteMarker.accept(row);
        }
        return true;
      }
    }
  }

  private static class DataFileFilter<T extends StructLike> extends Filter<T> {
    private static final Comparator<CharSequence> CHARSEQ_COMPARATOR = Comparators.charSequences();
    private final CharSequence dataLocation;

    DataFileFilter(CharSequence dataLocation) {
      this.dataLocation = dataLocation;
    }

    @Override
    protected boolean shouldKeep(T posDelete) {
      return CHARSEQ_COMPARATOR.compare(dataLocation, (CharSequence) FILENAME_ACCESSOR.get(posDelete)) == 0;
    }
  }
}
