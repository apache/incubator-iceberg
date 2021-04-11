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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DeleteMarker;
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

  public static <T> CloseableIterable<T> filter(CloseableIterable<T> rows,
                                                Function<T, Long> rowToPosition,
                                                Set<Long> deleteSet,
                                                DeleteSetter<T> mutationWrapper) {
    if (deleteSet.isEmpty()) {
      return rows;
    }

    PositionSetDeleteMarker<T> iterable = new PositionSetDeleteMarker<>(rowToPosition, deleteSet, mutationWrapper);
    return iterable.setDeleted(rows);
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

  public static <T> CloseableIterable<T> streamingDeleteMarker(CloseableIterable<T> rows,
                                                               Function<T, Long> rowToPosition,
                                                               CloseableIterable<Long> posDeletes,
                                                               DeleteSetter<T> deleteSetter) {
    return new PositionStreamDeleteMarker<>(rows, rowToPosition, posDeletes, deleteSetter);
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

  private static class PositionSetDeleteMarker<T> extends DeleteMarker<T> {
    private final Function<T, Long> rowToPosition;
    private final Set<Long> deleteSet;

    private PositionSetDeleteMarker(Function<T, Long> rowToPosition,
                                    Set<Long> deleteSet,
                                    DeleteSetter<T> deleteSetter) {
      super(deleteSetter);
      this.rowToPosition = rowToPosition;
      this.deleteSet = deleteSet;
    }

    @Override
    protected boolean shouldDelete(T row) {
      return deleteSet.contains(rowToPosition.apply(row));
    }
  }

  private static class PositionStreamDeleteMarker<T> extends CloseableGroup implements CloseableIterable<T> {
    private final CloseableIterable<T> rows;
    private final Function<T, Long> extractPos;
    private final CloseableIterable<Long> deletePositions;
    private final DeleteSetter<T> deleteSetter;

    private PositionStreamDeleteMarker(CloseableIterable<T> rows,
                                       Function<T, Long> extractPos,
                                       CloseableIterable<Long> deletePositions,
                                       DeleteSetter<T> deleteSetter) {
      this.rows = rows;
      this.extractPos = extractPos;
      this.deletePositions = deletePositions;
      this.deleteSetter = deleteSetter;
    }

    @Override
    public CloseableIterator<T> iterator() {
      CloseableIterator<Long> deletePosIterator = deletePositions.iterator();

      CloseableIterator<T> iter;
      if (deletePosIterator.hasNext()) {
        iter = new PositionDeleteMarkerIterator(rows.iterator(), deletePosIterator);
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

    private class PositionDeleteMarkerIterator implements CloseableIterator<T> {
      private final CloseableIterator<T> items;
      private final CloseableIterator<Long> deletePosIterator;
      private long nextDeletePos;

      protected PositionDeleteMarkerIterator(CloseableIterator<T> items, CloseableIterator<Long> deletePositions) {
        this.items = items;
        this.deletePosIterator = deletePositions;
        this.nextDeletePos = deletePosIterator.next();
      }

      @Override
      public boolean hasNext() {
        return items.hasNext();
      }

      @Override
      public T next() {
        if (!items.hasNext()) {
          throw new NoSuchElementException();
        }

        T current = items.next();

        deleteSetter.wrap(current);
        if (!deleteSetter.isDeleted() && shouldDelete(current)) {
          return deleteSetter.wrap(current).setDeleted();
        } else {
          return current;
        }
      }

      private boolean shouldDelete(T row) {
        long currentPos = extractPos.apply(row);
        if (currentPos < nextDeletePos) {
          return false;
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

        return !keep;
      }

      @Override
      public void close() {
        try {
          items.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close the iterator", e);
        }

        try {
          deletePosIterator.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close delete positions iterator", e);
        }
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
