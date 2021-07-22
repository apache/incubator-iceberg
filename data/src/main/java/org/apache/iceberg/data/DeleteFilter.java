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

package org.apache.iceberg.data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Filter;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.parquet.Preconditions;

public abstract class DeleteFilter<T> {
  private static final long DEFAULT_SET_FILTER_THRESHOLD = 100_000L;
  private static final Schema POS_DELETE_SCHEMA = new Schema(
      MetadataColumns.DELETE_FILE_PATH,
      MetadataColumns.DELETE_FILE_POS);

  private final long setFilterThreshold;
  private final DataFile dataFile;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final Schema requiredSchema;
  private final Accessor<StructLike> posAccessor;
  private Integer deleteMarkerIndex = null;

  protected DeleteFilter(FileScanTask task, Schema tableSchema, Schema requestedSchema) {
    this.setFilterThreshold = DEFAULT_SET_FILTER_THRESHOLD;
    this.dataFile = task.file();

    ImmutableList.Builder<DeleteFile> posDeleteBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> eqDeleteBuilder = ImmutableList.builder();
    for (DeleteFile delete : task.deletes()) {
      switch (delete.content()) {
        case POSITION_DELETES:
          posDeleteBuilder.add(delete);
          break;
        case EQUALITY_DELETES:
          eqDeleteBuilder.add(delete);
          break;
        default:
          throw new UnsupportedOperationException("Unknown delete file content: " + delete.content());
      }
    }

    this.posDeletes = posDeleteBuilder.build();
    this.eqDeletes = eqDeleteBuilder.build();
    this.requiredSchema = fileProjection(tableSchema, requestedSchema, posDeletes, eqDeletes);
    this.posAccessor = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
  }

  public Schema requiredSchema() {
    return requiredSchema;
  }

  protected int deleteMarkerIndex() {
    if (deleteMarkerIndex != null) {
      return deleteMarkerIndex;
    }

    int index = 0;
    for (Types.NestedField field : requiredSchema().columns()) {
      if (field.fieldId() != MetadataColumns.IS_DELETED.fieldId()) {
        index = index + 1;
      } else {
        break;
      }
    }

    deleteMarkerIndex = index;

    return deleteMarkerIndex;
  }

  protected abstract Consumer<T> deleteMarker();

  protected abstract boolean isDeletedRow(T row);

  Accessor<StructLike> posAccessor() {
    return posAccessor;
  }

  protected abstract StructLike asStructLike(T record);

  protected abstract InputFile getInputFile(String location);

  protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

  private Filter<T> deletedRowsSelector() {
    return new Filter<T>() {
      @Override
      protected boolean shouldKeep(T item) {
        return isDeletedRow(item);
      }
    };
  }

  private Predicate<T> buildEqDeletePredicate() {
    if (eqDeletes.isEmpty()) {
      return null;
    }
    Predicate<T> isDeleted = null;

    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds = Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
      filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry : filesByDeleteIds.asMap().entrySet()) {
      Set<Integer> ids = entry.getKey();
      Iterable<DeleteFile> deletes = entry.getValue();

      Schema deleteSchema = TypeUtil.select(requiredSchema, ids);

      // a projection to select and reorder fields of the file schema to match the delete rows
      StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

      Iterable<CloseableIterable<Record>> deleteRecords = Iterables.transform(deletes,
          delete -> openDeletes(delete, deleteSchema));
      StructLikeSet deleteSet = Deletes.toEqualitySet(
          // copy the delete records because they will be held in a set
          CloseableIterable.transform(CloseableIterable.concat(deleteRecords), Record::copy),
          deleteSchema.asStruct());

      isDeleted = isDeleted == null ? record -> deleteSet.contains(projectRow.wrap(asStructLike(record))) :
              isDeleted.or(record -> deleteSet.contains(projectRow.wrap(asStructLike(record))));
    }

    return isDeleted;
  }

  private Predicate<T> buildPosDeletePredicate() {
    if (posDeletes.isEmpty()) {
      return null;
    }

    List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);
    Set<Long> deleteSet = Deletes.toPositionSet(dataFile.path(), CloseableIterable.concat(deletes));
    if (deleteSet.isEmpty()) {
      return null;
    }

    return record -> deleteSet.contains(pos(record));
  }

  public CloseableIterable<T> keepRowsFromDeletes(CloseableIterable<T> records) {
    Predicate<T> isDeletedFromPosDeletes = buildPosDeletePredicate();
    if (isDeletedFromPosDeletes == null) {
      return keepRowsFromEqualityDeletes(records);
    }

    Predicate<T> isDeletedFromEqDeletes = buildEqDeletePredicate();
    if (isDeletedFromEqDeletes == null) {
      return keepRowsFromPosDeletes(records);
    }

    CloseableIterable<T> markedRecords;

    if (posDeletes.stream().mapToLong(DeleteFile::recordCount).sum() < setFilterThreshold) {
      markedRecords = CloseableIterable.transform(records, record -> {
        if (isDeletedFromPosDeletes.test(record) || isDeletedFromEqDeletes.test(record)) {
          deleteMarker().accept(record);
        }
        return record;
      });

    } else {
      List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);
      markedRecords = CloseableIterable.transform(Deletes.streamingDeletedRowMarker(records, this::pos,
          Deletes.deletePositions(dataFile.path(), deletes), deleteMarker()), record -> {
          if (!isDeletedRow(record) && isDeletedFromEqDeletes.test(record)) {
            deleteMarker().accept(record);
          }
          return record;
        });
    }
    return deletedRowsSelector().filter(markedRecords);
  }

  private CloseableIterable<T> selectRowsFromDeletes(CloseableIterable<T> records, Predicate<T> isDeleted) {
    CloseableIterable<T> markedRecords = CloseableIterable.transform(records, record -> {
      if (isDeleted.test(record)) {
        deleteMarker().accept(record);
      }
      return record;
    });

    return deletedRowsSelector().filter(markedRecords);
  }

  public CloseableIterable<T> keepRowsFromEqualityDeletes(CloseableIterable<T> records) {
    // Predicate to test whether a row has been deleted by equality deletions.
    Predicate<T> isDeleted = buildEqDeletePredicate();
    if (isDeleted == null) {
      return CloseableIterable.empty();
    }

    return selectRowsFromDeletes(records, isDeleted);
  }

  public CloseableIterable<T> keepRowsFromPosDeletes(CloseableIterable<T> records) {
    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeletes.stream().mapToLong(DeleteFile::recordCount).sum() < setFilterThreshold) {
      // Predicate to test whether a row has been deleted by equality deletions.
      Predicate<T> isDeleted = buildPosDeletePredicate();
      if (isDeleted == null) {
        return CloseableIterable.empty();
      }
      return selectRowsFromDeletes(records, isDeleted);
    } else {
      List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);
      CloseableIterable<T> markedRecords = Deletes.streamingDeletedRowMarker(records, this::pos,
              Deletes.deletePositions(dataFile.path(), deletes), deleteMarker());

      return deletedRowsSelector().filter(markedRecords);
    }
  }

  private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    // Predicate to test whether a row should be visible to user after applying equality deletions.
    Predicate<T> isDeleted = buildEqDeletePredicate();
    if (isDeleted == null) {
      return records;
    }

    CloseableIterable<T> markedRecords = CloseableIterable.transform(records, record -> {
      if (isDeleted.test(record)) {
        deleteMarker().accept(record);
      }
      return record;
    });

    Filter<T> remainingRowsFilter = new Filter<T>() {
      @Override
      protected boolean shouldKeep(T item) {
        return !isDeletedRow(item);
      }
    };

    return remainingRowsFilter.filter(markedRecords);
  }

  private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
      return records;
    }

    List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);

    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeletes.stream().mapToLong(DeleteFile::recordCount).sum() < setFilterThreshold) {
      return Deletes.filter(
          records, this::pos,
          Deletes.toPositionSet(dataFile.path(), CloseableIterable.concat(deletes)));
    }

    CloseableIterable<T> markedRecords = Deletes.streamingDeletedRowMarker(records, this::pos,
            Deletes.deletePositions(dataFile.path(), deletes), deleteMarker());

    Filter<T> remainingRowsFilter = new Filter<T>() {
      @Override
      protected boolean shouldKeep(T item) {
        return !isDeletedRow(item);
      }
    };

    return remainingRowsFilter.filter(markedRecords);
  }

  private CloseableIterable<Record> openPosDeletes(DeleteFile file) {
    return openDeletes(file, POS_DELETE_SCHEMA);
  }

  private CloseableIterable<Record> openDeletes(DeleteFile deleteFile, Schema deleteSchema) {
    InputFile input = getInputFile(deleteFile.path().toString());
    switch (deleteFile.format()) {
      case AVRO:
        return Avro.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(DataReader::create)
            .build();

      case PARQUET:
        Parquet.ReadBuilder builder = Parquet.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema));

        if (deleteFile.content() == FileContent.POSITION_DELETES) {
          builder.filter(Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), dataFile.path()));
        }

        return builder.build();

      case ORC:
      default:
        throw new UnsupportedOperationException(String.format(
            "Cannot read deletes, %s is not a supported format: %s", deleteFile.format().name(), deleteFile.path()));
    }
  }

  private static Schema fileProjection(Schema tableSchema, Schema requestedSchema,
                                       List<DeleteFile> posDeletes, List<DeleteFile> eqDeletes) {
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

    Set<Integer> requiredIds = Sets.newLinkedHashSet();
    if (!posDeletes.isEmpty()) {
      requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    for (DeleteFile eqDelete : eqDeletes) {
      requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    requiredIds.add(MetadataColumns.IS_DELETED.fieldId());

    Set<Integer> missingIds = Sets.newLinkedHashSet(
        Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
      return requestedSchema;
    }

    // TODO: support adding nested columns. this will currently fail when finding nested columns to add
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
      if (fieldId == MetadataColumns.ROW_POSITION.fieldId() || fieldId == MetadataColumns.IS_DELETED.fieldId()) {
        continue; // add _pos and _deleted at the end
      }

      Types.NestedField field = tableSchema.asStruct().field(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find required field for ID %s", fieldId);

      columns.add(field);
    }

    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
      columns.add(MetadataColumns.ROW_POSITION);
    }

    if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
      columns.add(MetadataColumns.IS_DELETED);
    }

    return new Schema(columns);
  }
}
