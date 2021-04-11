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
import org.apache.iceberg.deletes.DeleteSetter;
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
import org.apache.iceberg.util.DeleteMarker;
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
  private final DeleteSetter<T> deleteSetter;
  private final Accessor<StructLike> posAccessor;

  protected DeleteFilter(FileScanTask task,
                         Schema tableSchema,
                         Schema requestedSchema,
                         DeleteSetter<T> deleteSetter) {
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
    this.deleteSetter = deleteSetter;
    this.posAccessor = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
  }

  public Schema requiredSchema() {
    return requiredSchema;
  }

  Accessor<StructLike> posAccessor() {
    return posAccessor;
  }

  protected abstract StructLike asStructLike(T record);

  protected abstract InputFile getInputFile(String location);

  protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    CloseableIterable<T> rawRecords = applyEqDeletes(applyPosDeletes(records));
    return CloseableIterable.filter(rawRecords, row -> !deleteSetter.wrap(row).isDeleted());
  }

  private Predicate<T> buildEqDeletesPredicate() {
    Predicate<T> isDelete = t -> false;
    if (eqDeletes.isEmpty()) {
      return isDelete;
    }

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

      Predicate<T> isInDeleteSet = record -> deleteSet.contains(projectRow.wrap(asStructLike(record)));
      isDelete = isDelete.or(isInDeleteSet);
    }

    return isDelete;
  }

  public CloseableIterable<T> findDeletedRows(CloseableIterable<T> records) {
    CloseableIterable<T> rawRecords = applyEqDeletes(applyPosDeletes(records));
    return CloseableIterable.filter(rawRecords, row -> deleteSetter.wrap(row).isDeleted());
  }

  private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    // Predicate to test whether a row should be visible to user after applying equality deletions.
    Predicate<T> remainingRows = buildEqDeletesPredicate().negate();

    DeleteMarker<T> deleteMarker = new DeleteMarker<T>(deleteSetter) {
      @Override
      protected boolean shouldDelete(T item) {
        return remainingRows.test(item);
      }
    };

    return deleteMarker.setDeleted(records);
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
          Deletes.toPositionSet(dataFile.path(), CloseableIterable.concat(deletes)),
          deleteSetter);
    }

    return Deletes.streamingDeleteMarker(records,
        this::pos,
        Deletes.deletePositions(dataFile.path(), deletes),
        deleteSetter);
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

    Set<Integer> missingIds = Sets.newLinkedHashSet(
        Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
      return requestedSchema;
    }

    // TODO: support adding nested columns. this will currently fail when finding nested columns to add
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
      if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
        continue; // add _pos at the end
      }

      Types.NestedField field = tableSchema.asStruct().field(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find required field for ID %s", fieldId);

      columns.add(field);
    }

    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
      columns.add(MetadataColumns.ROW_POSITION);
    }

    return new Schema(columns);
  }
}
