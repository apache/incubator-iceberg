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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;

public class FileHelpers {
  private FileHelpers() {
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(Table table, OutputFile out,
                                                                    List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    return writeDeleteFile(table, out, null, deletes);
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(Table table, OutputFile out, StructLike partition,
                                                                    List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    PositionDeleteWriter<?> writer = Parquet.writeDeletes(out)
        .forTable(table)
        .partition(partition)
        .overwrite()
        .buildPositionWriter();

    try (Closeable toClose = writer) {
      for (Pair<CharSequence, Long> delete : deletes) {
        writer.delete(delete.first(), delete.second());
      }
    }

    return Pair.of(writer.toDeleteFile(), writer.referencedDataFiles());
  }

  public static DeleteFile writeDeleteFile(Table table, OutputFile out, List<Record> deletes, Schema deleteRowSchema)
      throws IOException {
    return writeDeleteFile(table, out, null, deletes, deleteRowSchema);
  }

  public static DeleteFile writeDeleteFile(Table table, OutputFile out, StructLike partition,
                                           List<Record> deletes, Schema deleteRowSchema) throws IOException {
    EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(out)
        .forTable(table)
        .partition(partition)
        .rowSchema(deleteRowSchema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .equalityFieldIds(deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray())
        .buildEqualityWriter();

    try (Closeable toClose = writer) {
      writer.deleteAll(deletes);
    }

    return writer.toDeleteFile();
  }

  public static DataFile writeDataFile(Table table, OutputFile out, List<Record> rows) throws IOException {
    FileAppender<Record> writer = Parquet.write(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .schema(table.schema())
        .overwrite()
        .build();

    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  public static DataFile writeDataFile(Table table, OutputFile out, StructLike partition, List<Record> rows)
      throws IOException {
    FileAppender<Record> writer = Parquet.write(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .schema(table.schema())
        .overwrite()
        .build();

    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withPartition(partition)
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }
}
