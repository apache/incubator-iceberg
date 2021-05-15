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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class GenericManifestFile
    implements ManifestFile, StructLike, IndexedRecord, SchemaConstructable, Serializable {
  private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(
      ManifestFile.schema(), "manifest_file");

  private transient Schema avroSchema; // not final for Java serialization
  private int[] fromProjectionPos;

  // data fields
  private InputFile file = null;
  private String manifestPath = null;
  private Long length = null;
  private int specId = -1;
  private ManifestContent content = ManifestContent.DATA;
  private long sequenceNumber = 0;
  private long minSequenceNumber = 0;
  private Long snapshotId = null;
  private Integer addedFilesCount = null;
  private Integer existingFilesCount = null;
  private Integer deletedFilesCount = null;
  private Long addedRowsCount = null;
  private Long existingRowsCount = null;
  private Long deletedRowsCount = null;
  private PartitionFieldSummary[] partitions = null;
  private byte[] keyMetadata = null;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericManifestFile(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;

    List<Types.NestedField> fields = AvroSchemaUtil.convert(avroSchema).asStructType().fields();
    List<Types.NestedField> allFields = ManifestFile.schema().asStruct().fields();

    this.fromProjectionPos = new int[fields.size()];
    for (int i = 0; i < fromProjectionPos.length; i += 1) {
      boolean found = false;
      for (int j = 0; j < allFields.size(); j += 1) {
        if (fields.get(i).fieldId() == allFields.get(j).fieldId()) {
          found = true;
          fromProjectionPos[i] = j;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
      }
    }
  }

  GenericManifestFile(InputFile file, int specId) {
    this.avroSchema = AVRO_SCHEMA;
    this.file = file;
    this.manifestPath = file.location();
    this.length = null; // lazily loaded from file
    this.specId = specId;
    this.sequenceNumber = 0;
    this.minSequenceNumber = 0;
    this.snapshotId = null;
    this.addedFilesCount = null;
    this.addedRowsCount = null;
    this.existingFilesCount = null;
    this.existingRowsCount = null;
    this.deletedFilesCount = null;
    this.deletedRowsCount = null;
    this.partitions = null;
    this.fromProjectionPos = null;
    this.keyMetadata = null;
  }

  public GenericManifestFile(String path, long length, int specId, ManifestContent content,
                             long sequenceNumber, long minSequenceNumber, Long snapshotId,
                             int addedFilesCount, long addedRowsCount, int existingFilesCount,
                             long existingRowsCount, int deletedFilesCount, long deletedRowsCount,
                             List<PartitionFieldSummary> partitions, ByteBuffer keyMetadata) {
    this.avroSchema = AVRO_SCHEMA;
    this.manifestPath = path;
    this.length = length;
    this.specId = specId;
    this.content = content;
    this.sequenceNumber = sequenceNumber;
    this.minSequenceNumber = minSequenceNumber;
    this.snapshotId = snapshotId;
    this.addedFilesCount = addedFilesCount;
    this.addedRowsCount = addedRowsCount;
    this.existingFilesCount = existingFilesCount;
    this.existingRowsCount = existingRowsCount;
    this.deletedFilesCount = deletedFilesCount;
    this.deletedRowsCount = deletedRowsCount;
    this.partitions = partitions == null ? null : partitions.toArray(new PartitionFieldSummary[0]);
    this.fromProjectionPos = null;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic manifest file to copy.
   */
  private GenericManifestFile(GenericManifestFile toCopy) {
    this.avroSchema = toCopy.avroSchema;
    this.manifestPath = toCopy.manifestPath;
    this.length = toCopy.length;
    this.specId = toCopy.specId;
    this.content = toCopy.content;
    this.sequenceNumber = toCopy.sequenceNumber;
    this.minSequenceNumber = toCopy.minSequenceNumber;
    this.snapshotId = toCopy.snapshotId;
    this.addedFilesCount = toCopy.addedFilesCount;
    this.addedRowsCount = toCopy.addedRowsCount;
    this.existingFilesCount = toCopy.existingFilesCount;
    this.existingRowsCount = toCopy.existingRowsCount;
    this.deletedFilesCount = toCopy.deletedFilesCount;
    this.deletedRowsCount = toCopy.deletedRowsCount;
    if (toCopy.partitions != null) {
      this.partitions = Stream.of(toCopy.partitions)
          .map(PartitionFieldSummary::copy)
          .toArray(PartitionFieldSummary[]::new);
    } else {
      this.partitions = null;
    }
    this.fromProjectionPos = toCopy.fromProjectionPos;
    this.keyMetadata = toCopy.keyMetadata == null ? null : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
  }

  /**
   * Constructor for Java serialization.
   */
  GenericManifestFile() {
  }

  @Override
  public String path() {
    return manifestPath;
  }

  public Long lazyLength() {
    if (length == null) {
      if (file != null) {
        // this was created from an input file and length is lazily loaded
        this.length = file.getLength();
      } else {
        // this was loaded from a file without projecting length, throw an exception
        return null;
      }
    }
    return length;
  }

  @Override
  public long length() {
    return lazyLength();
  }

  @Override
  public int partitionSpecId() {
    return specId;
  }

  @Override
  public ManifestContent content() {
    return content;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long minSequenceNumber() {
    return minSequenceNumber;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    return addedFilesCount;
  }

  @Override
  public Long addedRowsCount() {
    return addedRowsCount;
  }

  @Override
  public Integer existingFilesCount() {
    return existingFilesCount;
  }

  @Override
  public Long existingRowsCount() {
    return existingRowsCount;
  }

  @Override
  public Integer deletedFilesCount() {
    return deletedFilesCount;
  }

  @Override
  public Long deletedRowsCount() {
    return deletedRowsCount;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return partitions == null ? null : Arrays.asList(partitions);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata == null ? null : ByteBuffer.wrap(keyMetadata);
  }

  @Override
  public int size() {
    return ManifestFile.schema().columns().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public Object get(int i) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        return manifestPath;
      case 1:
        return lazyLength();
      case 2:
        return specId;
      case 3:
        return content.id();
      case 4:
        return sequenceNumber;
      case 5:
        return minSequenceNumber;
      case 6:
        return snapshotId;
      case 7:
        return addedFilesCount;
      case 8:
        return existingFilesCount;
      case 9:
        return deletedFilesCount;
      case 10:
        return addedRowsCount;
      case 11:
        return existingRowsCount;
      case 12:
        return deletedRowsCount;
      case 13:
        return partitions();
      case 14:
        return keyMetadata();
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void set(int i, T value) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.manifestPath = value.toString();
        return;
      case 1:
        this.length = (Long) value;
        return;
      case 2:
        this.specId = (Integer) value;
        return;
      case 3:
        this.content = value != null ? ManifestContent.values()[(Integer) value] : ManifestContent.DATA;
        return;
      case 4:
        this.sequenceNumber = value != null ? (Long) value : 0;
        return;
      case 5:
        this.minSequenceNumber = value != null ? (Long) value : 0;
        return;
      case 6:
        this.snapshotId = (Long) value;
        return;
      case 7:
        this.addedFilesCount = (Integer) value;
        return;
      case 8:
        this.existingFilesCount = (Integer) value;
        return;
      case 9:
        this.deletedFilesCount = (Integer) value;
        return;
      case 10:
        this.addedRowsCount = (Long) value;
        return;
      case 11:
        this.existingRowsCount = (Long) value;
        return;
      case 12:
        this.deletedRowsCount = (Long) value;
        return;
      case 13:
        this.partitions = value == null ? null :
            ((List<PartitionFieldSummary>) value).toArray(new PartitionFieldSummary[0]);
        return;
      case 14:
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  @Override
  public ManifestFile copy() {
    return new GenericManifestFile(this);
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof GenericManifestFile)) {
      return false;
    }
    GenericManifestFile that = (GenericManifestFile) other;
    return Objects.equal(manifestPath, that.manifestPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(manifestPath);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", manifestPath)
        .add("length", length)
        .add("partition_spec_id", specId)
        .add("added_snapshot_id", snapshotId)
        .add("added_data_files_count", addedFilesCount)
        .add("added_rows_count", addedRowsCount)
        .add("existing_data_files_count", existingFilesCount)
        .add("existing_rows_count", existingRowsCount)
        .add("deleted_data_files_count", deletedFilesCount)
        .add("deleted_rows_count", deletedRowsCount)
        .add("partitions", partitions)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .toString();
  }

  public static CopyBuilder copyOf(ManifestFile manifestFile) {
    return new CopyBuilder(manifestFile);
  }

  public static class CopyBuilder {
    private final GenericManifestFile manifestFile;

    private CopyBuilder(ManifestFile toCopy) {
      if (toCopy instanceof GenericManifestFile) {
        this.manifestFile = new GenericManifestFile((GenericManifestFile) toCopy);
      } else {
        this.manifestFile = new GenericManifestFile(
            toCopy.path(), toCopy.length(), toCopy.partitionSpecId(), toCopy.content(),
            toCopy.sequenceNumber(), toCopy.minSequenceNumber(), toCopy.snapshotId(),
            toCopy.addedFilesCount(), toCopy.addedRowsCount(), toCopy.existingFilesCount(),
            toCopy.existingRowsCount(), toCopy.deletedFilesCount(), toCopy.deletedRowsCount(),
            copyList(toCopy.partitions(), PartitionFieldSummary::copy), toCopy.keyMetadata());
      }
    }

    public CopyBuilder withSnapshotId(Long newSnapshotId) {
      manifestFile.snapshotId = newSnapshotId;
      return this;
    }

    public ManifestFile build() {
      return manifestFile;
    }
  }

  private static <E, R> List<R> copyList(List<E> list, Function<E, R> transform) {
    if (list != null) {
      List<R> copy = Lists.newArrayListWithExpectedSize(list.size());
      for (E element : list) {
        copy.add(transform.apply(element));
      }
      return copy;
    }
    return null;
  }
}
