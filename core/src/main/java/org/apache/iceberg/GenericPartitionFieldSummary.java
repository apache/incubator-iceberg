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
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class GenericPartitionFieldSummary
    implements PartitionFieldSummary, StructLike, Record, SchemaConstructable, Serializable {
  private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(PartitionFieldSummary.getType());

  private transient Schema avroSchema; // not final for Java serialization
  private int[] fromProjectionPos;
  private Map<String, Integer> nameToProjectedPos;

  // data fields
  private boolean containsNull = false;
  private byte[] lowerBound = null;
  private byte[] upperBound = null;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericPartitionFieldSummary(Schema avroSchema) {
    this.avroSchema = avroSchema;

    List<Types.NestedField> fields = AvroSchemaUtil.convert(avroSchema)
        .asNestedType()
        .asStructType()
        .fields();
    List<Types.NestedField> allFields = PartitionFieldSummary.getType().fields();
    this.nameToProjectedPos = Maps.newHashMap();
    this.fromProjectionPos = new int[fields.size()];
    for (int i = 0; i < fromProjectionPos.length; i += 1) {
      boolean found = false;
      for (int j = 0; j < allFields.size(); j += 1) {
        if (fields.get(i).fieldId() == allFields.get(j).fieldId()) {
          found = true;
          fromProjectionPos[i] = j;
          nameToProjectedPos.put(fields.get(i).name(), i);
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
      }
    }
  }

  public GenericPartitionFieldSummary(boolean containsNull, ByteBuffer lowerBound,
                                      ByteBuffer upperBound) {
    this.avroSchema = AVRO_SCHEMA;
    this.containsNull = containsNull;
    this.lowerBound = ByteBuffers.toByteArray(lowerBound);
    this.upperBound = ByteBuffers.toByteArray(upperBound);
    this.fromProjectionPos = null;
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic manifest file to copy.
   */
  private GenericPartitionFieldSummary(GenericPartitionFieldSummary toCopy) {
    this.avroSchema = toCopy.avroSchema;
    this.containsNull = toCopy.containsNull;
    this.lowerBound = toCopy.lowerBound == null ? null : Arrays.copyOf(toCopy.lowerBound, toCopy.lowerBound.length);
    this.upperBound = toCopy.upperBound == null ? null : Arrays.copyOf(toCopy.upperBound, toCopy.upperBound.length);
    this.fromProjectionPos = toCopy.fromProjectionPos;
  }

  /**
   * Constructor for Java serialization.
   */
  GenericPartitionFieldSummary() {
  }

  @Override
  public boolean containsNull() {
    return containsNull;
  }

  @Override
  public ByteBuffer lowerBound() {
    return lowerBound != null ? ByteBuffer.wrap(lowerBound) : null;
  }

  @Override
  public ByteBuffer upperBound() {
    return upperBound != null ? ByteBuffer.wrap(upperBound) : null;
  }

  @Override
  public int size() {
    return PartitionFieldSummary.getType().fields().size();
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
        return containsNull;
      case 1:
        return lowerBound();
      case 2:
        return upperBound();
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public Types.StructType struct() {
    return PartitionFieldSummary.getType();
  }

  @Override
  public Object getField(String name) {
    return get(nameToProjectedPos.get(name));
  }

  @Override
  public void setField(String name, Object value) {
    int pos = nameToProjectedPos.get(name);
    set(pos, value);
  }

  @Override
  public Record copy(Map<String, Object> overwriteValues) {
    return null;
  }

  @Override
  public GenericPartitionFieldSummary copy() {
    return new GenericPartitionFieldSummary(this);
  }

  @Override
  public <T> void set(int i, T value) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        this.containsNull = (Boolean) value;
        return;
      case 1:
        this.lowerBound = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 2:
        this.upperBound = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("contains_null", containsNull)
        .add("lower_bound", lowerBound)
        .add("upper_bound", upperBound)
        .toString();
  }
}
