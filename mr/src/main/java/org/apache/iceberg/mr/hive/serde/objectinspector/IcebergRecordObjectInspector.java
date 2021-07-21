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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public final class IcebergRecordObjectInspector extends StructObjectInspector {

  private static final IcebergRecordObjectInspector EMPTY =
          new IcebergRecordObjectInspector(Types.StructType.of(), Collections.emptyList());

  private final List<IcebergRecordStructField> structFields;

  public IcebergRecordObjectInspector(Types.StructType structType, List<ObjectInspector> objectInspectors) {
    Preconditions.checkArgument(structType.fields().size() == objectInspectors.size());

    this.structFields = Lists.newArrayListWithExpectedSize(structType.fields().size());

    int position = 0;

    for (Types.NestedField field : structType.fields()) {
      ObjectInspector oi = objectInspectors.get(position);
      Types.NestedField fieldInLowercase = Types.NestedField.of(field.fieldId(), field.isOptional(),
              field.name().toLowerCase(), field.type(), field.doc());
      IcebergRecordStructField structField = new IcebergRecordStructField(fieldInLowercase, oi, position);
      structFields.add(structField);
      position++;
    }
  }

  public static IcebergRecordObjectInspector empty() {
    return EMPTY;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public StructField getStructFieldRef(String name) {
    return ObjectInspectorUtils.getStandardStructFieldRef(name, structFields);
  }

  @Override
  public Object getStructFieldData(Object o, StructField structField) {
    return ((Record) o).getField(((IcebergRecordStructField) structField).getFieldName());
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object o) {
    Record record = (Record) o;
    return structFields
            .stream()
            .map(f -> record.get(f.position()))
            .collect(Collectors.toList());
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IcebergRecordObjectInspector that = (IcebergRecordObjectInspector) o;
    return structFields.equals(that.structFields);
  }

  @Override
  public int hashCode() {
    return structFields.hashCode();
  }

  private static class IcebergRecordStructField implements StructField {

    private final Types.NestedField field;
    private final ObjectInspector oi;
    private final int position;

    IcebergRecordStructField(Types.NestedField field, ObjectInspector oi, int position) {
      this.field = field;
      this.oi = oi;
      this.position = position; // position in the record
    }

    @Override
    public String getFieldName() {
      return field.name();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public int getFieldID() {
      return field.fieldId();
    }

    @Override
    public String getFieldComment() {
      return field.doc();
    }

    int position() {
      return position;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      IcebergRecordStructField that = (IcebergRecordStructField) o;
      return field.equals(that.field) && oi.equals(that.oi) && position == that.position;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, oi, position);
    }

  }

}
