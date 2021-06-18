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

package org.apache.iceberg.util;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

public class StructProjection implements StructLike {
  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   * <p>
   * This projection does not work with repeated types like lists and maps.
   *
   * @param schema schema of rows wrapped by this projection
   * @param ids field ids from the row schema to project
   * @return a wrapper to project rows
   */
  public static StructProjection create(Schema schema, Set<Integer> ids) {
    StructType structType = schema.asStruct();
    return create(structType, ids);
  }

  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   * <p>
   * This projection does not work with repeated types like lists and maps.
   *
   * @param structType struct type of rows wrapped by this projection
   * @param ids field ids from the row schema to project
   * @return a wrapper to project rows
   */
  public static StructProjection create(StructType structType, Set<Integer> ids) {
    return new StructProjection(structType, TypeUtil.select(structType, ids));
  }

  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   * <p>
   * This projection does not work with repeated types like lists and maps.
   *
   * @param dataSchema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static StructProjection create(Schema dataSchema, Schema projectedSchema) {
    return new StructProjection(dataSchema.asStruct(), projectedSchema.asStruct());
  }

  private final StructType type;
  private final int[] positionMap;
  private final StructProjection[] nestedProjections;
  private StructLike struct;

  private StructProjection(StructProjection other) {
    this.type = other.type;
    this.positionMap = other.positionMap;
    this.nestedProjections = other.nestedProjections;
    this.struct = other.struct;
  }

  private StructProjection(StructType structType, StructType projection) {
    this.type = projection;
    this.positionMap = new int[projection.fields().size()];
    this.nestedProjections = new StructProjection[projection.fields().size()];

    // set up the projection positions and any nested projections that are needed
    List<Types.NestedField> dataFields = structType.fields();
    for (int pos = 0; pos < positionMap.length; pos += 1) {
      Types.NestedField projectedField = projection.fields().get(pos);

      boolean found = false;
      for (int i = 0; !found && i < dataFields.size(); i += 1) {
        Types.NestedField dataField = dataFields.get(i);
        if (projectedField.fieldId() == dataField.fieldId()) {
          found = true;
          positionMap[pos] = i;
          switch (projectedField.type().typeId()) {
            case STRUCT:
              nestedProjections[pos] = new StructProjection(
                  dataField.type().asStructType(), projectedField.type().asStructType());
              break;
            case MAP:
            case LIST:
              throw new IllegalArgumentException(String.format("Cannot project list or map field: %s", projectedField));
            default:
              nestedProjections[pos] = null;
          }
        }
      }

      if (!found) {
        throw new IllegalArgumentException(String.format("Cannot find field %s in %s", projectedField, structType));
      }
    }
  }

  public StructProjection wrap(StructLike newStruct) {
    this.struct = newStruct;
    return this;
  }

  public StructProjection copy() {
    return new StructProjection(this);
  }

  public StructType type() {
    return type;
  }

  @Override
  public int size() {
    return type.fields().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (nestedProjections[pos] != null) {
      return javaClass.cast(nestedProjections[pos].wrap(struct.get(positionMap[pos], StructLike.class)));
    }

    return struct.get(positionMap[pos], javaClass);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot set fields in a TypeProjection");
  }
}
