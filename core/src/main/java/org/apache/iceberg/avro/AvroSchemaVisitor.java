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

package org.apache.iceberg.avro;

import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public abstract class AvroSchemaVisitor<T> {
  public static <T> T visit(Schema schema, AvroSchemaVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        // check to make sure this hasn't been visited before
        String name = schema.getFullName();
        Preconditions.checkState(!visitor.recordLevels.contains(name),
            "Cannot process recursive Avro record %s", name);

        visitor.recordLevels.push(name);
        visitor.parentSchemas.push(schema);

        List<Schema.Field> fields = schema.getFields();
        List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
        List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
        for (Schema.Field field : schema.getFields()) {
          names.add(field.name());
          T result = visitWithName(field.name(), field.schema(), visitor);
          results.add(result);
        }

        visitor.recordLevels.pop();
        visitor.parentSchemas.pop();

        return visitor.record(schema, names, results);

      case UNION:
        List<Schema> types = schema.getTypes();
        List<T> options = Lists.newArrayListWithExpectedSize(types.size());
        for (Schema type : types) {
          options.add(visit(type, visitor));
        }
        return visitor.union(schema, options);

      case ARRAY:
        if (schema.getLogicalType() instanceof LogicalMap) {
          T result = visit(schema.getElementType(), visitor);
          return visitor.array(schema, result);
        } else {
          visitor.parentSchemas.push(schema);
          T result = visitWithName("element", schema.getElementType(), visitor);
          visitor.parentSchemas.pop();
          return visitor.array(schema, result);
        }

      case MAP:
        visitor.parentSchemas.push(schema);
        T result = visitWithName("value", schema.getValueType(), visitor);
        visitor.parentSchemas.pop();
        return visitor.map(schema, result);

      default:
        return visitor.primitive(schema);
    }
  }

  private Deque<String> recordLevels = Lists.newLinkedList();
  private Deque<String> fieldNames = Lists.newLinkedList();
  private Deque<Schema> parentSchemas = Lists.newLinkedList();

  protected Deque<String> fieldNames() {
    return fieldNames;
  }

  protected String lastFieldName() {
    return fieldNames.peekLast();
  }

  protected Schema parentSchema() {
    return parentSchemas.peek();
  }

  private static <T> T visitWithName(String name, Schema schema, AvroSchemaVisitor<T> visitor) {
    try {
      visitor.fieldNames.addLast(name);
      return visit(schema, visitor);
    } finally {
      visitor.fieldNames.removeLast();
    }
  }

  public T record(Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(Schema union, List<T> options) {
    return null;
  }

  public T array(Schema array, T element) {
    return null;
  }

  public T map(Schema map, T value) {
    return null;
  }

  public T primitive(Schema primitive) {
    return null;
  }
}
