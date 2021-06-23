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
import org.apache.avro.Schema;

/**
 * This util class helps Avro DatumWriter builders to retrieve the correct field Id when building Avro DatumWriters
 * with visitor pattern.
 */
public class AvroWriterBuilderFieldIdUtil {

  private AvroWriterBuilderFieldIdUtil() {
  }

  public static void beforeField(Deque<Integer> fieldIds, String name, Schema parentSchema) {
    fieldIds.push(AvroSchemaUtil.getFieldId(parentSchema.getField(name)));
  }

  public static void afterField(Deque<Integer> fieldIds) {
    fieldIds.pop();
  }

  public static void beforeListElement(Deque<Integer> fieldIds, Schema parentSchema) {
    fieldIds.push(AvroSchemaUtil.getElementId(parentSchema));
  }

  public static void beforeMapKey(Deque<Integer> fieldIds, String name, Schema parentSchema) {
    fieldIds.push(AvroSchemaUtil.getFieldId(parentSchema.getField(name)));
  }

  public static void beforeMapValue(Deque<Integer> fieldIds, String name, Schema parentSchema) {
    if (parentSchema.getType() == Schema.Type.MAP) {
      fieldIds.push(AvroSchemaUtil.getValueId(parentSchema));
    } else {
      // logical map
      fieldIds.push(AvroSchemaUtil.getFieldId(parentSchema.getField(name)));
    }
  }
}
