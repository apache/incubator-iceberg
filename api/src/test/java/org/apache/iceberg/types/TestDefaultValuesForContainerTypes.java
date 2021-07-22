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

package org.apache.iceberg.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField;
import static org.apache.iceberg.types.Types.StructType;

public class TestDefaultValuesForContainerTypes {

  static NestedField intFieldType;
  static NestedField stringFieldType;
  static StructType structType;

  @BeforeClass
  public static void beforeClass() {
    intFieldType = NestedField.optional(0, "optionalIntField", Types.IntegerType.get());
    stringFieldType = NestedField.required(1, "requiredStringField", Types.StringType.get());
    structType = StructType.of(Arrays.asList(intFieldType, stringFieldType));
  }

  @Test
  public void testStructTypeDefault() {
    List<Object> structDefaultvalue = new ArrayList<>();
    structDefaultvalue.add(Integer.valueOf(1));
    structDefaultvalue.add("two");
    NestedField structField = NestedField.optional(2, "optionalStructField", structType, structDefaultvalue, "doc");
    Assert.assertTrue(structField.hasDefaultValue());
    Assert.assertEquals(structDefaultvalue, structField.getDefaultValue());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStructTypeDefaultInvalidFieldsTypes() {
    List<Object> structDefaultvalue = new ArrayList<>();
    structDefaultvalue.add("one");
    structDefaultvalue.add("two");
    NestedField.optional(2, "optionalStructField", structType, structDefaultvalue, "doc");
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStructTypeDefaultInvalidNumberFields() {
    List<Object> structDefaultvalue = new ArrayList<>();
    structDefaultvalue.add(Integer.valueOf(1));
    structDefaultvalue.add("two");
    structDefaultvalue.add("three");
    NestedField.optional(2, "optionalStructField", structType, structDefaultvalue, "doc");
  }
}
