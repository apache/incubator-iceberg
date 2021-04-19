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

import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class TestNestedFieldDefaultValues {

  private final int id = 1;
  private final String fieldName = "fieldName";
  private final Type fieldType = Types.IntegerType.get();
  private final String doc = "field doc";
  private final Integer defaultValue = 100;

  @Test
  public void testConstructorsValidCases() {
    // optional constructors
    Assert.assertFalse(optional(id, fieldName, fieldType).hasDefaultValue());
    Assert.assertFalse(optional(id, fieldName, fieldType, doc).hasDefaultValue());
    NestedField nestedFieldWithDefault = optional(id, fieldName, fieldType, defaultValue, doc);
    Assert.assertTrue(nestedFieldWithDefault.hasDefaultValue());
    Assert.assertEquals(defaultValue, nestedFieldWithDefault.getDefaultValue());
    nestedFieldWithDefault = optional(id, fieldName, fieldType, defaultValue, null);
    Assert.assertTrue(nestedFieldWithDefault.hasDefaultValue());
    Assert.assertEquals(defaultValue, nestedFieldWithDefault.getDefaultValue());

    // required constructors
    Assert.assertFalse(required(id, fieldName, fieldType).hasDefaultValue());
    Assert.assertFalse(required(id, fieldName, fieldType, doc).hasDefaultValue());
    nestedFieldWithDefault = required(id, fieldName, fieldType, defaultValue, doc);
    Assert.assertTrue(nestedFieldWithDefault.hasDefaultValue());
    Assert.assertEquals(defaultValue, nestedFieldWithDefault.getDefaultValue());
    nestedFieldWithDefault = required(id, fieldName, fieldType, defaultValue, null);
    Assert.assertTrue(nestedFieldWithDefault.hasDefaultValue());
    Assert.assertEquals(defaultValue, nestedFieldWithDefault.getDefaultValue());

    // of constructors
    Assert.assertFalse(NestedField.of(id, true, fieldName, fieldType).hasDefaultValue());
    Assert.assertFalse(NestedField.of(id, true, fieldName, fieldType, doc).hasDefaultValue());
    nestedFieldWithDefault = NestedField.of(id, true, fieldName, fieldType, defaultValue, doc);
    Assert.assertTrue(nestedFieldWithDefault.hasDefaultValue());
    Assert.assertEquals(defaultValue, nestedFieldWithDefault.getDefaultValue());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testRequiredWithDefaultNullDefault() {
    // illegal case (required with null defaultValue)
    required(id, fieldName, fieldType, null, doc);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testOptionalWithInvalidDefaultValueClass() {
    // class of default value does not match class of type
    Long wrongClassDefaultValue = 100L;
    optional(id, fieldName, fieldType, wrongClassDefaultValue, doc);
  }
}