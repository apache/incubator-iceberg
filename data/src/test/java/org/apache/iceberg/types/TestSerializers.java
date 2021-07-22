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

import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSerializers {

  private static final Types.StructType REQUIRED_PRIMITIVES = Types.StructType.of(
      required(100, "id", Types.LongType.get()),
      required(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      required(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      required(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      required(107, "date", Types.DateType.get()),
      required(108, "ts_tz", Types.TimestampType.withZone()),
      required(109, "ts", Types.TimestampType.withoutZone()),
      required(110, "s", Types.StringType.get()),
      required(112, "fixed", Types.FixedType.ofLength(7)),
      required(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
      required(117, "time", Types.TimeType.get())
  );

  private static final Types.StructType OPTIONAL_PRIMITIVES = Types.StructType.of(
      optional(200, "_id", Types.LongType.get()),
      optional(201, "_data", Types.StringType.get()),
      optional(202, "_b", Types.BooleanType.get()),
      optional(203, "_i", Types.IntegerType.get()),
      optional(204, "_l", Types.LongType.get()),
      optional(205, "_f", Types.FloatType.get()),
      optional(206, "_d", Types.DoubleType.get()),
      optional(207, "_date", Types.DateType.get()),
      optional(208, "_ts_tz", Types.TimestampType.withZone()),
      optional(209, "_ts", Types.TimestampType.withoutZone()),
      optional(210, "_s", Types.StringType.get()),
      optional(212, "_fixed", Types.FixedType.ofLength(7)),
      optional(213, "_bytes", Types.BinaryType.get()),
      optional(214, "_dec_9_0", Types.DecimalType.of(9, 0)),
      optional(215, "_dec_11_2", Types.DecimalType.of(11, 2)),
      optional(216, "_dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
      optional(217, "_time", Types.TimeType.get())
  );

  @Test
  public void testRequiredPrimitives() {
    generateAndValidate(new Schema(REQUIRED_PRIMITIVES.fields()));
  }

  @Test
  public void testOptionalPrimitives() {
    generateAndValidate(new Schema(OPTIONAL_PRIMITIVES.fields()));
  }

  @Test
  public void testListType() {
    Types.StructType structType = Types.StructType.of(
        required(0, "id", Types.LongType.get()),
        optional(1, "optional_list", Types.ListType.ofOptional(
            2, Types.ListType.ofRequired(
                3, Types.LongType.get()
            )
        )),
        required(6, "required_list", Types.ListType.ofRequired(
            7, Types.ListType.ofOptional(
                8, Types.LongType.get()
            )
        ))
    );

    generateAndValidate(new Schema(structType.fields()));
  }

  @Test
  @Ignore("The InternalRecordWrapper does not support nested ListType.")
  public void testNestedList() {
    Types.StructType structType = Types.StructType.of(
        optional(1, "list_struct", Types.ListType.ofOptional(
            2, Types.ListType.ofRequired(
                3, Types.StructType.of(REQUIRED_PRIMITIVES.fields())
            )
        )),
        optional(4, "struct_list", Types.StructType.of(
            Types.NestedField.required(5, "inner_list",
                Types.ListType.ofOptional(
                    6, Types.StructType.of(OPTIONAL_PRIMITIVES.fields())
                ))
        ))
    );

    generateAndValidate(new Schema(structType.fields()));
  }

  @Test
  public void testNestedSchema() {
    Types.StructType structType = Types.StructType.of(
        required(0, "id", Types.LongType.get()),
        required(1, "level1", Types.StructType.of(
            optional(2, "level2", Types.StructType.of(
                required(3, "level3", Types.StructType.of(
                    optional(4, "level4", Types.StructType.of(
                        required(5, "level5", Types.StructType.of(
                            REQUIRED_PRIMITIVES.fields()
                        ))
                    ))
                ))
            ))
        ))
    );

    generateAndValidate(new Schema(structType.fields()));
  }

  private void generateAndValidate(Schema schema) {
    List<Record> records = RandomGenericData.generate(schema, 100, 1_000_000_1);

    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(schema.asStruct());
    Serializer<StructLike> serializer = Serializers.forType(schema.asStruct());
    Comparator<StructLike> comparator = Comparators.forType(schema.asStruct());

    for (Record expectedRecord : records) {
      StructLike expectedStructLike = recordWrapper.wrap(expectedRecord);

      byte[] expectedData = serializer.serialize(expectedStructLike);
      StructLike actualStructLike = serializer.deserialize(expectedData);

      Assert.assertEquals("Should produce the equivalent StructLike", 0,
          comparator.compare(expectedStructLike, actualStructLike));

      byte[] actualData = serializer.serialize(actualStructLike);
      Assert.assertArrayEquals("Should have the expected serialized data", expectedData, actualData);
    }
  }
}
