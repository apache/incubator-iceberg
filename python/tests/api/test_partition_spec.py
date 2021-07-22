# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

from iceberg.api import PartitionSpec
from iceberg.api.schema import Schema
from iceberg.api.types import (BinaryType,
                               DateType,
                               DecimalType,
                               FixedType,
                               IntegerType,
                               LongType,
                               NestedField,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)
from tests.api.test_helpers import TestHelpers


class TestConversions(unittest.TestCase):

    def test_transforms(self):
        schema = Schema(NestedField.required(1, "i", IntegerType.get()),
                        NestedField.required(2, "l", LongType.get()),
                        NestedField.required(3, "d", DateType.get()),
                        NestedField.required(4, "t", TimeType.get()),
                        NestedField.required(5, "ts", TimestampType.without_timezone()),
                        NestedField.required(6, "dec", DecimalType.of(9, 2)),
                        NestedField.required(7, "s", StringType.get()),
                        NestedField.required(8, "u", UUIDType.get()),
                        NestedField.required(9, "f", FixedType.of_length(3)),
                        NestedField.required(10, "b", BinaryType.get()))
        specs = [PartitionSpec.builder_for(schema).identity("i").build(),
                 PartitionSpec.builder_for(schema).identity("l").build(),
                 PartitionSpec.builder_for(schema).identity("d").build(),
                 PartitionSpec.builder_for(schema).identity("t").build(),
                 PartitionSpec.builder_for(schema).identity("ts").build(),
                 PartitionSpec.builder_for(schema).identity("dec").build(),
                 PartitionSpec.builder_for(schema).identity("s").build(),
                 PartitionSpec.builder_for(schema).identity("u").build(),
                 PartitionSpec.builder_for(schema).identity("f").build(),
                 PartitionSpec.builder_for(schema).identity("b").build(),
                 PartitionSpec.builder_for(schema).bucket("i", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("l", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("d", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("t", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("ts", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("dec", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("s", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("u", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("f", 128).build(),
                 PartitionSpec.builder_for(schema).bucket("b", 128).build(),
                 PartitionSpec.builder_for(schema).year("d").build(),
                 PartitionSpec.builder_for(schema).month("d").build(),
                 PartitionSpec.builder_for(schema).day("d").build(),
                 PartitionSpec.builder_for(schema).year("ts").build(),
                 PartitionSpec.builder_for(schema).month("ts").build(),
                 PartitionSpec.builder_for(schema).day("ts").build(),
                 PartitionSpec.builder_for(schema).hour("ts").build(),
                 PartitionSpec.builder_for(schema).truncate("i", 10).build(),
                 PartitionSpec.builder_for(schema).truncate("l", 10).build(),
                 PartitionSpec.builder_for(schema).truncate("dec", 10).build(),
                 PartitionSpec.builder_for(schema).truncate("s", 10).build(),
                 PartitionSpec.builder_for(schema).add_without_field_id(6, "dec_unsupported", "unsupported").build(),
                 PartitionSpec.builder_for(schema).add(6, 1111, "dec_unsupported", "unsupported").build(),
                 ]

        for spec in specs:
            self.assertEqual(spec, TestHelpers.round_trip_serialize(spec))
