# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from iceberg.api import Schema
from iceberg.api.expressions import Expressions, ResidualEvaluator
from iceberg.core import PartitionData


def test_unpartitioned():
    row_filter = Expressions.equal("id", 1)
    part_data = PartitionData.from_json(Schema().as_struct(), {})
    evaluator = ResidualEvaluator.unpartitioned(row_filter)
    assert evaluator.residual_for(part_data) == Expressions.equal("id", 1)


def test_no_residual(residual_spec):
    row_filter = Expressions.equal("id", 1)
    part_data = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 1})
    evaluator = ResidualEvaluator.of(residual_spec, row_filter)
    assert evaluator.residual_for(part_data) == Expressions.always_true()


def test_residual(residual_spec):
    row_filter = Expressions.and_(Expressions.equal("id", 1),
                                  Expressions.equal("non_part_field", "test"))
    part_data = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 1})
    evaluator = ResidualEvaluator(residual_spec, row_filter)
    assert evaluator.residual_for(part_data) == Expressions.equal("non_part_field", "test")


def test_is_null_handling(residual_spec):
    row_filter = Expressions.and_(Expressions.is_null("all_nulls"), Expressions.equal("id", 1))

    part_data = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 1,
                                                                           "all_nulls": None,
                                                                           "some_nulls": "test",
                                                                           "no_nulls": "test"})
    evaluator = ResidualEvaluator(residual_spec, row_filter)
    assert evaluator.residual_for(part_data) == Expressions.always_true()


def test_is_not_null_handling(residual_spec):
    row_filter = Expressions.and_(Expressions.not_null("no_nulls"), Expressions.equal("id", 1))

    part_data = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 1,
                                                                           "all_nulls": None,
                                                                           "some_nulls": "test",
                                                                           "no_nulls": "test"})
    evaluator = ResidualEvaluator(residual_spec, row_filter)
    assert evaluator.residual_for(part_data) == Expressions.always_true()


def test_complex(residual_spec):
    row_filter = Expressions.or_(Expressions.and_(Expressions.equal("non_part_field", "a"),
                                                  Expressions.equal("id", 1)),
                                 Expressions.and_(Expressions.equal("non_part_field", "b"),
                                                  Expressions.not_equal("id", 1)))

    part_data_a = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 1, "no_nulls": "Test"})
    part_data_b = PartitionData.from_json(residual_spec.schema.as_struct(), {"id": 2, "no_nulls": "Test"})
    evaluator = ResidualEvaluator(residual_spec, row_filter)
    assert evaluator.residual_for(part_data_a) == Expressions.equal("non_part_field", "a")
    assert evaluator.residual_for(part_data_b) == Expressions.equal("non_part_field", "b")
