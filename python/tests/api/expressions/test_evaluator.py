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


import iceberg.api.expressions as exp
from iceberg.api.types import (FloatType,
                               IntegerType,
                               NestedField,
                               StringType,
                               StructType)
from iceberg.exceptions import ValidationException
from pytest import raises

STRUCT = StructType.of([NestedField.required(13, "x", IntegerType.get()),
                       NestedField.required(14, "y", IntegerType.get()),
                       NestedField.optional(15, "z", IntegerType.get())])

STRINGS_STRUCT = StructType.of([NestedField.required(13, "x", StringType.get()),
                                NestedField.required(14, "y", StringType.get()),
                                NestedField.optional(15, "z", StringType.get())])


def test_less_than(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.less_than("x", 7))
    assert not evaluator.eval(row_of((7, 8, None)))
    assert evaluator.eval(row_of((6, 8, None)))


def test_less_than_or_equal(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.less_than_or_equal("x", 7))
    assert evaluator.eval(row_of((7, 8, None)))
    assert evaluator.eval(row_of((6, 8, None)))
    assert not evaluator.eval(row_of((8, 8, None)))


def test_greater_than(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.greater_than("x", 7))
    assert not evaluator.eval(row_of((7, 8, None)))
    assert not evaluator.eval(row_of((6, 8, None)))
    assert evaluator.eval(row_of((8, 8, None)))


def test_greater_than_or_equal(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.greater_than_or_equal("x", 7))
    assert evaluator.eval(row_of((7, 8, None)))
    assert not evaluator.eval(row_of((6, 8, None)))
    assert evaluator.eval(row_of((8, 8, None)))


def test_equal(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.equal("x", 7))
    assert evaluator.eval(row_of((7, 8, None)))


def test_not_equal(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.not_equal("x", 7))
    assert not evaluator.eval(row_of((7, 8, None)))
    assert evaluator.eval(row_of((6, 8, None)))


def test_starts_with(row_of):
    evaluator = exp.evaluator.Evaluator(STRINGS_STRUCT,
                                        exp.expressions.Expressions.starts_with("x", "cheeseburger"))
    assert not evaluator.eval(row_of(("frenchfries", "cheese", None)))
    assert not evaluator.eval(row_of(("you should buy your friends cheeseburgers", "buttons", None)))
    assert evaluator.eval(row_of(("cheeseburgers are delicious with frenchfries.", "buttons", None)))


def test_startswith(row_of):
    evaluator = exp.evaluator.Evaluator(STRINGS_STRUCT,
                                        exp.expressions.Expressions.startswith("x", "cheeseburger"))
    assert not evaluator.eval(row_of(("frenchfries", "cheese", None)))
    assert not evaluator.eval(row_of(("you should buy your friends cheeseburgers", "buttons", None)))
    assert evaluator.eval(row_of(("cheeseburgers are delicious with frenchfries.", "buttons", None)))


def test_always_true(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.always_true())
    assert evaluator.eval(row_of(()))


def test_always_false(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.always_false())
    assert not evaluator.eval(row_of(()))


def test_is_null(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.is_null("z"))
    assert evaluator.eval(row_of((1, 2, None)))
    assert not evaluator.eval(row_of((1, 2, 3)))


def test_is_not_null(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.not_null("z"))
    assert not evaluator.eval(row_of((1, 2, None)))
    assert evaluator.eval(row_of((1, 2, 3)))


def test_and(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.and_(exp.expressions.Expressions.equal("x", 7),
                                                                         exp.expressions.Expressions.not_null("z")))
    assert evaluator.eval(row_of((7, 0, 3)))
    assert not evaluator.eval(row_of((8, 0, 3)))
    assert not evaluator.eval(row_of((7, 0, None)))
    assert not evaluator.eval(row_of((8, 0, None)))


def test_or(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.or_(exp.expressions.Expressions.equal("x", 7),
                                                                        exp.expressions.Expressions.not_null("z")))
    assert evaluator.eval(row_of((7, 0, 3)))
    assert evaluator.eval(row_of((8, 0, 3)))
    assert evaluator.eval(row_of((7, 0, None)))
    assert not evaluator.eval(row_of((8, 0, None)))


def test_not(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.not_(exp.expressions.Expressions.equal("x", 7)))
    assert not evaluator.eval(row_of((7,)))
    assert evaluator.eval(row_of((8,)))


def test_case_insensitive_not(row_of):
    evaluator = exp.evaluator.Evaluator(STRUCT,
                                        exp.expressions.Expressions.not_(exp.expressions.Expressions.equal("X", 7)),
                                        case_sensitive=False)
    assert not evaluator.eval(row_of((7,)))
    assert evaluator.eval(row_of((8,)))


def test_case_sensitive_not():
    with raises(ValidationException):
        exp.evaluator.Evaluator(STRUCT,
                                exp.expressions.Expressions.not_(exp.expressions.Expressions.equal("X", 7)),
                                case_sensitive=True)


def test_char_seq_value(row_of):
    struct = StructType.of([NestedField.required(34, "s", StringType.get())])
    evaluator = exp.evaluator.Evaluator(struct, exp.expressions.Expressions.equal("s", "abc"))
    assert evaluator.eval(row_of(("abc",)))
    assert not evaluator.eval(row_of(("abcd",)))


def test_nan_errors(row_of):
    # Placeholder until NaN support is fully implemented
    struct = StructType.of([NestedField.required(34, "f", FloatType.get())])
    evaluator = exp.evaluator.Evaluator(struct, exp.expressions.Expressions.is_nan("f"))
    with raises(NotImplementedError):
        evaluator.eval(row_of((123.4,)))

    evaluator = exp.evaluator.Evaluator(struct, exp.expressions.Expressions.not_nan("f"))
    with raises(NotImplementedError):
        evaluator.eval(row_of((123.4,)))
