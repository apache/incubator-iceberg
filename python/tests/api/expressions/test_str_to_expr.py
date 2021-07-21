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

from iceberg.api.expressions import Expressions


def test_equal():
    expected_expr = Expressions.equal("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a=1")
    assert expected_expr == conv_expr


def test_equal_alt_syntax():
    expected_expr = Expressions.equal("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a==1")
    assert expected_expr == conv_expr


def test_gt():
    expected_expr = Expressions.greater_than("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a > 1")
    assert expected_expr == conv_expr


def test_gte():
    expected_expr = Expressions.greater_than_or_equal("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a >= 1")
    assert expected_expr == conv_expr


def test_lt():
    expected_expr = Expressions.less_than("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a < 1")
    assert expected_expr == conv_expr


def test_lte():
    expected_expr = Expressions.less_than_or_equal("col_a", 1)
    conv_expr = Expressions.convert_string_to_expr("col_a <= 1")
    assert expected_expr == conv_expr


def test_and():
    expected_expr = Expressions.and_(Expressions.equal("col_a", 1), Expressions.equal("col_b", 2))
    conv_expr = Expressions.convert_string_to_expr("col_a=1 and col_b=2")
    assert expected_expr == conv_expr


def test_or():
    expected_expr = Expressions.or_(Expressions.equal("col_a", 1), Expressions.equal("col_b", 2))
    conv_expr = Expressions.convert_string_to_expr("col_a=1 or col_b=2")
    assert expected_expr == conv_expr


def test_between():
    expected_expr = Expressions.and_(Expressions.greater_than_or_equal("col_a", 1),
                                     Expressions.less_than_or_equal("col_a", 2))
    conv_expr = Expressions.convert_string_to_expr("col_a between 1 and 2")
    assert expected_expr == conv_expr


def test_is_null():
    expected_expr = Expressions.is_null("col_a")
    conv_expr = Expressions.convert_string_to_expr("col_a is null")
    assert expected_expr == conv_expr


def test_not_null():
    expected_expr = Expressions.not_null("col_a")
    conv_expr = Expressions.convert_string_to_expr("col_a is not null")
    assert expected_expr == conv_expr


def test_not():
    expected_expr = Expressions.not_("col_a")
    conv_expr = Expressions.convert_string_to_expr("not col_a")
    assert expected_expr == conv_expr


def test_not_equal():
    expected_expr = Expressions.not_equal("col_a", 7)
    conv_expr = Expressions.convert_string_to_expr("col_a <> 7")
    assert expected_expr == conv_expr


def test_starts_with():
    expected_expr = Expressions.starts_with("col_a", "cheeseburgers")
    conv_expr = Expressions.convert_string_to_expr("col_a startsWith cheeseburgers")
    assert expected_expr == conv_expr


def test_not_equal_alt_syntax():
    expected_expr = Expressions.not_equal("col_a", 7)
    conv_expr = Expressions.convert_string_to_expr("col_a != 7")
    assert expected_expr == conv_expr


def test_compound_not_equal():
    expected_expr = Expressions.not_(Expressions.equal("col_a", 7))
    conv_expr = Expressions.convert_string_to_expr("not (col_a = 7)")
    assert expected_expr == conv_expr


def test_ternary_condition():
    expected_expr = Expressions.and_(Expressions.equal("col_a", 1),
                                     Expressions.and_(Expressions.equal("col_b", 2),
                                                      Expressions.equal("col_c", 3)))

    conv_expr = Expressions.convert_string_to_expr("col_a=1 and col_b=2 and col_c=3")
    assert expected_expr == conv_expr


def test_precedence():
    expected_expr = Expressions.or_(Expressions.equal("col_a", 1),
                                    Expressions.and_(Expressions.equal("col_b", 2),
                                                     Expressions.equal("col_c", 3)))

    conv_expr = Expressions.convert_string_to_expr("col_a=1 or col_b=2 and col_c=3")
    assert expected_expr == conv_expr


def test_precedence_opposite_order():
    expected_expr = Expressions.or_(Expressions.and_(Expressions.equal("col_a", 1),
                                                     Expressions.equal("col_b", 2)),
                                    Expressions.equal("col_c", 3))

    conv_expr = Expressions.convert_string_to_expr("col_a=1 and col_b=2 or col_c=3")
    assert expected_expr == conv_expr


def test_precedence_explicit():
    expected_expr = Expressions.and_(Expressions.equal("col_a", 1),
                                     Expressions.or_(Expressions.equal("col_b", 2),
                                                     Expressions.equal("col_c", 3)))

    conv_expr = Expressions.convert_string_to_expr("col_a=1 and (col_b=2 or col_c=3)")
    assert expected_expr == conv_expr


def test_precedence_with_between():
    expected_expr = Expressions.or_(Expressions.and_(Expressions.greater_than_or_equal("col_a", 1),
                                                     Expressions.less_than_or_equal("col_a", 2)),
                                    Expressions.equal("col_c", 3))

    conv_expr = Expressions.convert_string_to_expr("col_a between 1 and 2 or col_c=3")
    assert expected_expr == conv_expr


def test_complex_expansion():
    expected_expr = Expressions.or_(Expressions.and_(Expressions.equal("a", 1),
                                                     Expressions.and_(Expressions.equal("b", 2),
                                                                      Expressions.not_equal("c", 3))),
                                    Expressions.is_null("d"))
    conv_expr = Expressions.convert_string_to_expr("(a=1 and b=2 and c<>3) or d is null")
    assert expected_expr == conv_expr
