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

import logging

from .expression import (And,
                         FALSE,
                         Not,
                         Operation,
                         Or,
                         TRUE)
from .expression_parser import parse_expr_string
from .predicate import (Predicate,
                        UnboundPredicate)
from .reference import NamedReference

_logger = logging.getLogger(__name__)


class Expressions(object):

    @staticmethod
    def and_(left, right):
        if left == Expressions.always_false() or right == Expressions.always_false():
            return Expressions.always_false()
        elif left == Expressions.always_true():
            return right
        elif right == Expressions.always_true():
            return left

        return And(left, right)

    @staticmethod
    def or_(left, right):
        if left == Expressions.always_true() or right == Expressions.always_true():
            return Expressions.always_true()
        elif left == Expressions.always_false():
            return right
        elif right == Expressions.always_false():
            return left

        return Or(left, right)

    @staticmethod
    def not_(child):
        if child == Expressions.always_true():
            return Expressions.always_false()
        elif child == Expressions.always_false():
            return Expressions.always_true()
        elif isinstance(child, Not):
            return child.child

        return Not(child)

    @staticmethod
    def is_null(name):
        return UnboundPredicate(Operation.IS_NULL, Expressions.ref(name))

    @staticmethod
    def not_null(name):
        return UnboundPredicate(Operation.NOT_NULL, Expressions.ref(name))

    @staticmethod
    def is_nan(name):
        return UnboundPredicate(Operation.IS_NAN, Expressions.ref(name))

    @staticmethod
    def not_nan(name):
        return UnboundPredicate(Operation.NOT_NAN, Expressions.ref(name))

    @staticmethod
    def less_than(name, value):
        return UnboundPredicate(Operation.LT, Expressions.ref(name), value)

    @staticmethod
    def less_than_or_equal(name, value):
        return UnboundPredicate(Operation.LT_EQ, Expressions.ref(name), value)

    @staticmethod
    def greater_than(name, value):
        return UnboundPredicate(Operation.GT, Expressions.ref(name), value)

    @staticmethod
    def greater_than_or_equal(name, value):
        return UnboundPredicate(Operation.GT_EQ, Expressions.ref(name), value)

    @staticmethod
    def equal(name, value):
        return UnboundPredicate(Operation.EQ, Expressions.ref(name), value)

    @staticmethod
    def not_equal(name, value):
        return UnboundPredicate(Operation.NOT_EQ, Expressions.ref(name), value)

    # startswith matches the Python API folks are used to using on strings.
    @staticmethod
    def startswith(name, value):
        return UnboundPredicate(Operation.STARTS_WITH, Expressions.ref(name), value)

    # starts_with matches the camelCase to snake_case conversion used elsewhere.
    @staticmethod
    def starts_with(name, value):
        return UnboundPredicate(Operation.STARTS_WITH, Expressions.ref(name), value)

    @staticmethod
    def predicate(op, name, value=None, lit=None):
        if value is not None and op not in (Operation.IS_NULL, Operation.NOT_NULL):
            return UnboundPredicate(op, Expressions.ref(name), value)
        elif lit is not None and op not in (Operation.IS_NULL, Operation.NOT_NULL):
            return UnboundPredicate(op, Expressions.ref(name), value)
        elif op in (Operation.IS_NULL, Operation.NOT_NULL):
            if value is not None or lit is not None:
                raise RuntimeError("Cannot create {} predicate inclusive a value".format(op))
            return UnboundPredicate(op, Expressions.ref(name))
        else:
            raise RuntimeError("Cannot create {} predicate without a value".format(op))

    @staticmethod
    def always_true():
        return TRUE

    @staticmethod
    def always_false():
        return FALSE

    @staticmethod
    def rewrite_not(expr):
        return ExpressionVisitors.visit(expr, RewriteNot.get()) # noqa

    @staticmethod
    def ref(name):
        return NamedReference(name)

    @staticmethod
    def convert_string_to_expr(predicate_string):
        expr_map = {"and": (Expressions.and_,),
                    "eq": (Expressions.equal,),
                    "exists": (Expressions.not_null,),
                    "gt": (Expressions.greater_than,),
                    "gte": (Expressions.greater_than_or_equal,),
                    "lt": (Expressions.less_than,),
                    "lte": (Expressions.less_than_or_equal,),
                    "missing": (Expressions.is_null,),
                    "neq": (Expressions.not_equal,),
                    "not": (Expressions.not_,),
                    "or": (Expressions.or_,),
                    "startsWith": (Expressions.starts_with,)}

        return parse_expr_string(predicate_string, expr_map)


class ExpressionVisitors(object):

    @staticmethod
    def visit(expr, visitor):
        if isinstance(expr, Predicate):
            return visitor.predicate(expr)

        if expr.op() == Operation.TRUE:
            return visitor.always_true()
        elif expr.op() == Operation.FALSE:
            return visitor.always_false()
        elif expr.op() == Operation.NOT:
            return visitor.not_(ExpressionVisitors.visit(expr.child, visitor))
        elif expr.op() == Operation.AND:
            return visitor.and_(ExpressionVisitors.visit(expr.left, visitor),
                                ExpressionVisitors.visit(expr.right, visitor))
        elif expr.op() == Operation.OR:
            return visitor.or_(ExpressionVisitors.visit(expr.left, visitor),
                               ExpressionVisitors.visit(expr.right, visitor))
        else:
            raise RuntimeError("Unknown operation: {}".format(expr.op()))

    class ExpressionVisitor(object):

        def always_true(self):
            return NotImplementedError()

        def always_false(self):
            return NotImplementedError()

        def not_(self, result):
            return NotImplementedError()

        def and_(self, left_result, right_result):
            return NotImplementedError()

        def or_(self, left_result, right_result):
            return NotImplementedError()

        def predicate(self, pred):
            return NotImplementedError()

    class BoundExpressionVisitor(ExpressionVisitor):

        def __init__(self):
            super(ExpressionVisitors.BoundExpressionVisitor, self).__init__()

        def is_null(self, ref):
            return NotImplementedError()

        def not_null(self, ref):
            return NotImplementedError()

        def is_nan(self, ref):
            return NotImplementedError()

        def not_nan(self, ref):
            return NotImplementedError()

        def lt(self, ref, lit):
            return NotImplementedError()

        def lt_eq(self, ref, lit):
            return NotImplementedError()

        def gt(self, ref, lit):
            return NotImplementedError()

        def gt_eq(self, ref, lit):
            return None

        def eq(self, ref, lit):
            return None

        def not_eq(self, ref, lit):
            return None

        def starts_with(self, ref, lit):
            return None

        def in_(self, ref, lit):
            return None

        def not_in(self, ref, lit):
            return None

        def predicate(self, pred): # noqa

            if isinstance(pred, UnboundPredicate):
                raise RuntimeError("Not a bound Predicate: {}".format(pred))

            if pred.op == Operation.IS_NULL:
                return self.is_null(pred.ref)
            elif pred.op == Operation.NOT_NULL:
                return self.not_null(pred.ref)
            elif pred.op in [Operation.IS_NAN, Operation.NOT_NAN]:
                raise NotImplementedError("IS_NAN and NOT_NAN not fully implemented for expressions")
            elif pred.op == Operation.LT:
                return self.lt(pred.ref, pred.lit)
            elif pred.op == Operation.LT_EQ:
                return self.lt_eq(pred.ref, pred.lit)
            elif pred.op == Operation.GT:
                return self.gt(pred.ref, pred.lit)
            elif pred.op == Operation.GT_EQ:
                return self.gt_eq(pred.ref, pred.lit)
            elif pred.op == Operation.EQ:
                return self.eq(pred.ref, pred.lit)
            elif pred.op == Operation.NOT_EQ:
                return self.not_eq(pred.ref, pred.lit)
            elif pred.op == Operation.IN:
                return self.in_(pred.ref, pred.lit)
            elif pred.op == Operation.NOT_IN:
                return self.not_in(pred.ref, pred.lit)
            elif pred.op == Operation.STARTS_WITH:
                return self.starts_with(pred.ref, pred.lit)
            else:
                raise RuntimeError("Unknown operation for Predicate: {}".format(pred.op))


class RewriteNot(ExpressionVisitors.ExpressionVisitor):
    __instance = None

    @staticmethod
    def get():
        if RewriteNot.__instance is None:
            RewriteNot()
        return RewriteNot.__instance

    def __init__(self):
        if RewriteNot.__instance is not None:
            raise Exception("Multiple RewriteNot Types created")
        RewriteNot.__instance = self

    def always_true(self):
        return Expressions.always_true()

    def always_false(self):
        return Expressions.always_false()

    def not_(self, result):
        return result.negate()

    def and_(self, left_result, right_result):
        return Expressions.and_(left_result, right_result)

    def or_(self, left_result, right_result):
        return Expressions.or_(left_result, right_result)

    def predicate(self, pred):
        return pred
