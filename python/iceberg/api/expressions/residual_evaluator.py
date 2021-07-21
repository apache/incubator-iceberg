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

from .expressions import Expressions, ExpressionVisitors
from .predicate import BoundPredicate, Predicate, UnboundPredicate


class ResidualEvaluator(object):

    def __init__(self, spec, expr):
        self._spec = spec
        self._expr = expr
        self.__visitor = None

    def _visitor(self):
        if self.__visitor is None:
            self.__visitor = ResidualVisitor()

        return self.__visitor

    def residual_for(self, partition_data):
        return self._visitor().eval(partition_data)


class ResidualVisitor(ExpressionVisitors.BoundExpressionVisitor):

    def __init__(self):
        self.struct = None

    def eval(self, struct):
        self.struct = struct

    def always_true(self):
        return Expressions.always_true()

    def always_false(self):
        return Expressions.always_false()

    def is_null(self, ref):
        return self.always_true() if ref.get(self.struct) is None else self.always_false()

    def not_null(self, ref):
        return self.always_true() if ref.get(self.struct) is not None else self.always_false()

    def lt(self, ref, lit):
        return self.always_true() if ref.get(self.struct) < lit.value else self.always_false()

    def lt_eq(self, ref, lit):
        return self.always_true() if ref.get(self.struct) <= lit.value else self.always_false()

    def gt(self, ref, lit):
        return self.always_true() if ref.get(self.struct) > lit.value else self.always_false()

    def gt_eq(self, ref, lit):
        return self.always_true() if ref.get(self.struct) >= lit.value else self.always_false()

    def eq(self, ref, lit):
        return self.always_true() if ref.get(self.struct) == lit.value else self.always_false()

    def not_eq(self, ref, lit):
        return self.always_true() if ref.get(self.struct) != lit.value else self.always_false()

    def starts_with(self, ref, lit):
        return self.always_true() if ref.get(self.struct).startsWith(lit.value) else self.always_false()

    def not_(self, result):
        return Expressions.not_(result)

    def and_(self, left_result, right_result):
        return Expressions.and_(left_result, right_result)

    def or_(self, left_result, right_result):
        return Expressions.or_(left_result, right_result)

    def predicate(self, pred):
        if isinstance(pred, BoundPredicate):
            return self.bound_predicate(pred)
        elif isinstance(pred, UnboundPredicate):
            return self.unbound_predicate(pred)

        raise RuntimeError("Invalid predicate argument %s" % pred)

    def bound_predicate(self, pred):
        part = self.spec.get_field_by_source_id(pred.ref.field_id)
        if part is None:
            return pred

        strict_projection = part.transform.project_strict(part.name, pred)
        if strict_projection is None:
            bound = strict_projection.bind(self.spec.partition_type())
            if isinstance(bound, BoundPredicate):
                return super(ResidualVisitor, self).predicate(bound)
            return bound

        return pred

    def unbound_predicate(self, pred):
        bound = pred.bind(self.spec.schema.as_struct())

        if isinstance(bound, BoundPredicate):
            bound_residual = self.predicate(bound)
            if isinstance(bound_residual, Predicate):
                return pred
            return bound_residual

        return bound
