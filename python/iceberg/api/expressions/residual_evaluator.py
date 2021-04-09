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

from .expression import Expression
from .expressions import Expressions, ExpressionVisitors
from .literals import Literal
from .predicate import BoundPredicate, Predicate, UnboundPredicate
from .reference import BoundReference
from ..partition_spec import PartitionSpec
from ..struct_like import StructLike


class ResidualEvaluator(object):
    """
    Finds the residuals for an {@link Expression} the partitions in the given {@link PartitionSpec}.

    A residual expression is made by partially evaluating an expression using partition values. For
    example, if a table is partitioned by day(utc_timestamp) and is read with a filter expression
    utc_timestamp &gt;= a and utc_timestamp &lt;= b, then there are 4 possible residuals expressions
    for the partition data, d:


        + If d > day(a) and d < day(b), the residual is always true
        + If d == day(a) and d != day(b), the residual is utc_timestamp >= b
        + if d == day(b) and d != day(a), the residual is utc_timestamp <= b
        + If d == day(a) == day(b), the residual is utc_timestamp >= a and utc_timestamp <= b

    Partition data is passed using StructLike. Residuals are returned by residualFor(StructLike).

    """
    @staticmethod
    def unpartitioned(expr: Expression) -> 'UnpartitionedEvaluator':
        return UnpartitionedEvaluator(expr)

    @staticmethod
    def of(spec: PartitionSpec, expr: Expression, case_sensitive=True) -> 'ResidualEvaluator':
        if len(spec.fields) > 0:
            return ResidualEvaluator(spec, expr, case_sensitive=case_sensitive)
        else:
            return ResidualEvaluator.unpartitioned(expr)

    def __init__(self, spec, expr, case_sensitive=True):
        self._spec = spec
        self._expr = expr
        self._case_sensitive = case_sensitive
        self.__visitor = None

    def _visitor(self) -> 'ResidualVisitor':
        if self.__visitor is None:
            self.__visitor = ResidualVisitor(self._spec,
                                             self._expr,
                                             self._case_sensitive)

        return self.__visitor

    def residual_for(self, partition_data: StructLike) -> Expression:
        """
        Returns a residual expression for the given partition values.

        Parameters
        ----------
        partition_data: StructLike
            partition data values

        Returns
        -------
        Expression
            the residual of this evaluator's expression from the partition values
        """
        return self._visitor().eval(partition_data)


class ResidualVisitor(ExpressionVisitors.BoundExpressionVisitor):

    def __init__(self, spec, expr, case_sensitive=True):
        self.struct = None
        self._spec = spec
        self._expr = expr
        self._case_sensitive = case_sensitive

    def eval(self, struct: StructLike) -> Expression:
        self.struct = struct
        return ExpressionVisitors.visit(self._expr, self)

    def always_true(self) -> Expression:
        return Expressions.always_true()

    def always_false(self) -> Expression:
        return Expressions.always_false()

    def is_null(self, ref: BoundReference) -> Expression:
        return self.always_true() if ref.get(self.struct) is None else self.always_false()

    def not_null(self, ref: BoundReference) -> Expression:
        return self.always_true() if ref.get(self.struct) is not None else self.always_false()

    def lt(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) < lit.value else self.always_false()

    def lt_eq(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) <= lit.value else self.always_false()

    def gt(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) > lit.value else self.always_false()

    def gt_eq(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) >= lit.value else self.always_false()

    def eq(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) == lit.value else self.always_false()

    def not_eq(self, ref: BoundReference, lit: Literal) -> Expression:
        return self.always_true() if ref.get(self.struct) != lit.value else self.always_false()

    def not_(self, result: Expression) -> Expression:
        return Expressions.not_(result)

    def and_(self, left_result: Expression, right_result: Expression) -> Expression:
        return Expressions.and_(left_result, right_result)

    def or_(self, left_result: Expression, right_result: Expression) -> Expression:
        return Expressions.or_(left_result, right_result)

    def predicate(self, pred: Predicate) -> Expression:
        if isinstance(pred, BoundPredicate):
            return self.bound_predicate(pred)
        elif isinstance(pred, UnboundPredicate):
            return self.unbound_predicate(pred)

        raise RuntimeError("Invalid predicate argument %s" % pred)

    def bound_predicate(self, pred: BoundPredicate) -> Expression:
        parts = self._spec.get_field_by_source_id(pred.ref.field_id)
        if parts is None:
            return pred

        strict_projections = [projection for projection in [part.transform.project_strict(part.name, pred)
                              for part in parts] if projection is not None]

        if len(strict_projections) == 0:
            # if there are no strict projections, the predicate must be in the residual
            return pred

        result = Expressions.always_false()
        for strict_projection in strict_projections:
            bound = strict_projection.bind(self._spec.partition_type())
            if isinstance(bound, BoundPredicate):
                result = Expressions.or_(result, super(ResidualVisitor, self).predicate(bound))
            else:
                result = Expressions.or_(result, bound)

        return result

    def unbound_predicate(self, pred: UnboundPredicate) -> Expression:
        bound = pred.bind(self._spec.schema.as_struct(), case_sensitive=self._case_sensitive)

        if isinstance(bound, BoundPredicate):
            bound_residual = self.predicate(bound)
            if isinstance(bound_residual, Predicate):
                return pred
            return bound_residual

        return bound


class UnpartitionedEvaluator(ResidualEvaluator):

    def __init__(self, expr):
        return super(UnpartitionedEvaluator, self).__init__(PartitionSpec.unpartitioned(),
                                                            expr, case_sensitive=False)
        self._expr = expr

    def residual_for(self, partition_data):
        return self._expr
