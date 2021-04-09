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

from typing import TYPE_CHECKING

from .expressions import Expressions, ExpressionVisitors, RewriteNot
from .predicate import BoundPredicate, UnboundPredicate

if TYPE_CHECKING:
    from .expression import Expression
    from .predicate import Predicate
    from ..partition_spec import PartitionSpec


def inclusive(spec: 'PartitionSpec', case_sensitive: bool = True):
    """
    Creates an inclusive ProjectionEvaluator for the PartitionSpec, defaulting
    to case sensitive mode.

    An evaluator is used to project expressions for a table's data rows into expressions on the
    table's partition values. The evaluator returned by this function is inclusive and will build
    expressions with the following guarantee: if the original expression matches a row, then the
    projected expression will match that row's partition.

    Each predicate in the expression is projected using Transform.project(String, BoundPredicate).

    Parameters
    ----------
    spec: PartitionSpec
       a partition spec
    case_sensitive: boolean
       whether the Projection should consider case sensitivity on column names or not.

    Returns
    -------
    InclusiveProjection
        an inclusive projection evaluator for the partition spec. Inclusive transform used for each predicate
    """
    return InclusiveProjection(spec, case_sensitive)


def strict(spec: 'PartitionSpec'):
    """
    Creates a strict ProjectionEvaluatorfor the PartitionSpec, defaulting to case sensitive mode.

    An evaluator is used to project expressions for a table's data rows into expressions on the
    table's partition values. The evaluator returned by this function is strict and will build
    expressions with the following guarantee: if the projected expression matches a partition,
    then the original expression will match all rows in that partition.

    Each predicate in the expression is projected using Transform.projectStrict(String, BoundPredicate).

    Parameters
    ----------
    spec: PartitionSpec
       a partition spec

    Returns
    -------
    StrictProjection
        a strict projection evaluator for the partition spec
    """
    return StrictProjection(spec)


class ProjectionEvaluator(ExpressionVisitors.ExpressionVisitor):

    def project(self, expr: 'Expression'):
        raise NotImplementedError()


class BaseProjectionEvaluator(ProjectionEvaluator):

    def __init__(self, spec: 'PartitionSpec', case_sensitive: bool = True):
        self.spec = spec
        self.case_sensitive = case_sensitive

    def project(self, expr: 'Expression') -> 'Expression':
        # projections assume that there are no NOT nodes in the expression tree. to ensure that this
        # is the case, the expression is rewritten to push all NOT nodes down to the expression
        # leaf nodes.
        # this is necessary to ensure that the default expression returned when a predicate can't be
        # projected is correct.
        #
        return ExpressionVisitors.visit(ExpressionVisitors.visit(expr, RewriteNot.get()), self)

    def always_true(self) -> 'Expression':
        return Expressions.always_true()

    def always_false(self) -> 'Expression':
        return Expressions.always_false()

    def not_(self, result) -> 'Expression':
        raise RuntimeError("[BUG] project called on expression with a not")

    def and_(self, left_result: 'Expression', right_result: 'Expression') -> 'Expression':
        return Expressions.and_(left_result, right_result)

    def or_(self, left_result: 'Expression', right_result: 'Expression') -> 'Expression':
        return Expressions.or_(left_result, right_result)

    def predicate(self, pred: 'Predicate') -> 'Expression':
        if isinstance(pred, UnboundPredicate):
            bound = pred.bind(self.spec.schema.as_struct(), case_sensitive=self.case_sensitive)
        else:
            raise NotImplementedError("No Base implementation for BoundPredicate")

        if isinstance(bound, BoundPredicate):
            return self.predicate(bound)

        return bound


class InclusiveProjection(BaseProjectionEvaluator):

    def __init__(self, spec: 'PartitionSpec', case_sensitive: bool = True):
        super(InclusiveProjection, self).__init__(spec,
                                                  case_sensitive=case_sensitive)

    def predicate(self, pred: 'Predicate') -> 'Expression':
        if isinstance(pred, UnboundPredicate):
            return super(InclusiveProjection, self).predicate(pred)
        parts = self.spec.get_field_by_source_id(pred.ref.field_id)

        if parts is None:
            return self.always_true()

        projections = [part.transform.project(part.name, pred) for part in parts]

        result = Expressions.always_true()
        for projection in [inclusive_projection for inclusive_projection in projections
                           if inclusive_projection is not None]:
            result = Expressions.and_(result, projection)

        return result


class StrictProjection(BaseProjectionEvaluator):

    def __init__(self, spec: 'PartitionSpec'):
        super(StrictProjection, self).__init__(spec)

    def predicate(self, pred: 'Predicate') -> 'Expression':
        if isinstance(pred, UnboundPredicate):
            return super(StrictProjection, self).predicate(pred)

        parts = self.spec.get_field_by_source_id(pred.ref.field_id)

        if parts is None:
            return self.always_false()

        projections = [part.transform.project_strict(part.name, pred) for part in parts]

        result = Expressions.always_true()
        for projection in [strict_projection for strict_projection in projections
                           if strict_projection is not None]:
            result = Expressions.or_(result, projection)

        return result
