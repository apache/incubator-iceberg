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

import threading

from .binder import Binder
from .expressions import ExpressionVisitors


class Evaluator(object):

    def __init__(self, struct, unbound, case_sensitive=True):
        self.expr = Binder.bind(struct, unbound, case_sensitive)
        self.thread_local_data = threading.local()

    def _visitor(self):
        if not hasattr(self.thread_local_data, "visitors"):
            self.thread_local_data.visitors = Evaluator.EvalVisitor()

        return self.thread_local_data.visitors

    def eval(self, data):
        return self._visitor().eval(data, self.expr)

    class EvalVisitor(ExpressionVisitors.BoundExpressionVisitor):

        def __init__(self):
            super(Evaluator.EvalVisitor, self).__init__()
            self.struct = None

        def eval(self, row, expr):
            self.struct = row
            return ExpressionVisitors.visit(expr, self)

        def always_true(self):
            return True

        def always_false(self):
            return False

        def not_(self, result):
            return not result

        def and_(self, left_result, right_result):
            return left_result and right_result

        def or_(self, left_result, right_result):
            return left_result or right_result

        def is_null(self, ref):
            return ref.get(self.struct) is None

        def not_null(self, ref):
            return not (ref.get(self.struct) is None)

        def lt(self, ref, lit):
            return ref.get(self.struct) < lit.value

        def lt_eq(self, ref, lit):
            return ref.get(self.struct) <= lit.value

        def gt(self, ref, lit):
            return ref.get(self.struct) > lit.value

        def gt_eq(self, ref, lit):
            return ref.get(self.struct) >= lit.value

        def eq(self, ref, lit):
            return ref.get(self.struct) == lit.value

        def not_eq(self, ref, lit):
            return ref.get(self.struct) != lit.value

        def starts_with(self, ref, lit):
            return ref.get(self.struct).startswith(lit.value)

        def in_(self, ref, lit):
            raise NotImplementedError()

        def not_in(self, ref, lit):
            return not self.in_(ref, lit.value)
