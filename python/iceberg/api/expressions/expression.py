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

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .predicate import Predicate


class Expression(object):

    left: 'Predicate'
    right: 'Predicate'
    child: 'Predicate'

    def __init__(self):
        pass

    def op(self):
        raise RuntimeError("No implementation for base class")

    def negate(self):
        raise RuntimeError("%s cannot be negated" % self)


class Operation(Enum):
    TRUE = "TRUE"
    FALSE = "FALSE"
    IS_NULL = "IS_NULL"
    NOT_NULL = "NOT_NULL"
    IS_NAN = "IS_NAN"
    NOT_NAN = "NOT_NAN"
    LT = "LT"
    LT_EQ = "LT_EQ"
    GT = "GT"
    GT_EQ = "GT_EQ"
    EQ = "EQ"
    NOT_EQ = "NOT_EQ"
    IN = "IN"
    NOT_IN = "NOT_IN"
    NOT = "NOT"
    AND = "AND"
    OR = "OR"
    STARTS_WITH = "STARTS_WITH"

    def negate(self): # noqa
        if self == Operation.IS_NULL:
            return Operation.NOT_NULL
        elif self == Operation.NOT_NULL:
            return Operation.IS_NULL
        elif self == Operation.IS_NAN:
            return Operation.NOT_NAN
        elif self == Operation.NOT_NAN:
            return Operation.IS_NAN
        elif self == Operation.LT:
            return Operation.GT_EQ
        elif self == Operation.LT_EQ:
            return Operation.GT
        elif self == Operation.GT:
            return Operation.LT_EQ
        elif self == Operation.GT_EQ:
            return Operation.LT
        elif self == Operation.EQ:
            return Operation.NOT_EQ
        elif self == Operation.NOT_EQ:
            return Operation.EQ
        elif self == Operation.IN:
            return Operation.NOT_IN
        elif self == Operation.NOT_IN:
            return Operation.IN
        else:
            raise RuntimeError("No negation for operation: %s" % self)

    def flipLR(self):
        if self == Operation.LT:
            return Operation.GT
        elif self == Operation.LT_EQ:
            return Operation.GT_EQ
        elif self == Operation.GT:
            return Operation.LT
        elif self == Operation.GT_EQ:
            return Operation.LT_EQ
        elif self == Operation.EQ:
            return Operation.EQ
        elif self == Operation.NOT_EQ:
            return Operation.NOT_EQ
        elif self == Operation.AND:
            return Operation.AND
        elif self == Operation.OR:
            return Operation.OR
        else:
            raise RuntimeError("No left-right flip for operation: %s" % self)

    def op(self):
        pass


class And(Expression):

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, And):
            return False

        return self.left == other.left and self.right == other.right

    def __ne__(self, other):
        return not self.__eq__(other)

    def op(self):
        return Operation.AND

    def negate(self):
        from .expressions import Expressions
        return Expressions.or_(self.left.negate(), self.right.negate()) # noqa

    def __repr__(self):
        return "And({},{})".format(self.left, self.right)

    def __str__(self):
        return '({} and {})'.format(self.left, self.right)


class FalseExp(Expression):

    def op(self):
        return Operation.FALSE

    def negate(self):
        return TRUE

    def __repr__(self):
        return "false"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if isinstance(other, FalseExp):
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class Or(Expression):

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, Or):
            return False

        return self.left == other.left and self.right == other.right

    def __ne__(self, other):
        return not self.__eq__(other)

    def op(self):
        return Operation.OR

    def negate(self):
        from .expressions import Expressions
        return Expressions.and_(self.left.negate(), self.right.negate()) # noqa

    def __repr__(self):
        return "Or({},{})".format(self.left, self.right)

    def __str__(self):
        return '({} or {})'.format(self.left, self.right)


class Not(Expression):

    def __init__(self, child):
        self.child = child

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, Not):
            return False

        return self.child == other.child

    def __ne__(self, other):
        return not self.__eq__(other)

    def op(self):
        return Operation.NOT

    def negate(self):
        return self.child

    def __repr__(self):
        return "Not({})".format(self.child)

    def __str__(self):
        return 'not({})'.format(self.child)


class TrueExp(Expression):

    def op(self):
        return Operation.TRUE

    def negate(self):
        return False

    def __repr__(self):
        return "true"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if isinstance(other, TrueExp):
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)


TRUE = TrueExp()
FALSE = FalseExp()
