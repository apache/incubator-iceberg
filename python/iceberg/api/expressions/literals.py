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

import datetime
from decimal import (Decimal,
                     ROUND_HALF_UP)
from typing import Any
import uuid

import pytz


from .expression import (FALSE,
                         TRUE)
from .java_variables import (JAVA_MAX_FLOAT,
                             JAVA_MIN_FLOAT)
from ..types.type import TypeID


class Literals(object):

    EPOCH = datetime.datetime.utcfromtimestamp(0)
    EPOCH_DAY = EPOCH.date()

    @staticmethod
    def from_(value):  # noqa: C901
        if value is None:
            raise RuntimeError("Cannot create an expression literal from None")
        if isinstance(value, bool):
            return BooleanLiteral(value)
        elif isinstance(value, int):
            if Literal.JAVA_MIN_INT < value < Literal.JAVA_MAX_INT:
                return IntegerLiteral(value)
            return LongLiteral(value)
        elif isinstance(value, float):
            if Literal.JAVA_MIN_FLOAT < value < Literal.JAVA_MAX_FLOAT:
                return FloatLiteral(value)
            return DoubleLiteral(value)
        elif isinstance(value, str):
            return StringLiteral(value)
        elif isinstance(value, uuid.UUID):
            return UUIDLiteral(value)
        elif isinstance(value, bytearray):
            return BinaryLiteral(value)
        elif isinstance(value, bytes):
            return FixedLiteral(value)
        elif isinstance(value, Decimal):
            return DecimalLiteral(value)
        else:
            raise RuntimeError("Unimplemented Type Literal")

    @staticmethod
    def above_max():
        return ABOVE_MAX

    @staticmethod
    def below_min():
        return BELOW_MIN


class Literal(object):
    value: Any
    JAVA_MAX_INT = 2147483647
    JAVA_MIN_INT = -2147483648
    JAVA_MAX_FLOAT = 3.4028235E38
    JAVA_MIN_FLOAT = -3.4028235E38

    @staticmethod
    def of(value):  # noqa: C901

        if isinstance(value, bool):
            return BooleanLiteral(value)
        elif isinstance(value, int):
            if value < Literal.JAVA_MIN_INT or value > Literal.JAVA_MAX_INT:
                return LongLiteral(value)
            return IntegerLiteral(value)
        elif isinstance(value, float):
            if value < Literal.JAVA_MIN_FLOAT or value > Literal.JAVA_MAX_FLOAT:
                return DoubleLiteral(value)
            return FloatLiteral(value)
        elif isinstance(value, str):
            return StringLiteral(value)
        elif isinstance(value, uuid.UUID):
            return UUIDLiteral(value)
        elif isinstance(value, bytes):
            return FixedLiteral(value)
        elif isinstance(value, bytearray):
            return BinaryLiteral(value)
        elif isinstance(value, Decimal):
            return DecimalLiteral(value)

    def to(self, type):
        raise NotImplementedError()


class BaseLiteral(Literal):
    def __init__(self, value):
        self.value = value

    def to(self, type):
        raise NotImplementedError()

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, BaseLiteral):
            return False

        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "BaseLiteral(%s)" % str(self.value)

    def __str__(self):
        return str(self.value)


class ComparableLiteral(BaseLiteral):

    def __init__(self, value):
        super(ComparableLiteral, self).__init__(value)

    def to(self, type):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if self.value is None:
            return True

        if other is None or other.value is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if self.value is None:
            return False

        if other is None or other.value is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if self.value is None:
            return True

        if other is None or other.value is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if self.value is None:
            return False

        if other is None or other.value is None:
            return True

        return self.value >= other.value


class AboveMax(Literal):
    def __init__(self):
        super(AboveMax, self).__init__()

    def value(self):
        raise RuntimeError("AboveMax has no value")

    def to(self, type):
        raise RuntimeError("Cannot change the type of AboveMax")

    def __str__(self):
        return "aboveMax"


class BelowMin(Literal):
    def __init__(self):
        super(BelowMin, self).__init__()

    def value(self):
        raise RuntimeError("BelowMin has no value")

    def to(self, type):
        raise RuntimeError("Cannot change the type of BelowMin")

    def __str__(self):
        return "belowMin"


class BooleanLiteral(ComparableLiteral):

    def __init__(self, value):
        super(BooleanLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.BOOLEAN:
            return self


class IntegerLiteral(ComparableLiteral):

    def __init__(self, value):
        super(IntegerLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.INTEGER:
            return self
        elif type_var.type_id == TypeID.LONG:
            return LongLiteral(self.value)
        elif type_var.type_id == TypeID.FLOAT:
            return FloatLiteral(float(self.value))
        elif type_var.type_id == TypeID.DOUBLE:
            return DoubleLiteral(float(self.value))
        elif type_var.type_id == TypeID.DATE:
            return DateLiteral(self.value)
        elif type_var.type_id == TypeID.DECIMAL:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(self.value))
            else:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                                rounding=ROUND_HALF_UP))


class LongLiteral(ComparableLiteral):

    def __init__(self, value):
        super(LongLiteral, self).__init__(value)

    def to(self, type_var):  # noqa: C901
        if type_var.type_id == TypeID.INTEGER:
            if Literal.JAVA_MAX_INT < self.value:
                return ABOVE_MAX
            elif Literal.JAVA_MIN_INT > self.value:
                return BELOW_MIN

            return IntegerLiteral(self.value)
        elif type_var.type_id == TypeID.LONG:
            return self
        elif type_var.type_id == TypeID.FLOAT:
            return FloatLiteral(float(self.value))
        elif type_var.type_id == TypeID.DOUBLE:
            return DoubleLiteral(float(self.value))
        elif type_var.type_id == TypeID.TIME:
            return TimeLiteral(self.value)
        elif type_var.type_id == TypeID.TIMESTAMP:
            return TimestampLiteral(self.value)
        elif type_var.type_id == TypeID.DECIMAL:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(self.value))
            else:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                                rounding=ROUND_HALF_UP))


class FloatLiteral(ComparableLiteral):

    def __init__(self, value):
        super(FloatLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.FLOAT:
            return self
        elif type_var.type_id == TypeID.DOUBLE:
            return DoubleLiteral(self.value)
        elif type_var.type_id == TypeID.DECIMAL:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal('1.'),
                                                rounding=ROUND_HALF_UP))
            else:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                                rounding=ROUND_HALF_UP))


class DoubleLiteral(ComparableLiteral):

    def __init__(self, value):
        super(DoubleLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.FLOAT:
            if JAVA_MAX_FLOAT < self.value:
                return ABOVE_MAX
            elif JAVA_MIN_FLOAT > self.value:
                return BELOW_MIN

            return FloatLiteral(self.value)
        elif type_var.type_id == TypeID.DOUBLE:
            return self
        elif type_var.type_id == TypeID.DECIMAL:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal('1.'),
                                                rounding=ROUND_HALF_UP))
            else:
                return DecimalLiteral(Decimal(self.value)
                                      .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                                rounding=ROUND_HALF_UP))


class DateLiteral(ComparableLiteral):

    def __init__(self, value):
        super(DateLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.DATE:
            return self


class TimeLiteral(ComparableLiteral):

    def __init__(self, value):
        super(TimeLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.TIME:
            return self


class TimestampLiteral(ComparableLiteral):

    def __init__(self, value):
        super(TimestampLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.TIMESTAMP:
            return self
        elif type_var.type_id == TypeID.DATE:
            return DateLiteral((datetime.datetime.fromtimestamp(self.value / 1000000) - Literals.EPOCH).days)


class DecimalLiteral(ComparableLiteral):

    def __init__(self, value):
        super(DecimalLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.DECIMAL and type_var.scale == abs(self.value.as_tuple().exponent):
            return self


class StringLiteral(BaseLiteral):
    def __init__(self, value):
        super(StringLiteral, self).__init__(value)

    def to(self, type_var):  # noqa: C901
        import dateutil.parser
        if type_var.type_id == TypeID.DATE:
            return DateLiteral((dateutil.parser.parse(self.value) - Literals.EPOCH).days)
        elif type_var.type_id == TypeID.TIME:
            return TimeLiteral(
                int((dateutil.parser.parse(Literals.EPOCH.strftime("%Y-%m-%d ") + self.value) - Literals.EPOCH)
                    .total_seconds() * 1000000))
        elif type_var.type_id == TypeID.TIMESTAMP:
            timestamp = dateutil.parser.parse(self.value)
            EPOCH = Literals.EPOCH
            if bool(timestamp.tzinfo) != bool(type_var.adjust_to_utc):
                raise RuntimeError("Cannot convert to %s when string is: %s" % (type_var, self.value))

            if timestamp.tzinfo is not None:
                EPOCH = EPOCH.replace(tzinfo=pytz.UTC)

            return TimestampLiteral(int((timestamp - EPOCH).total_seconds() * 1000000))
        elif type_var.type_id == TypeID.STRING:
            return self
        elif type_var.type_id == TypeID.UUID:
            return UUIDLiteral(uuid.UUID(self.value))
        elif type_var.type_id == TypeID.DECIMAL:
            dec_val = Decimal(str(self.value))
            if abs(dec_val.as_tuple().exponent) == type_var.scale:
                if type_var.scale == 0:
                    return DecimalLiteral(Decimal(str(self.value))
                                          .quantize(Decimal('1.'),
                                                    rounding=ROUND_HALF_UP))
                else:
                    return DecimalLiteral(Decimal(str(self.value))
                                          .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                                    rounding=ROUND_HALF_UP))

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, StringLiteral):
            return False

        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if other is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if other is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if other is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if other is None:
            return True

        return self.value >= other.value

    def __str__(self):
        return '"' + self.value + '"'


class UUIDLiteral(ComparableLiteral):
    def __init__(self, value):
        super(UUIDLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.UUID:
            return self


class FixedLiteral(BaseLiteral):
    def __init__(self, value):
        super(FixedLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.FIXED:
            if len(self.value) == type_var.length:
                return self
        elif type_var.type_id == TypeID.BINARY:
            return BinaryLiteral(self.value)

    def write_replace(self):
        return FixedLiteralProxy(self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if other is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if other is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if other is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if other is None:
            return True

        return self.value >= other.value


class BinaryLiteral(BaseLiteral):
    def __init__(self, value):
        super(BinaryLiteral, self).__init__(value)

    def to(self, type_var):
        if type_var.type_id == TypeID.FIXED:
            if type_var.length == len(self.value):
                return FixedLiteral(self.value)
            return None
        elif type_var.type_id == TypeID.BINARY:
            return self

    def write_replace(self):
        return BinaryLiteralProxy(self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if other is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if other is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if other is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if other is None:
            return True

        return self.value >= other.value


class FixedLiteralProxy(object):

    def __init__(self, buffer=None):
        if buffer is not None:
            self.bytes = list(buffer)

    def read_resolve(self):
        return FixedLiteral(self.bytes)


class ConstantExpressionProxy(object):

    def __init__(self, true_or_false=None):
        if true_or_false is not None:
            self.true_or_false = true_or_false

    def read_resolve(self):
        if self.true_or_false:
            return TRUE
        else:
            return FALSE


class BinaryLiteralProxy(FixedLiteralProxy):

    def __init__(self, buffer=None):
        super(BinaryLiteralProxy, self).__init__(buffer)

    def read_resolve(self):
        return BinaryLiteral(self.bytes)


ABOVE_MAX = AboveMax()
BELOW_MIN = BelowMin()
