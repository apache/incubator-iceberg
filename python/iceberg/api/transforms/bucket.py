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

import math
import struct
import sys

import mmh3

from .transform import Transform
from .transform_util import TransformUtil
from ..expressions import (Expressions,
                           JAVA_MAX_INT,
                           Operation)
from ..types.types import (IntegerType,
                           TypeID)
from ...api.types.conversions import Conversions


class Bucket(Transform):
    MURMUR3 = mmh3

    BUCKET_TYPE = {TypeID.DATE: lambda n: BucketInteger(n),
                   TypeID.INTEGER: lambda n: BucketInteger(n),
                   TypeID.TIME: lambda n: BucketLong(n),
                   TypeID.TIMESTAMP: lambda n: BucketLong(n),
                   TypeID.LONG: lambda n: BucketLong(n),
                   TypeID.DECIMAL: lambda n: BucketDecimal(n),
                   TypeID.STRING: lambda n: BucketString(n),
                   TypeID.FIXED: lambda n: BucketByteBuffer(n),
                   TypeID.BINARY: lambda n: BucketByteBuffer(n),
                   TypeID.UUID: lambda n: BucketUUID(n)}

    @staticmethod
    def get(type_var, n):
        bucket_type_func = Bucket.BUCKET_TYPE.get(type_var.type_id)
        if not bucket_type_func:
            raise RuntimeError("Cannot bucket by type: %s" % type_var)
        return bucket_type_func(n)

    def __init__(self, n):
        self.n = n

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, Bucket):
            return False

        return self.n == other.n

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return Bucket.__class__, self.n

    def __repr__(self):
        return "Bucket[%s]" % self.n

    def __str__(self):
        return "bucket[%s]" % self.n

    def apply(self, value):
        return (self.hash(value) & JAVA_MAX_INT) % self.n

    def hash(self):
        raise NotImplementedError()

    def project(self, name, predicate):
        if predicate.op == Operation.EQ:
            return Expressions.predicate(predicate.op, name, self.apply(predicate.lit.value))

    def project_strict(self, name, predicate):
        if predicate.op == Operation.NOT_EQ:
            return Expressions.predicate(predicate.op, name, self.apply(predicate.lit.value))

    def get_result_type(self, source_type):
        return IntegerType.get()


class BucketInteger(Bucket):

    def __init__(self, n):
        super(BucketInteger, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(struct.pack("q", value))

    def can_transform(self, type_var):
        return type_var.type_id in [TypeID.INTEGER, TypeID.DATE]


class BucketLong(Bucket):
    def __init__(self, n):
        super(BucketLong, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(struct.pack("q", value))

    def can_transform(self, type_var):
        return type_var.type_id in [TypeID.LONG,
                                    TypeID.TIME,
                                    TypeID.TIMESTAMP]


class BucketFloat(Bucket):
    def __init__(self, n):
        super(BucketFloat, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(struct.pack("d", value))

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.FLOAT


class BucketDouble(Bucket):
    def __init__(self, n):
        super(BucketDouble, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(struct.pack("d", value))

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.DOUBLE


class BucketDecimal(Bucket):

    def __init__(self, n):
        super(BucketDecimal, self).__init__(n)

    def hash(self, value):
        # to-do: unwrap to_bytes func since python2 support is being removed
        unscaled_value = TransformUtil.unscale_decimal(value)
        number_of_bytes = int(math.ceil(unscaled_value.bit_length() / 8))
        return Bucket.MURMUR3.hash(to_bytes(unscaled_value, number_of_bytes, byteorder='big'))

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.DECIMAL


class BucketString(Bucket):
    def __init__(self, n):
        super(BucketString, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(value)

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.STRING


class BucketByteBuffer(Bucket):
    def __init__(self, n):
        super(BucketByteBuffer, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(value)

    def can_transform(self, type_var):
        return type_var.type_id in [TypeID.BINARY, TypeID.FIXED]


class BucketUUID(Bucket):
    def __init__(self, n):
        super(BucketUUID, self).__init__(n)

    def hash(self, value):
        return Bucket.MURMUR3.hash(Conversions.to_byte_buffer(TypeID.UUID, value))

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.UUID


def to_bytes(n, length, byteorder='big'):
    if sys.version_info >= (3, 0):
        return n.to_bytes(length, byteorder=byteorder)
    h = '%x' % n
    s = ('0' * (len(h) % 2) + h).zfill(length * 2).decode('hex')
    return s if byteorder == 'big' else s[::-1]
