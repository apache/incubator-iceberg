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

from collections import defaultdict
from urllib.parse import quote_plus

from iceberg.exceptions import ValidationException

from .partition_field import PartitionField
from .schema import Schema
from .transforms import Transforms
from .types import (NestedField,
                    StructType)


class PartitionSpec(object):

    PARTITION_DATA_ID_START = 1000

    @staticmethod
    def UNPARTITIONED_SPEC():
        return PartitionSpec(Schema(), 0, [], PartitionSpec.PARTITION_DATA_ID_START - 1)

    @staticmethod
    def unpartitioned():
        return PartitionSpec.UNPARTITIONED_SPEC()

    def __init__(self, schema, spec_id, fields, last_assigned_field_id):
        self.fields_by_source_id = None
        self.fields_by_name = None
        self.__java_classes = None
        self.field_list = None

        self.schema = schema
        self.spec_id = spec_id
        self.__fields = list()
        for field in fields:
            self.__fields.append(field)
        self.last_assigned_field_id = last_assigned_field_id

    @property
    def fields(self):
        return self.lazy_field_list()

    @property
    def java_classes(self):
        if self.__java_classes is None:
            self.__java_classes
        for field in self.__fields:
            source_type = self.schema.find_type(field.source_id)
            result = field.transform().get_result_by_type(source_type)
            self.__java_classes.append(result.type_id.java_class())

        return self.__java_classes

    def get_field_by_source_id(self, field_id):
        return self.lazy_fields_by_source_id().get(field_id)

    def partition_type(self):
        struct_fields = list()
        for _i, field in enumerate(self.__fields):
            source_type = self.schema.find_type(field.source_id)
            result_type = field.transform.get_result_type(source_type)
            struct_fields.append(NestedField.optional(field.field_id,
                                                      field.name,
                                                      result_type))

        return StructType.of(struct_fields)

    def get(self, data, pos, java_class):
        data.get(pos, java_class)

    def escape(self, string):
        return quote_plus(string, encoding="UTF-8")

    def partition_to_path(self, data):
        sb = list()
        java_classes = self.java_classes
        for i, jclass in enumerate(java_classes):
            field = self.__fields[i]
            value_string = field.transform().to_human_string(self.get(data, i, jclass))

            if i > 0:
                sb.append("/")
            sb.append(field.name)
            sb.append("=")
            sb.append(self.escape(value_string))

        return "".join(sb)

    def compatible_with(self, other):
        if self.__eq__(other):
            return True

        if len(self.__fields) != len(other.__fields):
            return False

        for i, field in enumerate(self.__fields):
            that_field = other.__fields[i]
            if field.source_id != that_field.source_id or str(field.transform) != str(that_field.transform):
                return False

        return True

    def lazy_fields_by_source_id(self):
        if self.fields_by_source_id is None:
            self.fields_by_source_id = defaultdict(list)
            for field in self.fields:
                self.fields_by_source_id[field.source_id].append(field)

        return self.fields_by_source_id

    def identity_source_ids(self):
        source_ids = set()
        fields = self.fields
        for field in fields:
            if "identity" == str(field.transform()):
                source_ids.add(field)

        return source_ids

    def lazy_field_list(self):
        if self.field_list is None:
            self.field_list = list(self.__fields)

        return self.field_list

    def lazy_fields_by_source_name(self):
        if self.fields_by_name is None:
            self.fields_by_name = dict()
            for field in self.__fields:
                self.fields_by_name[field.name] = field

        return self.fields_by_name

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, PartitionSpec):
            return False

        return self.__fields == other.__fields

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return PartitionSpec.__class__, tuple(self.fields)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        sb = ["["]

        for field in self.__fields:
            sb.append("\n {field_id}: {name}: {transform}({source_id})".format(field_id=field.field_id,
                                                                               name=field.name,
                                                                               transform=str(field.transform),
                                                                               source_id=field.source_id))

        if len(self.__fields) > 0:
            sb.append("\n")
        sb.append("]")

        return "".join(sb)

    @staticmethod
    def builder_for(schema: Schema) -> "PartitionSpecBuilder":
        return PartitionSpecBuilder(schema)

    @staticmethod
    def check_compatibility(spec, schema):
        for field in spec.fields:
            src_type = schema.find_type(field.source_id)
            if not src_type.is_primitive_type():
                raise ValidationException("Cannot partition by non-primitive source field: %s", src_type)
            if not field.transform.can_transform(src_type):
                raise ValidationException("Invalid source type %s for transform: %s", (src_type, field.transform))


class PartitionSpecBuilder(object):

    def __init__(self, schema):
        self.schema = schema
        self.fields = list()
        self.partition_names = set()
        self.spec_id = 0
        self.last_assigned_field_id = PartitionSpec.PARTITION_DATA_ID_START - 1

    def __next_field_id(self):
        self.last_assigned_field_id = self.last_assigned_field_id + 1
        return self.last_assigned_field_id

    def with_spec_id(self, spec_id):
        self.spec_id = spec_id
        return self

    def check_and_add_partition_name(self, name):
        if name is None or name == "":
            raise RuntimeError("Cannot use empty or null partition name")
        if name in self.partition_names:
            raise RuntimeError("Cannot use partition names more than once: %s" % name)

        self.partition_names.add(name)
        return self

    def find_source_column(self, source_name):
        source_column = self.schema.find_field(source_name)
        if source_column is None:
            raise RuntimeError("Cannot find source column: %s" % source_name)

        return source_column

    def identity(self, source_name):
        self.check_and_add_partition_name(source_name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          source_name,
                                          Transforms.identity(source_column.type)))
        return self

    def year(self, source_name):
        name = "{}_year".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.year(source_column.type)))
        return self

    def month(self, source_name):
        name = "{}_month".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.month(source_column.type)))
        return self

    def day(self, source_name):
        name = "{}_day".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.day(source_column.type)))
        return self

    def hour(self, source_name):
        name = "{}_hour".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.hour(source_column.type)))
        return self

    def bucket(self, source_name, num_buckets):
        name = "{}_bucket".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.bucket(source_column.type, num_buckets)))
        return self

    def truncate(self, source_name, width):
        name = "{}_truncate".format(source_name)
        self.check_and_add_partition_name(name)
        source_column = self.find_source_column(source_name)
        self.fields.append(PartitionField(source_column.field_id,
                                          self.__next_field_id(),
                                          name,
                                          Transforms.truncate(source_column.type, width)))
        return self

    def add_without_field_id(self, source_id, name, transform):
        return self.add(source_id, self.__next_field_id(), name, transform)

    def add(self, source_id: int, field_id: int, name: str, transform: str) -> "PartitionSpecBuilder":
        self.check_and_add_partition_name(name)
        column = self.schema.find_field(source_id)
        if column is None:
            raise RuntimeError("Cannot find source column: %s" % source_id)

        transform_obj = Transforms.from_string(column.type, transform)
        field = PartitionField(source_id,
                               field_id,
                               name,
                               transform_obj)
        self.fields.append(field)
        self.last_assigned_field_id = max(self.last_assigned_field_id, field_id)
        return self

    def build(self):
        spec = PartitionSpec(self.schema, self.spec_id, self.fields, self.last_assigned_field_id)
        PartitionSpec.check_compatibility(spec, self.schema)
        return spec
