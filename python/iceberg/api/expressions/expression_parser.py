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

# Derived from the SimpleSQL Parser example in pyparsing, retrofitted to just handle the
# where clause predicates
# https://github.com/pyparsing/pyparsing/blob/master/examples/simpleSQL.py

import logging

from pyparsing import (
    alphanums,
    alphas,
    CaselessKeyword,
    delimitedList,
    Group,
    infixNotation,
    oneOf,
    opAssoc,
    pyparsing_common as ppc,
    quotedString,
    Word
)


_logger = logging.getLogger(__name__)

AND, OR, IN, IS, NOT, NULL, BETWEEN = map(
    CaselessKeyword, "and or in is not null between".split()
)
NOT_NULL = NOT + NULL

ident = Word(alphas, alphanums + "_$").setName("identifier")
columnName = delimitedList(ident, ".", combine=True).setName("column name")

binop = oneOf("= == != < > >= <= eq ne lt le gt ge <> startsWith", caseless=False)
realNum = ppc.real()
intNum = ppc.signed_integer()

columnRval = (realNum
              | intNum
              | quotedString
              | columnName)  # need to add support for alg expressions
whereCondition = Group(
    (columnName + binop + columnRval)
    | (columnName + IN + Group("(" + delimitedList(columnRval) + ")"))
    | (columnName + IS + (NULL | NOT_NULL))
    | (columnName + BETWEEN + columnRval + AND + columnRval)

)

whereExpression = infixNotation(
    Group(whereCondition
          | NOT + whereCondition
          | NOT + Group('(' + whereCondition + ')')
          | NOT + columnName),
    [(NOT, 1, opAssoc.LEFT), (AND, 2, opAssoc.LEFT), (OR, 2, opAssoc.LEFT), (IS, 2, opAssoc.LEFT)],
)

op_map = {"=": "eq",
          "==": "eq",
          "eq": "eq",
          ">": "gt",
          "gt": "gt",
          ">=": "gte",
          "gte": "gte",
          "<": "lt",
          "lt": "lt",
          "<=": "lte",
          "lte": "lte",
          "!": "not",
          "not": "not",
          "!=": "neq",
          "<>": "neq",
          "neq": "neq",
          "||": "or",
          "or": "or",
          "&&": "and",
          "and": "and",
          "in": "in",
          "between": "between",
          "is": "is",
          "startsWith": "startsWith"}


def get_expr_tree(tokens):
    if isinstance(tokens, (str, int)):
        return tokens
    if len(tokens) > 1:
        if (tokens[0] == "not"):
            return {"not": get_expr_tree(tokens[1])}
        if (tokens[0] == "(" and tokens[-1] == ")"):
            return get_expr_tree(tokens[1:-1])
    else:
        return get_expr_tree(tokens[0])

    op = op_map[tokens[1]]

    if op == "in":
        return {'in': [get_expr_tree(tokens[0]), [token for token in tokens[2][1:-1]]]}
    elif op == "between":
        return {'and': [{"gte": [get_expr_tree(tokens[0]), tokens[2]]},
                        {"lte": [get_expr_tree(tokens[0]), tokens[4]]}]}
    elif op == "is":

        if tokens[2] == 'null':
            return {"missing": tokens[0]}
        else:
            return {"exists": tokens[0]}
    if len(tokens) > 3:
        binary_tuples = get_expr_tree(tokens[2:])
    else:
        binary_tuples = get_expr_tree(tokens[2])

    return {op: [get_expr_tree(tokens[0]),
                 binary_tuples]}


def get_expr(node, expr_map):
    if isinstance(node, dict):
        for i in node.keys():
            op = i
        if op == "literal":
            return node["literal"]
        mapped_op = expr_map.get(op, expr_map)
        if len(mapped_op) == 1:
            mapped_op = mapped_op[0]
        if mapped_op is None:
            raise RuntimeError("no mapping for op: %s" % op)
        if op in ("not", "exists", "missing"):
            return mapped_op(get_expr(node[op], expr_map))

        return mapped_op(*get_expr(node[op], expr_map))
    elif isinstance(node, (list, tuple)):
        return (get_expr(item, expr_map) for item in node)
    elif isinstance(node, (str, int, float)):
        return node
    else:
        raise RuntimeError("unknown node type" % node)


def parse_expr_string(predicate_string, expr_map):
    from pyparsing import ParseException

    try:
        expr = whereExpression.parseString(predicate_string, parseAll=True)
        expr = get_expr_tree(expr)
        return get_expr(expr, expr_map)
    except ParseException as pe:
        _logger.error("Error parsing string expression into iceberg expression: %s" % str(pe))
        raise
