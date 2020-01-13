# sqlalchemy_teiid/__init__.py
# Copyright (C) 2019 Kolmar Kafran
# Copyright (C) 2017 Kairui Song
#
# This project is a fork from https://github.com/ryncsn/sqlalchemy-teiid
"""
A simple and hacky SQLAlchemy dialect for Teiid.

For more information on SQL Support please read
https://teiid.gitbooks.io/documents/content/reference/SQL_Support.html
"""

from __future__ import print_function

import json
import logging
import re

from sqlalchemy import sql, util
from sqlalchemy.dialects.postgresql.psycopg2 import (
    PGCompiler_psycopg2,
    PGDialect_psycopg2,
)
from sqlalchemy.engine import reflection
from sqlalchemy.sql import elements, sqltypes
from sqlalchemy.types import (
    BIGINT,
    BLOB,
    BOOLEAN,
    CHAR,
    CLOB,
    DATE,
    FLOAT,
    INTEGER,
    NUMERIC,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
)

from . import system_tables as systables

logger = logging.getLogger("teiid")

ischema_names = {
    "string": VARCHAR,
    "varchar": VARCHAR,
    "varbinary": VARBINARY,
    "char": CHAR,
    "boolean": SMALLINT,
    "byte": SMALLINT,
    "tinyint": SMALLINT,
    "short": SMALLINT,
    "smallint": SMALLINT,
    "integer": INTEGER,
    "serial": INTEGER,
    "long": BIGINT,
    "bigint": BIGINT,
    "biginteger": NUMERIC,
    "float": FLOAT,
    "real": REAL,
    "double": FLOAT,
    "bigdecimal": NUMERIC,
    "decimal": NUMERIC,
    "date": DATE,
    "time": TIME,
    "timestamp": TIMESTAMP,
    "blob": BLOB,
    "clob": CLOB,
}

RESERVED_WORDS = set(
    [
        "add",
        "all",
        "alter",
        "and",
        "any",
        "array_agg",
        "as",
        "asc",
        "atomic",
        "begin",
        "between",
        "bigdecimal",
        "bigint",
        "biginteger",
        "blob",
        "boolean",
        "both",
        "break",
        "by",
        "byte",
        "call",
        "case",
        "cast",
        "char",
        "clob",
        "column",
        "commit",
        "constraint",
        "continue",
        "convert",
        "create",
        "cross",
        "date",
        "day",
        "decimal",
        "declare",
        "default",
        "delete",
        "desc",
        "distinct",
        "double",
        "drop",
        "each",
        "else",
        "end",
        "error",
        "escape",
        "except",
        "exec",
        "execute",
        "exists",
        "false",
        "fetch",
        "filter",
        "float",
        "for",
        "foreign",
        "from",
        "full",
        "function",
        "geometry",
        "global",
        "group",
        "having",
        "hour",
        "if",
        "immediate",
        "in",
        "inner",
        "inout",
        "insert",
        "integer",
        "intersect",
        "into",
        "is",
        "join",
        "language",
        "lateral",
        "leading",
        "leave",
        "left",
        "like",
        "like_regex",
        "limit",
        "local",
        "long",
        "loop",
        "makedep",
        "makeind",
        "makenotdep",
        "merge",
        "minute",
        "month",
        "no",
        "nocache",
        "not",
        "null",
        "object",
        "of",
        "offset",
        "on",
        "only",
        "option",
        "options",
        "or",
        "order",
        "out",
        "outer",
        "over",
        "parameter",
        "partition",
        "primary",
        "procedure",
        "real",
        "references",
        "return",
        "returns",
        "right",
        "rollup",
        "row",
        "rows",
        "second",
        "select",
        "set",
        "short",
        "similar",
        "smallint",
        "some",
        "sqlexception",
        "sqlstate",
        "sqlwarning",
        "string",
        "table",
        "temporary",
        "then",
        "time",
        "timestamp",
        "tinyint",
        "to",
        "trailing",
        "translate",
        "trigger",
        "true",
        "union",
        "unique",
        "unknown",
        "update",
        "user",
        "using",
        "values",
        "varbinary",
        "varchar",
        "virtual",
        "when",
        "where",
        "while",
        "with",
        "without",
        "xml",
        "xmlagg",
        "xmlattributes",
        "xmlcast",
        "xmlcomment",
        "xmlconcat",
        "xmlelement",
        "xmlexists",
        "xmlforest",
        "xmlnamespaces",
        "xmlparse",
        "xmlpi",
        "xmlquery",
        "xmlserialize",
        "xmltable",
        "xmltext",
        "year",
    ]
)


class TeiidCompiler(PGCompiler_psycopg2):
    def visit_column(
        self, column, add_to_result_map=None, include_table=True, **kwargs
    ):
        name = orig_name = column.name
        if name is None:
            name = self._fallback_column_name(column)

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(name, orig_name, (column, name, column.key), column.type)

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name)

        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return name
        else:
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)

            return self.preparer.quote(tablename + "." + name)

    def _truncated_identifier(self, ident_class, name):
        if (ident_class, name) in self.truncated_names:
            return self.truncated_names[(ident_class, name)]

        anonname = name.apply_map(self.anon_map).replace(".", "_")

        if len(anonname) > self.label_length - 6:
            counter = self.truncated_names.get(ident_class, 1)
            truncname = (
                anonname[0 : max(self.label_length - 6, 0)] + "_" + hex(counter)[2:]
            )
            self.truncated_names[ident_class] = counter + 1
        else:
            truncname = anonname
        self.truncated_names[(ident_class, name)] = truncname
        return truncname


class TeiidDialect(PGDialect_psycopg2):
    statement_compiler = TeiidCompiler

    ischema_names = ischema_names

    def __init__(self, *args, **kwargs):
        PGDialect_psycopg2.__init__(self, *args, **kwargs)
        self.supports_isolation_level = False

    def get_isolation_level(self, connection):
        return "READ COMMITTED"

    def _get_server_version_info(self, connection):
        """
        Returns the PostgresSQL 8.2 as TEIID's version.
        """
        return (8, 2)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = sql.select(
            [systables.schemas.c.schema_name],
            order_by=[systables.schemas.c.schema_name],
        )
        schema_names = [r[0] for r in connection.execute(s)]
        return schema_names

    @reflection.cache
    def get_table_names(self, connection, schema, **kw):
        tables = systables.tables
        s = sql.select(
            [tables.c.table_name],
            sql.and_(tables.c.table_schema == schema, tables.c.table_type == "Table",),
            order_by=[tables.c.table_name],
        )
        table_names = [r[0] for r in connection.execute(s)]
        return table_names

    @reflection.cache
    def get_view_names(self, connection, schema, **kw):
        tables = systables.tables
        s = sql.select(
            [tables.c.table_name],
            sql.and_(tables.c.table_schema == schema, tables.c.table_type == "View"),
            order_by=[tables.c.table_name],
        )
        view_names = [r[0] for r in connection.execute(s)]
        return view_names

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        # TEIID aparently don't have indexes
        return []

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema, **kw):
        return ""

    @reflection.cache
    def get_columns(self, connection, table_name, schema, **kw):
        # Get base columns
        columns = systables.columns
        s = sql.select(
            [columns],
            sql.and_(
                columns.c.table_name == table_name, columns.c.table_schema == schema,
            ),
            order_by=[columns.c.ordinal_position],
        )

        c = connection.execute(s)
        cols = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            (
                name,
                type_,
                nullable,
                charlen,
                numericprec,
                numericscale,
                default,
                description,
            ) = (
                row[columns.c.column_name],
                row[columns.c.data_type],
                row[columns.c.is_nullable] == "Nullable",
                row[columns.c.element_length],
                row[columns.c.numeric_precision],
                row[columns.c.numeric_scale],
                row[columns.c.default_value],
                row[columns.c.description],
            )
            coltype = self.ischema_names.get(type_, None)

            kwargs = {}
            if coltype in (VARCHAR, VARBINARY, CHAR):
                if charlen == -1:
                    charlen = None
                kwargs["length"] = charlen
                # if collation:
                #     kwargs["collation"] = collation

            if coltype is None:
                util.warn("Did not recognize type '%s' of column '%s'" % (type_, name))
                coltype = sqltypes.NULLTYPE
            else:
                if issubclass(coltype, sqltypes.Numeric):
                    kwargs["precision"] = numericprec

                    if not issubclass(coltype, sqltypes.Float):
                        kwargs["scale"] = numericscale

                coltype = coltype(**kwargs)
            cdict = {
                "name": name,
                "type": coltype,
                "nullable": nullable,
                "default": default,
            }
            cols.append(cdict)
        return cols

    @reflection.cache
    def get_pk_constraint(self, connection, tablename, schema, **kw):
        # SERPRO doesn't implemented SYS.Keys appropriately right now
        return {"constrained_columns": [], "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, tablename, schema, **kw):
        # SERPRO doesn't implemented SYS.Keys appropriately right now
        return []

    @reflection.cache
    def get_unique_constraints(self, connection, tablename, schema, **kw):
        return []

    @reflection.cache
    def get_check_constraints(self, connection, tablename, schema, **kw):
        return []


dialect = TeiidDialect
