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

from sqlalchemy.types import BIGINT
from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import CHAR
from sqlalchemy.types import DATE
from sqlalchemy.types import FLOAT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import REAL
from sqlalchemy.types import SMALLINT
from sqlalchemy.types import TEXT
from sqlalchemy.types import VARCHAR

logger = logging.getLogger("teiid")


class TIMESTAMP(sqltypes.TIMESTAMP):
    def __init__(self, timezone=False, precision=None):
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


class TIME(sqltypes.TIME):
    def __init__(self, timezone=False, precision=None):
        super(TIME, self).__init__(timezone=timezone)
        self.precision = precision


ischema_names = {
    "integer": INTEGER,
    "bigint": BIGINT,
    "long": BIGINT,  # TEIID
    "smallint": SMALLINT,
    "short": SMALLINT,  # TEIID
    "character varying": VARCHAR,
    "string": VARCHAR,  # TEIID
    "character": CHAR,
    '"char"': sqltypes.String,
    "name": sqltypes.String,
    "text": TEXT,
    "numeric": NUMERIC,
    "bigdecimal": NUMERIC,  # TEIID
    "float": FLOAT,
    "real": REAL,
    "timestamp": TIMESTAMP,
    "timestamp with time zone": TIMESTAMP,
    "timestamp without time zone": TIMESTAMP,
    "time with time zone": TIME,
    "time without time zone": TIME,
    "date": DATE,
    "time": TIME,
    "boolean": BOOLEAN,
}


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
    def get_columns(self, connection, table_name, schema=None, **kw):

        # table_oid = self.get_table_oid(
        #     connection, table_name, schema, info_cache=kw.get("info_cache")
        # )
        SQL_COLS = """
            SELECT Name,
            DataType,
            Scale,
            Length,
            CASE
                WHEN NullType = 'No Nulls' THEN False
                ELSE True
            END AS "Nullable",
            Position,
            Description
            FROM SYS.Columns
            WHERE SchemaName = :schema AND TableName = :table_name
            ORDER BY Position
        """
        s = (
            sql.text(SQL_COLS)
            .bindparams(
                sql.bindparam("schema", type_=sqltypes.Unicode),
                sql.bindparam("table_name", type_=sqltypes.Unicode),
            )
            .columns(Name=sqltypes.Unicode, Description=sqltypes.Unicode)
        )
        c = connection.execute(s, schema=schema, table_name=table_name)
        rows = c.fetchall()

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        # domains = self._load_domains(connection)

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        # enums = dict(
        #     ((rec["name"],), rec)
        #     if rec["visible"]
        #     else ((rec["schema"], rec["name"]), rec)
        #     for rec in self._load_enums(connection, schema="*")
        # )

        # format columns
        columns = []

        for (name, datatype, scale, length, nullable, position, description) in rows:
            column_info = self._get_column_info(
                name, datatype, scale, length, nullable, schema, description
            )
            columns.append(column_info)
        return columns

    def _get_column_info(
        self, name, datatype, scale, length, nullable, schema, description
    ):
        def _handle_array_type(attype):
            return (
                # strip '[]' from integer[], etc.
                re.sub(r"\[\]$", "", attype),
                attype.endswith("[]"),
            )

        # strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r"\(.*\)", "", datatype)

        # strip '[]' from integer[], etc. and check if an array
        attype, is_array = _handle_array_type(attype)

        # strip quotes from case sensitive enum or domain names
        # enum_or_domain_key = tuple(util.quoted_token_parser(attype))

        # nullable = not notnull

        charlen = re.search(r"\(([\d,]+)\)", datatype)
        if charlen:
            charlen = charlen.group(1)
        args = re.search(r"\((.*)\)", datatype)
        if args and args.group(1):
            args = tuple(re.split(r"\s*,\s*", args.group(1)))
        else:
            args = ()
        kwargs = {}

        if attype == "numeric":
            if charlen:
                prec, scale = charlen.split(",")
                args = (int(prec), int(scale))
            else:
                args = ()
        elif attype == "double precision":
            args = (53,)
        elif attype == "integer":
            args = ()
        elif attype in ("timestamp with time zone", "time with time zone"):
            kwargs["timezone"] = True
            if charlen:
                kwargs["precision"] = int(charlen)
            args = ()
        elif attype in (
            "timestamp without time zone",
            "time without time zone",
            "time",
        ):
            kwargs["timezone"] = False
            if charlen:
                kwargs["precision"] = int(charlen)
            args = ()
        elif attype == "bit varying":
            kwargs["varying"] = True
            if charlen:
                args = (int(charlen),)
            else:
                args = ()
        elif attype.startswith("interval"):
            field_match = re.match(r"interval (.+)", attype, re.I)
            if charlen:
                kwargs["precision"] = int(charlen)
            if field_match:
                kwargs["fields"] = field_match.group(1)
            attype = "interval"
            args = ()
        elif charlen:
            args = (int(charlen),)

        while True:
            # looping here to suit nested domains
            if attype in self.ischema_names:
                coltype = self.ischema_names[attype]
                break
            else:
                coltype = None
                break

        if coltype:
            coltype = coltype(*args, **kwargs)
            if is_array:
                coltype = self.ischema_names["_array"](coltype)
        else:
            util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
            coltype = sqltypes.NULLTYPE

        column_info = dict(
            name=name,
            type=coltype,
            nullable=nullable,
            default=None,
            autoincrement=False,
            comment=description,
        )
        return column_info

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pass

    def get_foreign_keys(
        self,
        connection,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw
    ):
        return [
            dict(
                name="",
                constrained_columns=[],
                referred_schema=schema,
                referred_table=table_name,
                referred_columns=[],
            )
        ]

    def get_indexes(self, connection, table_name, schema, **kw):
        return [dict(name="", column_names=[], unique=None)]

    def get_unique_constraints(
        self, connection, table_name, schema=None, **kw
    ):
        return [dict(name="", column_names=[])]

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return [dict(name="", sqltext="", )]

dialect = TeiidDialect
