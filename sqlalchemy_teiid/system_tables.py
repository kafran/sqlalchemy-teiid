# system_tables.py
# Copyright (C) 2020 Kolmar Kafran

from sqlalchemy import Column, MetaData, Table, cast, util
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression, quoted_name
from sqlalchemy.types import Boolean, Integer, String

systables = MetaData(quote_schema=False)

schemas = Table(
    "Schemas",
    systables,
    Column(quoted_name("VDBName", False), String, key="vdb_name"),
    Column(quoted_name("Name", False), String, key="schema_name"),
    Column(quoted_name("IsPhysical", False), Boolean, key="is_physical"),
    Column(quoted_name("UID", False), String, key="schema_uid"),
    schema="SYS",
)

tables = Table(
    "Tables",
    systables,
    Column(quoted_name("VDBName", False), String, key="vdb_name"),
    Column(quoted_name("SchemaName", False), String, key="schema_name"),
    Column(quoted_name("Name", False), String, key="table_name"),
    Column(quoted_name("Type", False), String, key="table_type"),
    Column(quoted_name("IsSystem", False), Boolean, key="is_system"),
    Column(quoted_name("UID", False), String, key="table_uid"),
    Column(quoted_name("Description", False), String, key="table_description"),
    schema="SYS",
)

columns = Table(
    "Columns",
    systables,
    Column(quoted_name("VDBName", False), String, key="vdb_name"),
    Column(quoted_name("SchemaName", False), String, key="table_schema"),
    Column(quoted_name("TableName", False), String, key="table_name"),
    Column(quoted_name("Name", False), String, key="column_name"),
    Column(quoted_name("NullType", False), String, key="is_nullable"),
    Column(quoted_name("DataType", False), String, key="data_type"),
    Column(quoted_name("Position", False), Integer, key="ordinal_position"),
    Column(quoted_name("Length", False), Integer, key="element_length"),
    Column(quoted_name("Precision", False), Integer, key="numeric_precision"),
    Column(quoted_name("Scale", False), Integer, key="numeric_scale"),
    Column(quoted_name("DefaultValue", False), String, key="default_value"),
    Column(quoted_name("Format", False), String, key="collation_name"),
    Column(quoted_name("Description", False), String, key="description"),
    schema="SYS",
)
