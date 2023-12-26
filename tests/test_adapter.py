import sys
from typing import Generator

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinQueryError
from harlequin_trino.adapter import HarlequinTrinoAdapter, HarlequinTrinoConnection
from textual_fastdatatable.backend import create_backend

from trino.dbapi import connect

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


@pytest.fixture
def trino_options() -> dict:
    return {"host": "localhost", "port": 8080, "user": "trino"}


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "trino"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()  # type: ignore
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinTrinoAdapter


def test_connect(trino_options: dict) -> None:
    conn = HarlequinTrinoAdapter(**trino_options).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs(trino_options: dict) -> None:
    assert HarlequinTrinoAdapter(**trino_options, foo=1, bar="baz").connect()


@pytest.fixture
def connection(trino_options: dict) -> Generator[HarlequinTrinoConnection, None, None]:
    mytrinoconn = connect(**trino_options)
    cur = mytrinoconn.cursor()
    cur.execute("drop schema if exists my_catalog.my_schema cascade")
    cur.execute("create schema my_catalog.my_schema")
    cur.close()
    conn = HarlequinTrinoAdapter(**trino_options).connect()
    yield conn
    cur = mytrinoconn.cursor()
    cur.execute("drop schema if exists my_catalog.my_schema cascade")
    cur.close()


def test_get_catalog(connection: HarlequinTrinoConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection: HarlequinTrinoConnection) -> None:
    cur = connection.execute("CREATE TABLE my_catalog.my_schema.my_table (a int)")
    assert cur is not None
    data = cur.fetchall()
    assert not data


def test_execute_select(connection: HarlequinTrinoConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_dupe_cols(connection: HarlequinTrinoConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinTrinoConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinTrinoConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")
