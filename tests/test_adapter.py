import sys

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin_trino.adapter import HarlequinTrinoAdapter, HarlequinTrinoConnection
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

@pytest.fixture
def trino_options():
    return {
        "host": "localhost",
        "port": 8080,
        "user": "trino"
    }

def test_plugin_discovery() -> None:
    PLUGIN_NAME = "trino"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinTrinoAdapter


def test_connect(trino_options) -> None:
    conn = HarlequinTrinoAdapter(conn_str=tuple(), **trino_options).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs(trino_options) -> None:
    assert HarlequinTrinoAdapter(conn_str=tuple(), **trino_options, foo=1, bar="baz").connect()


def test_connect_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinTrinoAdapter(conn_str=("foo",)).connect()


@pytest.fixture
def connection(trino_options) -> HarlequinTrinoConnection:
    return HarlequinTrinoAdapter(conn_str=tuple(), **trino_options).connect()


def test_get_catalog(connection: HarlequinTrinoConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection: HarlequinTrinoConnection) -> None:
    cur = connection.execute("create table tpch.tiny.foo (a int)")
    assert cur is None


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
