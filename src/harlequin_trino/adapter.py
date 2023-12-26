from __future__ import annotations

from typing import Any

from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_trino.cli_options import TRINO_OPTIONS
from harlequin_trino.completions import load_completions
from trino.auth import BasicAuthentication
from trino.dbapi import connect


class HarlequinTrinoCursor(HarlequinCursor):
    def __init__(self, cur: Any) -> None:
        self.cur = cur
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        assert self.cur.description is not None
        return [(col[0], self._get_short_type(col[1])) for col in self.cur.description]

    def set_limit(self, limit: int) -> HarlequinTrinoCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        finally:
            self.cur.close()

    @staticmethod
    def _get_short_type(type_name: str) -> str:
        MAPPING = {
            "array": "[]",
            "bigint": "##",
            "boolean": "t/f",
            "char": "s",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "ipaddress": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "real": "#.#",
            "smallint": "#",
            "time": "t",
            "timestamp": "ts",
            "tinyint": "#",
            "uuid": "uid",
            "varchar": "t",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")


class HarlequinTrinoConnection(HarlequinConnection):
    def __init__(
        self,
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        modified_options = options.copy()
        password = modified_options.pop("password")
        auth = modified_options.pop("require_auth", "None")
        sslcert = modified_options.pop("sslcert", None)
        if auth == "password":
            user = modified_options.get("user")
            modified_options["auth"] = BasicAuthentication(user, password)
            modified_options["http_scheme"] = "https"
            modified_options["verify"] = sslcert if sslcert else False
        try:
            self.conn = connect(**modified_options)
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to your database."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.cursor().execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        return HarlequinTrinoCursor(cur)

    def get_catalog(self) -> Catalog:
        catalogs = self._get_catalogs()
        db_items: list[CatalogItem] = []
        for (catalog,) in catalogs:
            schemas = self._get_schemas(catalog)
            schema_items: list[CatalogItem] = []
            for (schema,) in schemas:
                relations = self._get_relations(catalog, schema)
                rel_items: list[CatalogItem] = []
                for rel, rel_type in relations:
                    cols = self._get_columns(catalog, schema, rel)
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{catalog}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=self._get_short_col_type(col_type),
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{catalog}"."{schema}"."{rel}"',
                            query_name=f'"{catalog}"."{schema}"."{rel}"',
                            label=rel,
                            type_label=rel_type,
                            children=col_items,
                        )
                    )
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{catalog}"."{schema}"',
                        query_name=f'"{catalog}"."{schema}"',
                        label=schema,
                        type_label="s",
                        children=rel_items,
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"{catalog}',
                    query_name=f'"{catalog}"',
                    label=catalog,
                    type_label="c",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def _get_catalogs(self) -> list[tuple[str]]:
        cur = self.conn.cursor()
        cur.execute("SHOW CATALOGS")
        results: list[tuple[str]] = cur.fetchall()
        cur.close()
        return [
            result for result in results if result[0] not in ["jmx", "memory", "system"]
        ]

    def _get_schemas(self, catalog: str) -> list[tuple[str]]:
        cur = self.conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        results: list[tuple[str]] = cur.fetchall()
        cur.close()
        return [result for result in results if result[0] != "information_schema"]

    def _get_relations(self, catalog: str, schema: str) -> list[tuple[str, str]]:
        cur = self.conn.cursor()
        query = f"""
            SELECT
                table_name,
                case
                    when table_type like '%TABLE' then 't'
                    else 'v'
                end as table_type
            FROM "{catalog}".information_schema.tables
            WHERE table_schema = '{schema}'
        """
        cur.execute(query)
        results: list[tuple[str, str]] = cur.fetchall()
        cur.close()
        return results

    def _get_columns(
        self, catalog: str, schema: str, rel: str
    ) -> list[tuple[str, str]]:
        cur = self.conn.cursor()
        query = f"""
            SELECT
                column_name,
                data_type 
            FROM {catalog}.information_schema.columns
            WHERE 
                table_schema = '{schema}'
                and table_name = '{rel}'
        """
        cur.execute(query)
        results: list[tuple[str, str]] = cur.fetchall()
        cur.close()
        return results

    @staticmethod
    def _get_short_col_type(type_name: str) -> str:
        MAPPING = {
            "array": "[]",
            "bigint": "##",
            "boolean": "t/f",
            "char": "s",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "ipaddress": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "real": "#.#",
            "smallint": "#",
            "time": "t",
            "timestamp": "ts",
            "tinyint": "#",
            "uuid": "uid",
            "varchar": "t",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")

    def get_completions(self) -> list[HarlequinCompletion]:
        return load_completions()


class HarlequinTrinoAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = TRINO_OPTIONS

    def __init__(
        self,
        host: str | None = None,
        port: str | None = None,
        user: str | None = None,
        password: str | None = None,
        require_auth: str | None = None,
        sslcert: str | None = None,
        **_: Any,
    ) -> None:
        self.options = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "require_auth": require_auth,
            "sslcert": sslcert,
        }

    def connect(self) -> HarlequinTrinoConnection:
        conn = HarlequinTrinoConnection(options=self.options)
        return conn
