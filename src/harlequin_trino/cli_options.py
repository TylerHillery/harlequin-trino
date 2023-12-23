from __future__ import annotations

from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
    else:
        return True, ""


host = TextOption(
    name="host",
    description=("Hostname of the Trino coordinator"),
    short_decls=["-h"],
    default="localhost",
)


port = TextOption(
    name="port",
    description=("Port number to connect at the Trino coordinator."),
    short_decls=["-p"],
    default="8080",
)


user = TextOption(
    name="user",
    description=("Trino user name to connect as."),
    short_decls=["-u", "--username", "-U"],
    default="trino",
)


password = TextOption(
    name="password",
    description=("Password to be used if the server demands password authentication."),
)


connect_timeout = TextOption(
    name="connect_timeout",
    description=(
        "Maximum time to wait while connecting, in seconds (write as an integer, "
        "e.g., 10)."
    ),
    validator=_int_validator,
)


TRINO_OPTIONS = [
    host,
    port,
    user,
    password,
    connect_timeout,
]
