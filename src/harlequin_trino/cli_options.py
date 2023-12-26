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


require_auth = SelectOption(
    name="require_auth",
    description=(
        "Specifies the authentication method that the client requires from the server. "
        "If the server does not use the required method to authenticate the client, or "
        "if the authentication handshake is not fully completed by the server, the "
        "connection will fail."
    ),
    choices=["password", "none"],
)

sslcert = PathOption(
    name="sslcert",
    description=("Specifies the file name of the client SSL certificate. "),
)

TRINO_OPTIONS = [host, port, user, password, require_auth, sslcert]
