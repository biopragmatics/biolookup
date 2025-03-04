"""Proxies for the Biolookup Service."""

from typing import cast

from flask import current_app
from werkzeug.local import LocalProxy

from ..backends import Backend

__all__ = ["backend"]

backend = cast(Backend, LocalProxy(lambda: current_app.config["resolver_backend"]))
