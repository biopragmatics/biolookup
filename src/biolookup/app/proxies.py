# -*- coding: utf-8 -*-

"""Proxies for the Biolookup Service."""

from flask import current_app
from werkzeug.local import LocalProxy

from ..backends import Backend

__all__ = ["backend"]

backend: Backend = LocalProxy(lambda: current_app.config["resolver_backend"])
