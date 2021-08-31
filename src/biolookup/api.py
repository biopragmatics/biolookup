# -*- coding: utf-8 -*-

"""High-level API for the biolookup service."""

import logging
from functools import lru_cache
from typing import Any, Mapping

import pystow

from .backends import Backend, RawSQLBackend, RemoteBackend

__all__ = [
    "lookup",
]

logger = logging.getLogger(__name__)


@lru_cache(maxsize=2)
def _get_default_backend(remote_fallback: bool = True) -> Backend:
    uri = pystow.get_config("biolookup", "sqlalchemy_uri")
    if uri is not None:
        return RawSQLBackend(uri)
    if remote_fallback:
        logger.debug(
            "no connection to BIOLOOKUP_SQLALCHEMY_URI found, defaulting to remote backend"
        )
        return RemoteBackend()
    raise RuntimeError(
        "could not get default backend since BIOLOOKUP_SQLALCHEMY_URI"
        " not found and not using remote fallback"
    )


def lookup(curie: str, remote_fallback: bool = True, **kwargs) -> Mapping[str, Any]:
    """Return the results and summary when resolving a CURIE string."""
    backend = _get_default_backend(remote_fallback=remote_fallback)
    return backend.lookup(curie=curie, **kwargs)
