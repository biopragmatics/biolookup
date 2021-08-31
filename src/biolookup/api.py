# -*- coding: utf-8 -*-

"""High-level API for the biolookup service."""

from functools import lru_cache
from typing import Any, Mapping

import pystow

from .backends import Backend, get_backend

__all__ = [
    "lookup",
]


@lru_cache(1)
def _get_default_backend() -> Backend:
    uri = pystow.get_config("biolookup", "sqlalchemy_uri")
    if uri is None:
        raise ValueError("must set BIOLOOKUP_SQLALCHEMY_URI to use high level functionality")
    return get_backend(sql=True, uri=uri)


def lookup(curie: str, fallback: bool = True, **kwargs) -> Mapping[str, Any]:
    """Return the results and summary when resolving a CURIE string."""
    backend = _get_default_backend()
    return backend.lookup(curie=curie, **kwargs)
