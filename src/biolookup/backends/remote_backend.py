# -*- coding: utf-8 -*-

"""A remote backend for the Biolookup Service."""

from typing import Any, Mapping, Optional

import requests

from .backend import Backend
from ..constants import DEFAULT_ENDPOINT, DEFAULT_URL

__all__ = [
    "RemoteBackend",
]


class RemoteBackend(Backend):
    """A remote backend."""

    def __init__(self, base_url: Optional[str] = None, endpoint: Optional[str] = None):
        """Instantiate the remote backend.

        :param base_url: The base URL, defaults to http://biolookup.io
        :param endpoint: The endpoint name. Defaults to ``lookup``. This is configurable since
            some instances might mount the API differently.
        """
        self.base_url = (base_url or DEFAULT_URL).rstrip("/")
        self.endpoint = (endpoint or DEFAULT_ENDPOINT).strip("/")

    def lookup(self, curie: str, *, resolve_alternate: bool = True) -> Mapping[str, Any]:
        """Lookup the CURIE using the remote service."""
        res = requests.get(f"{self.base_url}/{self.endpoint}/{curie}")
        res.raise_for_status()
        return res.json()


def _main():
    backend = RemoteBackend()
    print(backend.lookup("doid:14330"))


if __name__ == "__main__":
    _main()
