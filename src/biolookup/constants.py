# -*- coding: utf-8 -*-

"""Constants for the biolookup service."""

import pystow

REFS_TABLE_NAME = "obo_reference"
SPECIES_TABLE_NAME = "obo_species"
ALTS_TABLE_NAME = "obo_alt"
DEFS_TABLE_NAME = "obo_def"
DERIVED_NAME = "entities"
SYNONYMS_NAME = "synonyms"
XREFS_NAME = "xrefs"
RELS_NAME = "relations"

MODULE = pystow.module("biolookup")

DEFAULT_URL = "http://biolookup.io"
DEFAULT_URL = DEFAULT_URL.rstrip("/")
DEFAULT_ENDPOINT = "api/lookup"
DEFAULT_ENDPOINT = DEFAULT_ENDPOINT.strip("/")


def get_sqlalchemy_uri() -> str:
    """Get the SQLAlchemy URI."""
    rv = pystow.get_config("biolookup", "sqlalchemy_uri")
    if rv is not None:
        return rv

    # Default value
    return MODULE.joinpath_sqlite(name="biolookup.db")
