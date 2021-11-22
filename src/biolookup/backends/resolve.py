# -*- coding: utf-8 -*-

"""A resolution function for BLS backends."""

import gzip
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, Mapping, Optional, Union

import pandas as pd
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .backend import Backend
from .memory_backend import MemoryBackend
from .sql_backend import RawSQLBackend

__all__ = [
    "get_backend",
]

logger = logging.getLogger(__name__)


def get_backend(
    *,
    name_data: Union[None, str, pd.DataFrame] = None,
    alts_data: Union[None, str, pd.DataFrame] = None,
    defs_data: Union[None, str, pd.DataFrame] = None,
    species_data: Union[None, str, pd.DataFrame] = None,
    lazy: bool = False,
    sql: bool = False,
    uri: Union[None, str, Engine] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
    species_table: Optional[str] = None,
    synonyms_table: Optional[str] = None,
    xrefs_table: Optional[str] = None,
    rels_table: Optional[str] = None,
) -> Backend:
    """Get the backend based on the input data."""
    if sql:
        logger.info("using raw SQL backend")
        return RawSQLBackend(
            engine=uri,
            refs_table=refs_table,
            alts_table=alts_table,
            defs_table=defs_table,
            species_table=species_table,
            synonyms_table=synonyms_table,
            xrefs_table=xrefs_table,
            rels_table=rels_table,
        )

    if lazy:
        name_lookup = None
    elif isinstance(name_data, str):
        name_lookup = _get_lookup_from_path(name_data, desc=f"Processing names from {name_data}")
    elif name_data is None:
        from pyobo.resource_utils import ensure_ooh_na_na

        name_lookup = _get_lookup_from_path(ensure_ooh_na_na(), desc="Processing names from zenodo")
    elif isinstance(name_data, pd.DataFrame):
        name_lookup = _get_lookup_from_df(name_data, desc="Processing names from dataframe")
    else:
        raise TypeError(f"invalid type for `name_data`: {name_data}")

    if lazy:
        species_lookup = None
    elif isinstance(species_data, str):
        species_lookup = _get_lookup_from_path(
            species_data, desc=f"Processing species from {species_data}"
        )
    elif species_data is None:
        from pyobo.resource_utils import ensure_species

        species_lookup = _get_lookup_from_path(
            ensure_species(), desc="Processing species from zenodo"
        )
    elif isinstance(species_data, pd.DataFrame):
        species_lookup = _get_lookup_from_df(species_data, desc="Processing species from dataframe")
    else:
        raise TypeError(f"invalid type for `species_data`: {species_data}")

    if lazy:
        alts_lookup = None
    elif isinstance(alts_data, str):
        alts_lookup = _get_lookup_from_path(alts_data, desc=f"Processing alts from {alts_data}")
    elif alts_data is None and not lazy:
        from pyobo.resource_utils import ensure_alts

        alts_lookup = _get_lookup_from_path(ensure_alts(), desc="Processing alts from zenodo")
    elif isinstance(alts_data, pd.DataFrame):
        alts_lookup = _get_lookup_from_df(alts_data, desc="Processing alts from dataframe")
    else:
        raise TypeError(f"invalid type for `alt_data`: {alts_data}")

    if lazy:
        defs_lookup = None
    elif isinstance(defs_data, str):
        defs_lookup = _get_lookup_from_path(defs_data, desc=f"Processing defs from {defs_data}")
    elif defs_data is None and not lazy:
        from pyobo.resource_utils import ensure_definitions

        defs_lookup = _get_lookup_from_path(
            ensure_definitions(), desc="Processing defs from zenodo"
        )
    elif isinstance(defs_data, pd.DataFrame):
        defs_lookup = _get_lookup_from_df(defs_data, desc="Processing defs from dataframe")
    else:
        raise TypeError(f"invalid type for `defs_data`: {defs_data}")

    return _prepare_backend_with_lookup(
        name_lookup=name_lookup,
        alts_lookup=alts_lookup,
        defs_lookup=defs_lookup,
        species_lookup=species_lookup,
    )


def _get_lookup_from_df(
    df: pd.DataFrame, desc: Optional[str] = None
) -> Mapping[str, Mapping[str, str]]:
    lookup: DefaultDict[str, Dict[str, str]] = defaultdict(dict)
    if desc is None:
        desc = "processing mappings from df"
    it = tqdm(df.values, total=len(df.index), desc=desc, unit_scale=True)
    for prefix, identifier, name in it:
        lookup[prefix][identifier] = name
    return dict(lookup)


def _get_lookup_from_path(
    path: Union[str, Path], desc: Optional[str] = None
) -> Mapping[str, Mapping[str, str]]:
    lookup: DefaultDict[str, Dict[str, str]] = defaultdict(dict)
    if desc is None:
        desc = "loading mappings"
    with gzip.open(path, "rt") as file:
        _ = next(file)
        for line in tqdm(file, desc=desc, unit_scale=True):
            prefix, identifier, name = line.strip().split("\t")
            lookup[prefix][identifier] = name
    return dict(lookup)


def _prepare_backend_with_lookup(
    name_lookup: Optional[Mapping[str, Mapping[str, str]]] = None,
    alts_lookup: Optional[Mapping[str, Mapping[str, str]]] = None,
    defs_lookup: Optional[Mapping[str, Mapping[str, str]]] = None,
    species_lookup: Optional[Mapping[str, Mapping[str, str]]] = None,
) -> Backend:
    import pyobo

    get_id_name_mapping, summarize_names = _h(name_lookup, pyobo.get_id_name_mapping)
    get_id_species_mapping, summarize_species = _h(species_lookup, pyobo.get_id_species_mapping)
    get_alts_to_id, summarize_alts = _h(alts_lookup, pyobo.get_alts_to_id)
    get_id_definition_mapping, summarize_definitions = _h(
        defs_lookup, pyobo.get_id_definition_mapping
    )

    return MemoryBackend(
        get_id_name_mapping=get_id_name_mapping,
        get_id_species_mapping=get_id_species_mapping,
        get_alts_to_id=get_alts_to_id,
        get_id_definition_mapping=get_id_definition_mapping,
        summarize_names=summarize_names,
        summarize_alts=summarize_alts,
        summarize_definitions=summarize_definitions,
        summarize_species=summarize_species,
    )


def _h(lookup, alt_lookup):
    if lookup is None:  # lazy mode, will download/cache data as needed
        return alt_lookup, Counter

    def _summarize():
        return Counter({k: len(v) for k, v in lookup.items()})

    return lookup.get, _summarize
