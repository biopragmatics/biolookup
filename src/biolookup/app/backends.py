# -*- coding: utf-8 -*-

"""Backends."""

import gzip
import logging
import time
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, DefaultDict, Dict, List, Mapping, Optional, Union

import bioregistry
import pandas as pd
import pyobo
from pyobo import normalize_curie
from pyobo.constants import ALTS_TABLE_NAME, DEFS_TABLE_NAME, REFS_TABLE_NAME, get_sqlalchemy_uri
from pyobo.resource_utils import ensure_alts, ensure_definitions, ensure_ooh_na_na
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm


__all__ = [
    "Backend",
    "RawSQLBackend",
    "MemoryBackend",
    "get_backend",
]

logger = logging.getLogger(__name__)


class Backend:
    """A resolution service."""

    def has_prefix(self, prefix: str) -> bool:
        """Check if there is a resource available with the given prefix."""
        raise NotImplementedError

    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier in the given resource."""
        raise NotImplementedError

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the canonical/preferred (english) name for the identifier in the given resource."""
        raise NotImplementedError

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the definition associated with the prefix/identifier."""
        raise NotImplementedError

    def get_synonyms(self, prefix: str, identifier: str) -> List[str]:
        """Get a list of synonyms."""
        raise NotImplementedError

    def get_xrefs(self, prefix: str, identifier: str) -> List[Mapping[str, str]]:
        """Get a list of xrefs."""
        raise NotImplementedError

    def summarize_names(self) -> Mapping[str, Any]:
        """Summarize the names."""
        raise NotImplementedError

    def summarize_alts(self) -> Mapping[str, Any]:
        """Summarize the alternate identifiers."""
        raise NotImplementedError

    def summarize_definitions(self) -> Mapping[str, Any]:
        """Summarize the definitions."""
        raise NotImplementedError

    def count_all(self):
        """Count all."""
        self.count_prefixes()
        self.count_prefixes()
        self.count_alts()
        self.count_names()

    def count_names(self) -> Optional[int]:
        """Count the number of names in the database."""

    def count_definitions(self) -> Optional[int]:
        """Count the number of definitions in the database."""

    def count_alts(self) -> Optional[int]:
        """Count the number of alternative identifiers in the database."""

    def count_prefixes(self) -> Optional[int]:
        """Count the number of prefixes in the database."""

    def resolve(self, curie: str, resolve_alternate: bool = True) -> Mapping[str, Any]:
        """Return the results and summary when resolving a CURIE string."""
        prefix, identifier = normalize_curie(curie, strict=False)
        if prefix is None or identifier is None:
            return dict(
                query=curie,
                success=False,
                message="Could not identify prefix",
            )

        providers = bioregistry.get_providers(prefix, identifier)
        if not self.has_prefix(prefix):
            rv = dict(
                query=curie,
                prefix=prefix,
                identifier=identifier,
                providers=providers,
                success=False,
                message=f"Could not find id->name mapping for {prefix}",
            )
            return rv

        name = self.get_name(prefix, identifier)
        if name is None and resolve_alternate:
            identifier, _secondary_id = self.get_primary_id(prefix, identifier), identifier
            if identifier != _secondary_id:
                providers = bioregistry.get_providers(prefix, identifier)
                name = self.get_name(prefix, identifier)

        if name is None:
            return dict(
                query=curie,
                prefix=prefix,
                identifier=identifier,
                success=False,
                providers=providers,
                message="Could not look up identifier",
            )
        rv = dict(
            query=curie,
            prefix=prefix,
            identifier=identifier,
            name=name,
            success=True,
            providers=providers,
        )
        definition = self.get_definition(prefix, identifier)
        if definition:
            rv["definition"] = definition

        return rv

    def summary_df(self) -> pd.DataFrame:
        """Generate a summary dataframe."""
        summary_names = self.summarize_names()
        summary_alts = self.summarize_alts() if self.summarize_alts is not None else {}
        summary_defs = (
            self.summarize_definitions() if self.summarize_definitions is not None else {}
        )
        return pd.DataFrame(
            [
                (
                    prefix,
                    bioregistry.get_name(prefix),
                    bioregistry.get_homepage(prefix),
                    bioregistry.get_example(prefix),
                    bioregistry.get_link(prefix, bioregistry.get_example(prefix)),
                    names_count,
                    summary_alts.get(prefix, 0),
                    summary_defs.get(prefix, 0),
                )
                for prefix, names_count in summary_names.items()
            ],
            columns=[
                "prefix",
                "name",
                "homepage",
                "example",
                "link",
                "names",
                "alts",
                "defs",
            ],
        )


class MemoryBackend(Backend):
    """A resolution service using a dictionary-based in-memory cache."""

    def __init__(
        self,
        get_id_name_mapping,
        get_alts_to_id,
        summarize_names: Optional[Callable[[], Mapping[str, Any]]],
        summarize_alts: Optional[Callable[[], Mapping[str, Any]]] = None,
        summarize_definitions: Optional[Callable[[], Mapping[str, Any]]] = None,
        get_id_definition_mapping=None,
    ) -> None:
        """Initialize the in-memory backend.

        :param get_id_name_mapping: A function for getting id-name mappings
        :param get_alts_to_id: A function for getting alts-id mappings
        :param summarize_names: A function for summarizing references
        :param summarize_alts: A function for summarizing alts
        :param summarize_definitions: A function for summarizing definitions
        :param get_id_definition_mapping: A function for getting id-def mappings
        """
        self.get_id_name_mapping = get_id_name_mapping
        self.get_alts_to_id = get_alts_to_id
        self.get_id_definition_mapping = get_id_definition_mapping
        self._summarize_names = summarize_names
        self._summarize_alts = summarize_alts
        self._summarize_definitions = summarize_definitions

    def has_prefix(self, prefix: str) -> bool:
        """Check for the prefix using the id/name getter."""
        return self.get_id_name_mapping(prefix) is not None

    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier with the alts/id getter."""
        alts_to_id = self.get_alts_to_id(prefix) or {}
        return alts_to_id.get(identifier, identifier)

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with the id/name getter."""
        id_name_mapping = self.get_id_name_mapping(prefix) or {}
        return id_name_mapping.get(identifier)

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with the id/definition getter, if available."""
        if self.get_id_definition_mapping is None:
            return None
        id_definition_mapping = self.get_id_definition_mapping(prefix) or {}
        return id_definition_mapping.get(identifier)

    def summarize_names(self) -> Mapping[str, Any]:
        """Summarize the names with the internal name summary function, if available."""
        if self._summarize_names is None:
            return {}
        return self._summarize_names()

    def count_prefixes(self) -> int:
        """Count prefixes using the name summary."""
        return len(self.summarize_names().keys())

    def count_names(self) -> int:
        """Count names using the name summary."""
        return sum(self.summarize_names().values())

    def summarize_alts(self) -> Mapping[str, Any]:
        """Summarize the alts with the internal alt summary function, if available."""
        if self._summarize_alts is None:
            return {}
        return self._summarize_alts()

    def count_alts(self) -> int:
        """Count alts using the alt summary."""
        return sum(self.summarize_alts().values())

    def summarize_definitions(self) -> Mapping[str, Any]:
        """Summarize the definitions with the internal definition summary function, if available."""
        if self._summarize_definitions is None:
            return {}
        return self._summarize_definitions()

    def count_definitions(self) -> int:
        """Count definitions using the definition summary."""
        return sum(self.summarize_definitions().values())


def _ensure_engine(engine: Union[None, str, Engine]) -> Engine:
    if engine is None:
        return create_engine(get_sqlalchemy_uri())
    elif isinstance(engine, str):
        return create_engine(engine)
    else:
        return engine


class RawSQLBackend(Backend):
    """A backend that communicates with low-level SQL statements."""

    #: The engine
    engine: Engine

    def __init__(
        self,
        *,
        refs_table: Optional[str] = None,
        alts_table: Optional[str] = None,
        defs_table: Optional[str] = None,
        engine: Union[None, str, Engine] = None,
    ):
        """Initialize the raw SQL backend.

        :param refs_table: A name for the references (prefix-id-name) table. Defaults to 'obo_reference'
        :param alts_table: A name for the alts (prefix-id-alt) table. Defaults to 'obo_alt'
        :param defs_table: A name for the defs (prefix-id-def) table. Defaults to 'obo_def'
        :param engine: An engine, connection string, or None if you want the default.
        """
        self.engine = _ensure_engine(engine)

        self.refs_table = refs_table or REFS_TABLE_NAME
        self.alts_table = alts_table or ALTS_TABLE_NAME
        self.defs_table = defs_table or DEFS_TABLE_NAME

    @lru_cache(maxsize=1)
    def count_names(self) -> int:
        """Get the number of names."""
        return self._get_one(
            f"SELECT SUM(identifier_count) FROM {self.refs_table}_summary;"  # noqa:S608
        )

    @lru_cache(maxsize=1)
    def count_prefixes(self) -> int:
        """Count prefixes using a SQL query to the references summary table."""
        logger.info("counting prefixes")
        start = time.time()
        rv = self._get_one(
            f"SELECT COUNT(DISTINCT prefix) FROM {self.refs_table}_summary;"  # noqa:S608
        )
        logger.info("done counting prefixes after %.2fs", time.time() - start)
        return rv

    @lru_cache(maxsize=1)
    def count_definitions(self) -> int:
        """Count definitions using a SQL query to the definitions summary table."""
        return self._get_one(
            f"SELECT SUM(identifier_count) FROM {self.defs_table}_summary;"  # noqa:S608
        )

    @lru_cache(maxsize=1)
    def count_alts(self) -> Optional[int]:
        """Count alts using a SQL query to the alts summary table."""
        logger.info("counting alts")
        return self._get_one(f"SELECT COUNT(*) FROM {self.alts_table};")  # noqa:S608

    def _get_one(self, sql: str):
        with self.engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return result[0]

    def summarize_names(self) -> Counter:
        """Return the results of a SQL query that dumps the name summary table."""
        return self._get_summary(self.refs_table)

    def summarize_alts(self) -> Counter:
        """Return the results of a SQL query that dumps the alts summary table."""
        return self._get_summary(self.alts_table)

    def summarize_definitions(self) -> Counter:
        """Return the results of a SQL query that dumps the definitions summary table."""
        return self._get_summary(self.defs_table)

    @lru_cache()
    def _get_summary(self, table) -> Counter:
        sql = f"SELECT prefix, identifier_count FROM {table}_summary;"  # noqa:S608
        with self.engine.connect() as connection:
            return Counter(dict(connection.execute(sql).fetchall()))

    @lru_cache()
    def has_prefix(self, prefix: str) -> bool:
        """Check for the prefix with a SQL query."""
        sql = text(
            f"SELECT EXISTS(SELECT 1 from {self.refs_table} WHERE prefix = :prefix);"  # noqa:S608
        )
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix).fetchone()
            return bool(result)

    @lru_cache(maxsize=100_000)
    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier with a SQL query to the alts table."""
        sql = text(
            f"""
            SELECT identifier
            FROM {self.alts_table}
            WHERE prefix = :prefix and alt = :alt;
        """  # noqa:S608
        )
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix, alt=identifier).fetchone()
            return result[0] if result else identifier

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with a SQL query to the names table."""
        return self._help_one(self.refs_table, "name", prefix, identifier)

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the definition with a SQL query to the definitions table."""
        return self._help_one(self.defs_table, "definition", prefix, identifier)

    @lru_cache(maxsize=100_000)
    def _help_one(self, table: str, column: str, prefix: str, identifier: str) -> Optional[str]:
        sql = text(
            f"""
            SELECT {column}
            FROM {table}
            WHERE prefix = :prefix and identifier = :identifier;
        """
        )  # noqa:S608
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix, identifier=identifier).fetchone()
            if result:
                return result[0]
        return None


def get_backend(
    name_data: Union[None, str, pd.DataFrame] = None,
    alts_data: Union[None, str, pd.DataFrame] = None,
    defs_data: Union[None, str, pd.DataFrame] = None,
    lazy: bool = False,
    sql: bool = False,
    uri: Optional[str] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
) -> Backend:
    """Get the backend based on the input data."""
    if sql:
        logger.info("using raw SQL backend")
        return RawSQLBackend(
            engine=uri,
            refs_table=refs_table,
            alts_table=alts_table,
            defs_table=defs_table,
        )

    if lazy:
        name_lookup = None
    elif name_data is None:
        name_lookup = _get_lookup_from_path(ensure_ooh_na_na(), desc="Processing names from zenodo")
    elif isinstance(name_data, str):
        name_lookup = _get_lookup_from_path(name_data, desc=f"Processing names from {name_data}")
    elif isinstance(name_data, pd.DataFrame):
        name_lookup = _get_lookup_from_df(name_data, desc="Processing names from dataframe")
    else:
        raise TypeError(f"invalid type for `name_data`: {name_data}")

    if lazy:
        alts_lookup = None
    elif alts_data is None and not lazy:
        alts_lookup = _get_lookup_from_path(ensure_alts(), desc="Processing alts from zenodo")
    elif isinstance(alts_data, str):
        alts_lookup = _get_lookup_from_path(alts_data, desc=f"Processing alts from {alts_data}")
    elif isinstance(alts_data, pd.DataFrame):
        alts_lookup = _get_lookup_from_df(alts_data, desc="Processing alts from dataframe")
    else:
        raise TypeError(f"invalid type for `alt_data`: {alts_data}")

    if lazy:
        defs_lookup = None
    elif defs_data is None and not lazy:
        defs_lookup = _get_lookup_from_path(
            ensure_definitions(), desc="Processing defs from zenodo"
        )
    elif isinstance(defs_data, str):
        defs_lookup = _get_lookup_from_path(defs_data, desc=f"Processing defs from {defs_data}")
    elif isinstance(defs_data, pd.DataFrame):
        defs_lookup = _get_lookup_from_df(defs_data, desc="Processing defs from dataframe")
    else:
        raise TypeError(f"invalid type for `defs_data`: {defs_data}")

    return _prepare_backend_with_lookup(
        name_lookup=name_lookup,
        alts_lookup=alts_lookup,
        defs_lookup=defs_lookup,
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
) -> Backend:
    get_id_name_mapping, summarize_names = _h(name_lookup, pyobo.get_id_name_mapping)
    get_alts_to_id, summarize_alts = _h(alts_lookup, pyobo.get_alts_to_id)
    get_id_definition_mapping, summarize_definitions = _h(
        defs_lookup, pyobo.get_id_definition_mapping
    )

    return MemoryBackend(
        get_id_name_mapping=get_id_name_mapping,
        get_alts_to_id=get_alts_to_id,
        get_id_definition_mapping=get_id_definition_mapping,
        summarize_names=summarize_names,
        summarize_alts=summarize_alts,
        summarize_definitions=summarize_definitions,
    )


def _h(lookup, alt_lookup):
    if lookup is None:  # lazy mode, will download/cache data as needed
        return alt_lookup, Counter

    def _summarize():
        return Counter({k: len(v) for k, v in lookup.items()})

    return lookup.get, _summarize
