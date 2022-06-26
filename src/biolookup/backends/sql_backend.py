# -*- coding: utf-8 -*-

"""An BLS backend that communicates with a SQL database."""

import logging
import time
from collections import Counter
from functools import lru_cache
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .backend import Backend
from ..constants import (
    ALTS_TABLE_NAME,
    DEFS_TABLE_NAME,
    DERIVED_NAME,
    REFS_TABLE_NAME,
    RELS_NAME,
    SPECIES_TABLE_NAME,
    SYNONYMS_NAME,
    XREFS_NAME,
    get_sqlalchemy_uri,
)

__all__ = [
    "RawSQLBackend",
]

logger = logging.getLogger(__name__)


def _ensure_engine(
    engine: Union[None, str, Engine], engine_kwargs: Optional[Mapping[str, Any]] = None
) -> Engine:
    if engine is None:
        return create_engine(get_sqlalchemy_uri(), **(engine_kwargs or {}))
    elif isinstance(engine, str):
        return create_engine(engine, **(engine_kwargs or {}))
    else:
        return engine


class RawSQLBackend(Backend):
    """A backend that communicates with low-level SQL statements."""

    #: The engine
    engine: Engine

    def __init__(
        self,
        engine: Union[None, str, Engine] = None,
        *,
        engine_kwargs: Optional[Mapping[str, Any]] = None,
        refs_table: Optional[str] = None,
        alts_table: Optional[str] = None,
        defs_table: Optional[str] = None,
        species_table: Optional[str] = None,
        derived_table: Optional[str] = None,
        synonyms_table: Optional[str] = None,
        xrefs_table: Optional[str] = None,
        rels_table: Optional[str] = None,
    ):
        """Initialize the raw SQL backend.

        :param engine: An engine, connection string, or None if you want the default.
        :param engine_kwargs: Kwargs for making the engine, if engine is given as a string or None
        :param refs_table: A name for the references (prefix-id-name) table. Defaults to 'obo_reference'
        :param alts_table: A name for the alts (prefix-id-alt) table. Defaults to 'obo_alt'
        :param defs_table: A name for the defs (prefix-id-def) table. Defaults to 'obo_def'
        :param species_table: A name for the defs (prefix-id-species) table. Defaults to 'obo_species'
        :param derived_table: A name for the prefix-id-... derived table.
        :param synonyms_table: A name for the prefix-id-synonym table.
        :param xrefs_table: A name for the prefix-id-xprefix-xidentifier-provenance table.
        :param rels_table: A name for the relation table.
        """
        self.engine = _ensure_engine(engine, engine_kwargs=engine_kwargs)

        self.refs_table = refs_table or REFS_TABLE_NAME
        self.alts_table = alts_table or ALTS_TABLE_NAME
        self.defs_table = defs_table or DEFS_TABLE_NAME
        self.species_table = species_table or SPECIES_TABLE_NAME
        self.derived_table = derived_table or DERIVED_NAME
        self.synonyms_table = synonyms_table or SYNONYMS_NAME
        self.xrefs_table = xrefs_table or XREFS_NAME
        self.rels_table = rels_table or RELS_NAME

    def _count_summary(self, table):
        return self._get_one(f"SELECT SUM(identifier_count) FROM {table}_summary;")  # noqa:S608

    @lru_cache(maxsize=1)
    def count_names(self) -> int:
        """Get the number of names."""
        return self._count_summary(self.refs_table)

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
        return self._count_summary(self.defs_table)

    @lru_cache(maxsize=1)
    def count_species(self) -> Optional[int]:
        """Count species using a SQL query to the species summary table."""
        logger.info("counting species")
        return self._count_summary(self.species_table)

    @lru_cache(maxsize=1)
    def count_synonyms(self) -> Optional[int]:
        """Count synonyms using a SQL query to the synonyms summary table."""
        return self._count_summary(self.synonyms_table)

    @lru_cache(maxsize=1)
    def count_xrefs(self) -> Optional[int]:
        """Count xrefs using a SQL query to the xrefs summary table."""
        return self._count_summary(self.xrefs_table)

    @lru_cache(maxsize=1)
    def count_rels(self) -> Optional[int]:
        """Count relations using a SQL query to the relations summary table."""
        return self._count_summary(self.rels_table)

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

    def summarize_species(self) -> Counter:
        """Return the results of a SQL query that dumps the species summary table."""
        return self._get_summary(self.species_table)

    def summarize_synonyms(self) -> Counter:
        """Return the results of a SQL query that dumps the synonyms summary table."""
        return self._get_summary(self.synonyms_table)

    def summarize_xrefs(self) -> Counter:
        """Return the results of a SQL query that dumps the xrefs summary table."""
        return self._get_summary(self.xrefs_table)

    def summarize_rels(self) -> Counter:
        """Return the results of a SQL query that dumps the relations summary table."""
        return self._get_summary(self.rels_table)

    @lru_cache()
    def _get_summary(self, table) -> Counter:
        sql = f"SELECT prefix, identifier_count FROM {table}_summary;"  # noqa:S608
        with self.engine.connect() as connection:
            return Counter(dict(connection.execute(sql).fetchall()))

    @lru_cache()
    def has_prefix(self, prefix: str) -> bool:
        """Check for the prefix with a SQL query."""
        sql = text(
            f"SELECT EXISTS(SELECT 1 from {self.derived_table} WHERE prefix = :prefix);"  # noqa:S608
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
        return self._help_one("name", prefix, identifier)

    def get_species(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the species with a SQL query to the species table."""
        return self._help_one("species", prefix, identifier)

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the definition with a SQL query to the definitions table."""
        return self._help_one("definition", prefix, identifier)

    @lru_cache(maxsize=100_000)
    def _help_one(self, column: str, prefix: str, identifier: str) -> Optional[str]:
        sql = text(
            f"""
            SELECT {column}
            FROM {self.derived_table}
            WHERE prefix = :prefix and identifier = :identifier;
        """
        )  # noqa:S608
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix, identifier=identifier).fetchone()
            if result:
                return result[0]
        return None

    def get_synonyms(self, prefix: str, identifier: str) -> List[str]:
        """Get synonyms with a SQL query to the synonyms table."""
        return self._help_many(
            "synonym", table=self.synonyms_table, prefix=prefix, identifier=identifier
        )

    def get_xrefs(self, prefix: str, identifier: str) -> List[Mapping[str, str]]:
        """Get xrefs with a SQL query to the xrefs table."""
        return self._help_many_dict(
            "xref_prefix",
            "xref_identifier",
            "provenance",
            table=self.xrefs_table,
            prefix=prefix,
            identifier=identifier,
        )

    def get_rels(self, prefix: str, identifier: str) -> List[Mapping[str, str]]:
        """Get relations with a SQL query to the relations table."""
        return self._help_many_dict(
            "relation_prefix",
            "relation_identifier",
            "target_prefix",
            "target_identifier",
            table=self.rels_table,
            prefix=prefix,
            identifier=identifier,
        )

    def _help_many(self, column: str, *, table: str, prefix: str, identifier: str) -> List[str]:
        results = self._fetchmany(column, table=table, prefix=prefix, identifier=identifier)
        return [result for result, in results]

    def _help_many_dict(
        self, *columns: str, table: str, prefix: str, identifier: str
    ) -> List[Mapping[str, str]]:
        results = self._fetchmany(
            ", ".join(columns), table=table, prefix=prefix, identifier=identifier
        )
        return [dict(zip(columns, result)) for result in results]

    def _fetchmany(
        self, columns: str, table: str, prefix: str, identifier: str
    ) -> Iterable[Tuple[str]]:
        sql = text(
            f"""
            SELECT {columns}
            FROM {table}
            WHERE prefix = :prefix and identifier = :identifier;
        """
        )  # noqa:S608
        with self.engine.connect() as connection:
            results = connection.execute(sql, prefix=prefix, identifier=identifier).fetchall()
        return results
